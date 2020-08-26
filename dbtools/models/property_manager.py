from app import app
from .queries import Query


class PropertyManager:
	def __init__(self, tx, input_name):
		self.tx = tx
		self.input_name = input_name
		# Simple properties have a corresponding value that matches the record value
		# This value is also stored on the item node (with the following map of input variable to property name)
		self.simple_inputs_properties = {
			'set sample unit': 'unit',
			'set custom name': 'name',
			'set row': 'row',
			'set column': 'column',
			'set elevation': 'elevation',
			'set location': 'location'
		}
		# Time properties are used to set text_date and text_time as well as time properties on the item
		self.time_inputs_properties = {
			'set harvest date': 'text_date',
			'set harvest time': 'text_time',
			'set planting date': 'text_date'
		}
		self.variety_inputs =  {
			'assign variety name'
			'assign variety (el frances code)'
		}
		self.block_source_inputs = {
			'assign block to block by name',
			'assign block to block by id'
		}
		self.tree_source_inputs = {
			'assign tree to block by name',
			'assign tree to block by id'
		}
		self.sample_source_inputs = {
			'assign sample to sample(s) by id',
			'assign sample to tree(s) by id',
			'assign sample to block(s) by id',
			'assign sample to block(s) by name'
		}

	def update_kin_varieties(self, uid_list):
		# Inheritance:
		#  when assigning new variety or a new source we update varieties property for affected kin (see below):
		#
		# Terms:
		#    item: the primary subject of the query to which the property is being assigned
		#    ancestor:
		#      = direct lineal source of item
		#      = (item)-[:FROM | IS_IN*]->(ancestor)
		#    ancestors:
		#      = all ancestors of item
		#      = collect(ancestor)
		#    descendant:
		#      = direct lineal product of item
		#      = (descendant)-[:FROM | IS_IN*]->(item)
		#    descendants:
		#      = all descendants of item
		#      = collect(descendant)
		#    lineage:
		#      = ancestors + descendants
		#      = items connected to item by any path consisting of IS_IN or FROM relationships with single direction
		#    kin:
		#      = lineal kinsman
		#      = member of lineage
		#    kin_lineage:
		#      = lineage of kin
		#      i.e.:
		#        kin_ancestor = (kin)-[:FROM | IS_IN*]->(ancestor)
		#        kin_descendant = (descendant)-[:FROM | IS_IN*]->(kin)
		#        kin_lineage = collect(kin_ancestor) + collect(kin_descendant)
		#    kin_of_kin:
		#      = member of kin_lineage
		#
		# Updates:
		#  kin.varieties = collect(distinct kin_of_kin.variety)
		#  item.varieties = collect(distinct kin.variety)
		query = (
			' UNWIND $uid_list as uid '
			'	MATCH (item: Item {uid: uid}) '
			'	MATCH '
			'		ancestor_path = (:Field)<-[:FROM | IS_IN *0..]-(item) '
			'	MATCH '
			'		descendants_path = (item)<-[:FROM | IS_IN *0..]-(descendant: Item) '
			'		WHERE NOT (descendant)<-[:FROM | IS_IN]-() '
			'	WITH '
			'		item, '
			'		[i in nodes(ancestor_path) WHERE i.uid IS NOT NULL] + nodes(descendants_path)[1..] as lineage '
			'	UNWIND lineage as kin '
			'	MATCH '
			'		kin_ancestor_path = (field)<-[:FROM | IS_IN *0..]-(kin) '
			'	MATCH '
			'		kin_descendants_path = (kin)<-[:FROM | IS_IN *0..]-(kin_descendant: Item) '
			'		WHERE NOT (kin_descendant)<-[:FROM | IS_IN]-() '
			'	WITH '
			'		item, kin, '
			'		[i IN nodes(ancestor_path) + nodes(descendants_path)[1..] | i.variety] as lineage_variety_names '
			'	UNWIND '
			'		lineage_variety_names as variety_name '
			'	WITH '
			'		item, kin, collect(DISTINCT variety.name) as varieties '
			'	SET kin.varieties = varieties '
			'	WITH DISTINCT item '
		)
		parameters = {
			'uid_list': uid_list,
			'variety_inputs': self.variety_inputs
		}
		self.tx.run(query, parameters)


class PropertyUpdater(PropertyManager):
	def __init__(self, tx, input_name, records, row_errors):
		super().__init__(tx, input_name)
		self.records = records
		self.row_errors = row_errors

	def add_error(self, row_index, field, message):
		if row_index not in self.row_errors:
			self.row_errors[row_index] = {}
		if field not in self.row_errors[row_index]:
			self.row_errors[row_index][field] = set()
		self.row_errors[row_index][field].add(message)

	def set_properties(self):
		if self.input_name in self.simple_inputs_properties.keys():
			self.set_simple_property(self.simple_inputs_properties[self.input_name])
		elif self.input_name in self.time_inputs_properties:
			self.set_time()
		elif self.input_name in self.variety_inputs:
			self.set_variety()
		elif self.input_name in self.block_source_inputs:
			self.assign_block_to_block()
		elif self.input_name in self.tree_source_inputs:
			self.assign_tree_to_block()
		elif self.input_name in self.sample_source_inputs:
			self.assign_sample_to_sources()

	def set_simple_property(self, property_name):
		statement = (
			' UNWIND $records as record_data '
			'	MATCH (item: Item {uid: record_data.uid}) '
			' 	SET item.%s = record.value '
			'	SET item._LOCK_ = NULL '
		) % property_name  # can't use a property key as parameter
		parameters = {'records': self.records}
		self.tx.run_query(statement, parameters)

	def set_time(self):
		statement = (
			' UNWIND $records as record_data '
			'	MATCH (item: Item {uid: record_data.uid}) '
		)
		if 'date' in self.input_name:
			statement += (
				' SET item.text_date = record.value '
			)
		if 'time' in self.input_name:
			statement += (
				' SET item.text_time = record.value '
			)
		statement += (
			' SET item.time = CASE '
			'	WHEN item.text_time IS NULL '
			'	THEN apoc.date.parse(item.text_date + " " + item.text_time, "ms", "yyyy-MM-dd HH:mm")'
			'	ELSE apoc.date.parse(item.text_date + " 12:00" + , "ms", "yyyy-MM-dd HH:mm") '
			' SET item._LOCK_ = NULL '
		)
		parameters = {
			'records': self.records
		}
		self.tx.run_query(statement, parameters)

	def set_variety(self):
		# Properties affected:
		#  variety: a single value matching a known variety that is set from a record submission
		#  varieties: a collection (and reflected in relationships to the FieldVariety node)
		#
		# Relationships affected:
		#  OF_VARIETY: a definite specification of a single variety for an item
		#  CONTAINS_VARIETY:
		#    a relationship between a Field item and a given FieldVariety container node
		#    exists when any item with direct (single direction) path of IS_IN and/or FROM relationships to field
		#      has relationship OF_VARIETY to the relevant Variety
		#
		if 'code' in self.input_name:
			for record in self.records:
				try:
					record.value = app.config['SYSTEM_CODE_VARIETY']['el frances'][record.value]
				except KeyError:
					self.add_error(record.row_index, self.input_name, 'Variety code not recognised')
		statement = (
			' UNWIND $records as record_data '
			'	MATCH '
			'		(item: Item {uid: record_data.uid}) '
			'	SET item._LOCK_ = NULL '
			'	WITH item '
			'	MATCH '
			'		(field: Field {uid: record_data.field_uid}) '
			'	MATCH '
			'		(variety: Variety {name_lower: toLower(record_data.value) }) '
			'	SET item.variety = variety.name '
			'	MERGE (field)-[:CONTAINS_VARIETY]->(fv:FieldVariety)-[:OF_VARIETY]->(variety) '
			'	MERGE (item)-[of_variety:OF_VARIETY]->(fv) '
			'		ON CREATE SET of_variety.found = False '
			'		ON MERGE SET of_variety.found = True '
			# Get list of items that require kin to be updated (i.e. the variety is actually being set as a new value)
			'	WITH '
			'		item '
			'	WHERE NOT of_variety.found '
			'	RETURN item.uid '
		)
		parameters = {
			'records': self.records
		}
		uid_list = list(self.tx.run(statement, parameters))
		self.update_kin_varieties(uid_list)

# Notes for assign source functions
	# In all assign to source functions we need to consider inheritance updates.
	# Ensure match for variety property between source (and its ancestors) and item (and its descendants)
	# Need to update varieties property for all members of new item lineage (including item)
	#  don't have to update prior lineage:
	#  - item prior ancestors are still included in new item ancestors
	#    - We enforce this by only allowing reassignments to descendants of a prior source
	#  - item prior descendants are included in new lineage
	#  - source prior ancestors are included in new lineage
	#  - source prior descendants are unaffected as they were not in the prior lineage
	# Ensure any tree in new lineage has size(varieties) <= 1

	def assign_tree_to_block(self):
		if 'name' in self.input_name:
			input_property = 'name'
		else:
			input_property = 'id'
		statement = (
			' UNWIND $records as record_data '
			'	MATCH '
			'		(item: Tree {uid: record_data.uid}) '
			'	SET item._LOCK_ = Null '  # Remove the property used to lock the node
			'	WITH item '
			'	MATCH '
			'		(item)-[current_source_relationship: IS_IN]->(current_source) '  # FieldTrees or BlockTrees
			' 	MATCH '
			'		(:Field {uid: record_data.field_uid}) '
			'		<-[: IS_IN*]-(: Block {%s: record_data.value})'
			'		<-[:IS_IN]-(new_source: BlockTrees) '
			'	WITH '
			'		item,'
			'		current_source_relationship, '
			'		new_source '
			'	WHERE current_source <> new_source'
			'	DELETE current_source_relationship '
			'	CREATE (item)-[:IS_IN]->(new_source) '
			'	RETURN item.uid '
			% input_property
		)
		parameters = {
			'records': self.records
		}
		uid_list = list(self.tx.run(statement, parameters))
		self.update_kin_varieties(uid_list)

	def assign_block_to_block(self):
		if 'name' in self.input_name:
			input_property = 'name'
		else:
			input_property = 'id'
		statement = (
				' UNWIND $records as record_data '
				'	MATCH '
				'		(item: Block {uid: record_data.uid}) '
				'	SET item._LOCK_ = NULL '
				'	WITH item '
				'	MATCH '
				'		(item)-[current_source_relationship: IS_IN]->(current_source) '  # FieldBlocks or Block
				' 	MATCH '
				'		(:Field {uid: record_data.field_uid}) '
				'		<-[: IS_IN*]-(new_source: Block {%s: record_data.value})'
				'	WITH '
				'		item,'
				'		current_source_relationship, '
				'		new_source '
				'	WHERE current_source <> new_source'
				'	DELETE current_source_relationship '
				'	CREATE (item)-[:IS_IN]->(new_source) '
				'	RETURN item.uid '
				% input_property
		)
		parameters = {
			'records': self.records
		}
		uid_list = list(self.tx.run(statement, parameters))
		self.update_kin_varieties(uid_list)

	def assign_sample_to_sources(self):
		if 'to block' in self.input_name:
			source_label = "Block"
		elif 'to tree' in self.input_name:
			source_label = "Tree"
		else:  # 'to sample' in input_variable:
			source_label = "Sample"
		if 'name' in self.input_name:
			input_property = 'name'
		else:
			input_property = 'id'
		statement = (
			' UNWIND $records as record_data '
			'	MATCH '
			'		(item: Sample {uid: record_data.uid}) '
			'	SET item._LOCK_ = NULL '
			'	WITH item '
			'	MATCH '
			'		(item)-[current_source_relationship: FROM]->()'	 # Sample or ItemSamples
			'		DELETE current_source_relationship '
			'	WITH DISTINCT '
			'		item, record_data '
			'	MATCH '
			'		(new_source: %s)-[:IS_IN*]->(:Field {uid: record_data.field_uid}) '
			'		WHERE source.%s IN record_data.value '
			'	OPTIONAL MATCH '
			'		(new_source)<-[:FROM]-(new_source_sample_container: ItemSamples) '
			'	WITH '
			'		item, '
			'		current_source_relationship, '
			'		coalesce(new_source_sample_container, new_source) as new_source, '
			'	MERGE (item)-[:FROM]->(new_source) '
			'	WITH DISTINCT item '
			'	RETURN item.uid '
		) % (source_label, input_property)
		parameters = {
			'records': self.records
		}
		uid_list = list(self.tx.run(statement, parameters))
		self.update_kin_varieties(uid_list)


class PropertyReverter(PropertyManager):
	def __init__(self, tx, input_name, uid_list):
		super().__init__(tx, input_name)
		self.uid_list = uid_list

	def revert_properties(self):
		if self.input_name in self.simple_inputs_properties:
			self.remove()
		if self.input_name in self.time_inputs_properties:
			self.update_time()
		elif self.input_name in self.variety_inputs:
			self.update_variety()
		elif self.input_name in self.block_source_inputs:
			self.update_tree_source_block()
		elif self.input_name in self.tree_source_inputs:
			self.update_block_source_block()
		elif self.input_name in self.sample_source_inputs:
			self.update_sample_sources()

	def remove(self):
		query = (
			' UNWIND $uid_list as uid '
			'	MATCH (item: Item {uid: uid}) '
			'	SET item.%s = NULL '
		) % self.simple_inputs_properties[self.input_name]
		parameters = {
			'uid_list': self.uid_list
		}
		self.tx.run(query, parameters)

	def update_time(self):
		query = (
			' UNWIND $uid_list as uid '
			'	MATCH (item: Item {uid: uid}) '
			'	SET item.%s = NULL '
			'	SET item.time = CASE '
			'		WHEN item.text_date IS NULL '
			'		THEN NULL '
			'		WHEN item.text_time IS NULL '
			'		THEN apoc.date.parse(item.text_date + " " + item.text_time, "ms", "yyyy-MM-dd HH:mm")'
			'		ELSE apoc.date.parse(item.text_date + " 12:00" + , "ms", "yyyy-MM-dd HH:mm") '
			'		END '

		) % self.time_inputs_properties[self.input_name]
		parameters = {
			'uid_list': self.uid_list
		}
		self.tx.run(query, parameters)

	def update_variety(self):
		# We have to match the available records for variety assignment (could be code or name)
		# If there is an existing record then we don't change anything as all values have to agree
		query = (
			' UNWIND $uid_list as uid '
			'	MATCH (item: Item {uid: uid}) '
			'	OPTIONAL MATCH  '
			'		(item)'
			'		<-[:FOR_ITEM]-(ii: ItemInput)'
			'		<-[:RECORD_FOR]-(:Record),'
			'		(ii)-[:FOR_INPUT]->(:FieldInput) '
			'		-[:FOR_INPUT]->(input: Input) '
			'		WHERE input.name_lower IN $variety_inputs	'
			'	WITH DISTINCT item WHERE ii IS NULL '
			'	MATCH '
			'		(item)-[of_variety: OF_VARIETY]->(:FieldVariety) '
			'	DELETE of_variety '
			'	SET item.variety = NULL '
			'	WITH DISTINCT item '
			'	RETURN item.uid '
		)
		parameters = {
			'uid_list': self.uid_list,
			'variety_inputs': self.variety_inputs
		}
		uid_list = list(self.tx.run(query, parameters))
		self.update_kin_varieties(uid_list)

	def update_tree_source_block(self):
		# we allow multiple records for "assign" input variables to support remapping to a lower level block with
		# so have to set mapping based on most recent existing record (and map back to FieldTrees if no record)
		statement = (
			' UNWIND $uid_list as uid '
			'	MATCH '
			'		(item: Tree {uid: uid}) '
			'	OPTIONAL MATCH  '
			'		(item)'
			'		<-[:FOR_ITEM]-(ii: ItemInput)'
			'		<-[:RECORD_FOR]-(record: Record),'
			'		(ii)-[:FOR_INPUT]->(:FieldInput) '
			'		-[:FOR_INPUT]->(input: Input), '
			'		(record)<-[submitted :SUBMITTED]-()'
			'		WHERE input.name_lower IN $source_inputs '
			'	WITH '
			'		item, '
			'		record '
			'	ORDER BY submitted.time '
			'	WITH item, collect(record.value)[0] as value '
			'	MATCH '
			'		current_source_path = (item)-[:IS_IN*]->(field: Field)'
			'	MATCH '
			'		(field_trees: FieldTrees)-[:IS_IN]->(field) '
			' 	OPTIONAL MATCH '
			'		(block_trees: BlockTrees) '
			'		-[:IS_IN]->(block: Block) '
			'		-[:IS_IN*]->(field) '
			'		WHERE block.name_lower = toLower(record.value) OR block.id = record.value ' 
			# this filter works for both types of input (name or id)
			# as IDs are integers and names are strings so no overlap is possible 
			'	WITH '
			'		item, '
			'		relationships(current_source_path)[0] as current_source_rel, '
			'		coalesce(block_trees, field_trees) as new_source '
			'	WHERE '
			'		nodes(current_source_path)[1] <> new_source ' # only keep the items that are actually changing
			'	DELETE current_source_rel '
			'	CREATE (item)-[:IS_IN]->(new_source) '
			'	RETURN '
			'		item.uid '
		)
		parameters = {
			'uid_list': self.uid_list,
			'source_inputs': self.tree_source_inputs
		}
		uid_list = list(self.tx.run(statement, parameters))
		self.update_kin_varieties(uid_list)

	def update_block_source_block(self):
		# we allow multiple records for "assign" input variables to support remapping to a lower level block with
		# so have to set mapping based on most recent existing record (and map back to FieldBlocks if no record)
		statement = (
			' UNWIND $uid_list as uid '
			'	MATCH '
			'		(item: Block {uid: uid}) '
			'	OPTIONAL MATCH  '
			'		(item)'
			'		<-[:FOR_ITEM]-(ii: ItemInput)'
			'		<-[:RECORD_FOR]-(record: Record),'
			'		(ii)-[:FOR_INPUT]->(:FieldInput) '
			'		-[:FOR_INPUT]->(input: Input), '
			'		(record)<-[submitted :SUBMITTED]-()'
			'		WHERE input.name_lower IN $source_inputs '
			'	WITH '
			'		item, '
			'		record '
			'	ORDER BY submitted.time '
			'	WITH item, collect(record.value)[0] as value '
			'	MATCH '
			'		current_source_path = (item)-[:IS_IN*]->(field: Field)'
			'	MATCH '
			'		(field_blocks: FieldBlocks)-[:IS_IN]->(field) '
			' 	OPTIONAL MATCH '
			'		(block: Block) '
			'		-[:IS_IN*]->(field) '
			'		WHERE block.name_lower = toLower(record.value) OR block.id = record.value ' 
			# this filter works for both types of input (name or id)
			# as IDs are integers and names are strings so no overlap is possible 
			'	WITH '
			'		item, '
			'		relationships(current_source_path)[0] as current_source_rel '
			'		coalesce(block, field_blocks) as new_source '
			'	WHERE '
			'		nodes(current_source_path)[1] <> new_source ' # only keep the items that are actually changing
			'	DELETE current_source_rel '
			'	CREATE (item)-[:IS_IN]->(new_source) '
			'	RETURN '
			'		item.uid '
		)
		parameters = {
			'uid_list': self.uid_list,
			'source_inputs': self.block_source_inputs
		}
		uid_list = list(self.tx.run(statement, parameters))
		self.update_kin_varieties(uid_list)

	def update_sample_sources(self):
		# we allow multiple records for "assign" input variables to support remapping to a lower level block with
		# so have to set mapping based on most recent existing record (and map back to FieldBlocks if no record)
		statement = (
			' UNWIND $uid_list as uid '
			'	MATCH '
			'		current_source_path = (item: Sample {uid: uid})-[:IS_IN*]->(field: Field) '
			'	DELETE relationships(current_source_path)[0] ' 
			' 	WITH DISTINCT '
			'		item, '
			'		field '
			'	OPTIONAL MATCH  '
			'		(item) '
			'		<-[:FOR_ITEM]-(ii: ItemInput)'
			'		<-[:RECORD_FOR]-(record: Record),'
			'		(ii)-[:FOR_INPUT]->(:FieldInput) '
			'		-[:FOR_INPUT]->(input: Input), '
			'		(record)<-[submitted :SUBMITTED]-()'
			'		WHERE input.name_lower IN $source_inputs '
			'	WITH '
			'		item, '
			'		field, '
			'		record,'
			'		CASE '
			'			WHEN input.name_lower IN $source_tree_inputs '
			'			THEN "Tree" '
			'			WHEN input.name_lower IN $source_block_inputs '
			'			THEN "Block" '
			'			ELSE "Sample" '
			'			END as source_label '
			'	ORDER BY submitted.time ' 
			'	WITH '
			'		item, '
			'		field, '
			'		collect([source_label, record.value])[0] as label_value '
			'	MATCH '
			'		(field_container: ItemSamples)-[:FROM]->(field) '
			' 	OPTIONAL MATCH '
			'		(source: Item) '
			'		-[:IS_IN*]->(field) '
			'		WHERE label_value[0] in labels(source) '
			'		AND ('
			'			WHERE source.name_lower = toLower(label_value[1]) '
			'			OR '
			'			source.id = label_value[1] ' 
			'		)'
			'	OPTIONAL MATCH '
			'		(source_container: ItemSamples)-[:FROM]->(source) '
			'	WITH '
			'		item, '
			'		coalesce(source_container, source, field_container) as new_source '
			'	MERGE (item)-[:IS_IN]->(new_source) '
		)
		parameters = {
			'uid_list': self.uid_list,
			'source_inputs': self.sample_source_inputs,
			'source_tree_inputs': {i for i in self.sample_source_inputs if 'tree' in i},
			'source_block_inputs': {i for i in self.sample_source_inputs if 'block' in i}
		}
		self.tx.run(statement, parameters)
		self.update_kin_varieties(self.uid_list)
