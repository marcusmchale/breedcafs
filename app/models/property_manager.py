from app.models.queries import Query

class PropertyUpdater:
	def __init__(self, tx, username, batch_id, input_name, row_errors):
		self.tx = tx
		self.username = username
		self.batch_id = batch_id
		self.row_errors = row_errors
		self.input_name = input_name

	def set_properties(self):
		simple_properties = {
			'set sample unit': 'unit',
			'set custom name': 'name',
			'set row': 'row',
			'set column': 'column',
			'set elevation': 'elevation',
			'set location': 'location',
		}
		if self.input_name in simple_properties.keys():
			self.set_simple_property(simple_properties[self.input_name])
		elif any([i in self.input_name for i in ['date', 'time']]):
			#'set harvest date'
			#'set harvest time'
			#'set planting date'
			self.set_time()
		elif 'variety' in self.input_name:
			self.assign_variety()


#
#
#
#		# The below modify relationships , call these "assign" updates rather than "set"
#		'assign tree to block by name': self.assign_tree_to_block,
#		'assign tree to block by id': self.assign_tree_to_block,
#		'assign sample to block(s) by name': self.assign_sample_to_sources,
#		'assign sample to block(s) by id': self.assign_sample_to_sources,
#		'assign sample to tree(s) by id': self.assign_sample_to_sources,
#		'assign sample to sample(s) by id': self.assign_sample_to_sources,
#

	def set_simple_property(self, property_name):
		statement = (
			' MATCH '
			'	(user: User {username_lower: toLower($username)}) '
			'	-[:SUBMITTED]->(:Submissions) '
			'	-[:SUBMITTED]->(:Records) '
			'	-[:SUBMITTED]->(: UserFieldInput) '
			'	-[submitted: SUBMITTED { '
			'		time: $submission_time, '
			'		batch: $batch_id '
			'	}]->(record: Record) '
			'	-[:RECORD_FOR]->(ii:ItemInput) '
			'	-[:FOR_ITEM]->(item: Item), '
			'	(ii)'
			'	-[:FOR_INPUT]->(:FieldInput) '
			'	-[:FOR_INPUT]->(input: Input {name_lower: $input_name) '
			' SET item.%s = record.value '
		) % property_name  # can't use a property key as parameter
		parameters = {
			'input_name' : self.input_name
		}
		Query(write=True).run_query(statement, parameters)

	def set_time(self):
		statement = (
			' MATCH '
			'	(user: User {username_lower: toLower($username)}) '
			'	-[:SUBMITTED]->(:Submissions) '
			'	-[:SUBMITTED]->(:Records) '
			'	-[:SUBMITTED]->(: UserFieldInput) '
			'	-[submitted: SUBMITTED { '
			'		time: $submission_time, '
			'		batch: $batch_id '
			'	}]->(record: Record) '
			'	-[:RECORD_FOR]->(ii:ItemInput) '
			'	-[:FOR_ITEM]->(item: Item), '
			'	(ii)'
			'	-[:FOR_INPUT]->(:FieldInput) '
			'	-[:FOR_INPUT]->(input: Input {name_lower: $input_name) '
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
		)
		parameters = {
			'input_name': self.input_name
		}
		Query(write=True).run_query(statement, parameters)

	def assign_variety(self):
		#  Variety assignment can come from multiple properties (variety name/variety code)
		#    so we need to handle cases where variety is already set
		#
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
		# Inheritance:
		#  when assigning new variety we update varieties for affected kin (see below):
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
		#    lineage: (NB: here this term excludes item)
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
		#
		# Errors to be raised:
		#  - kin.variety IS NOT NULL and (kin.varieties <> [kin.variety])
		#  - size(kin.varieties) > 1 WHERE "Tree" in labels(kin)
		statement = (
			' MATCH '
			'	(user: User {username_lower: toLower($username)}) '
			'	-[:SUBMITTED]->(:Submissions) '
			'	-[:SUBMITTED]->(:Records) '
			'	-[:SUBMITTED]->(: UserFieldInput) '
			'	-[submitted: SUBMITTED { '
			'		time: $submission_time, '
			'		batch: $batch_id '
			'	}]->(record: Record) '
			'	-[:RECORD_FOR]->(ii:ItemInput) '
			'	-[:FOR_ITEM]->(item: Item), '
			'	(ii)'
			'	-[:FOR_INPUT]->(:FieldInput) '
			'	-[:FOR_INPUT]->(input: Input {name_lower: $input_name) '
		)

			'	OPTIONAL MATCH '
			'		(variety: Variety) '
		)
		if 'name' in input_variable:
			statement += (
				' WHERE variety.name_lower = toLower(trim(value)) '
			)
		else:  # input_variable contains 'code'
			statement += (
				' WHERE variety.code = toLower(trim(value)) '
			)
		statement += (
			'	WITH '
			'		uid, value, item, field, variety, '
			'		item.variety as existing_variety '
			'	FOREACH (n IN CASE '
			'		WHEN item.variety IS NULL '
			'		THEN [1] ELSE [] END | '
			'		MERGE '
			'			(field) '
			'			-[: CONTAINS_VARIETY]->(fv:FieldVariety) '
			'			-[: OF_VARIETY]->(variety) '
			'		MERGE '
			'			(item) '
			'			-[: OF_VARIETY]->(fv) '
			'		SET '
			'			item.variety = variety.name,  '
			'			item.varieties = [variety.name] '
			'	) '
			'	WITH '
			'		uid, value, item, existing_variety, variety'
			'	OPTIONAL MATCH '
			'		(item)-[:IS_IN | FROM *]->(ancestor: Item) '
			'	WITH '
			'		uid, value, item, existing_variety, variety, '
			'		collect(distinct ancestor) as ancestors '
			'	OPTIONAL MATCH '
			'		(item)<-[:IS_IN | FROM *]-(descendant: Item) '
			'	WITH  '
			'		uid, value, item, existing_variety, variety, '
			'		ancestors + collect(distinct descendant) as lineage '
			'	UNWIND lineage AS kin '
			'		OPTIONAL MATCH '
			'			(kin)-[:IS_IN | FROM *]->(kin_ancestor: Item) '
			'		WITH '
			'			uid, value, item, existing_variety, variety, '
			'			kin, '
			'			collect(distinct kin_ancestor) as kin_ancestors '
			'		OPTIONAL MATCH '
			'			(kin)<-[:IS_IN | FROM *]-(kin_descendant: Item) '
			'		WITH '
			'			uid, value, item, existing_variety, variety, '
			'			kin, '
			'			kin_ancestors + collect(distinct kin_descendant) as kin_lineage '
			'		UNWIND '
			'			kin_lineage as kin_of_kin '
			'		WITH '
			'			uid, value, item, existing_variety, variety, '
			'			kin, '
			'			collect(distinct kin_of_kin.variety) as kin_varieties, '
			# If kin is a Tree we need to record kin_of_kin UID and variety if it differs from variety.name
			# as we could have a conflict where two samples from same tree have different variety assigned
			# For fields/blocks/samples we accept cases of multiple varieties
			# Among this list will also be direct kin conflicts
			#   so only include these errors in response when no direct kin conflicts
			'			[ '
			'				x in collect(distinct [kin_of_kin.uid, kin_of_kin.variety]) WHERE x[1] IS NOT NULL'
			'			] as kin_variety_sources '
			'		SET kin.varieties = CASE WHEN kin.variety IS NOT NULL THEN [kin.variety] ELSE kin_varieties END '
			'	WITH '
			'		uid, value, item, existing_variety, variety, '
			'		[ '
			'			x in collect( '
			'				distinct [kin.uid, kin.variety] '
			'			) WHERE x[1] IS NOT NULL AND x[1] <> variety.name '
			'		] as kin_conflicts, '
			'		[x in collect(distinct [[kin.uid, kin_variety_sources], labels(kin), kin.varieties]) '
			'			WHERE "Tree" IN x[1] AND size(x[2]) > 1 '
			'			| x[0] '
			'		] as tree_varieties_error '
			'	RETURN { '
			'		UID: uid, '
			'		value: value, '
			'		item_uid: item.uid, '
			'		existing_variety: existing_variety, '
			'		assigned_variety: variety.name, '
			'		item_variety: item.variety, '
			'		item_varieties: item.varieties, '
			'		tree_varieties_error: tree_varieties_error, '
			'		kin_conflicts: kin_conflicts '
			'	} '
		)
		result = self.tx.run(
			statement,
			uid_value_list=self.updates[input_variable]
		)
		self.variety_error_check(
			result,
			input_variable
		)


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

	def assign_tree_to_block(self, input_variable):
		statement = (
			' UNWIND $uid_value_list AS uid_value '
			'	WITH '
			'		uid_value[0] as uid,'
			'		uid_value[1] as value '
			'	OPTIONAL MATCH '
			'		(tree: Tree { '
			'			uid: uid '
			'		}) '
			'	OPTIONAL MATCH '
			'		(field: Field {uid: '
			'			CASE '
			'			WHEN toInteger(uid) IS NOT NULL '
			'				THEN uid '
			'			ELSE '
			'				toInteger(split(uid, "_")[0]) '
			'			END '
			'		}) '
			'	OPTIONAL MATCH '
			'		(tree) '
			'		-[:IS_IN]->(:BlockTrees) '
			'		-[:IS_IN]->(prior_block: Block) '
			'	OPTIONAL MATCH '
			'		(new_block: Block) '
			'		-[:IS_IN]->(:FieldBlocks) '
			'		-[:IS_IN]->(field) '
		)
		if 'name' in input_variable:
			statement += (
				' WHERE new_block.name_lower = toLower(trim(value)) '
			)
		else:
			statement += (
				' WHERE new_block.id = toInteger(value) '
			)
		statement += (
			# check for variety conflicts
			'	OPTIONAL MATCH '
			'		(tree)<-[: FROM*]-(sample: Sample) '
			'	WITH '
			'		uid, value, '
			'		tree, field, '
			'		prior_block, '
			'		new_block, '
			' 		CASE WHEN tree.variety IS NOT NULL AND new_block.variety IS NOT NULL AND tree.variety <> new_block.variety '
			'			THEN [{ '
			'				ancestor: new_block.uid,'
			'				ancestor_variety: new_block.variety, '
			'				descendant: tree.uid, '
			'				descendant_variety: tree.variety '
			'			}] '
			'			ELSE [] END + ' 
			'		[ '
			'			x in collect({ '
			'				ancestor: new_block.uid,'
			'				ancestor_variety: new_block.variety, '
			'				descendant: sample.uid, '
			'				descendant_variety: sample.variety '
			'			}) WHERE '
			'				x["descendant_variety"] IS NOT NULL '
			'				AND x["ancestor_variety"] IS NOT NULL '
			'				AND  x["descendant_variety"] <> x["ancestor_variety"] '
			'		] as variety_conflicts '
			'	FOREACH (n IN CASE WHEN '
			'			prior_block IS NULL AND '
			'			new_block IS NOT NULL AND '
			'			size(variety_conflicts) = 0 '
			'		THEN [1] ELSE [] END | '
			'		MERGE '
			'			(bt:BlockTrees) '
			'			-[:IS_IN]->(new_block) '
			'		MERGE '
			'			(bt) '
			'			<-[:FOR]-(c:Counter) '
			'			ON CREATE SET '
			'				c.count = 0, '
			'				c.name = "tree", '
			'				c.uid = (new_block.uid + "_tree") '
			'		SET c._LOCK_ = True '
			'		MERGE (tree)-[:IS_IN]->(bt) '
			'		SET c.count = c.count + 1 '
			'		REMOVE c._LOCK_ '
			'	) '
			'	WITH '
			'		uid, value, tree, field, prior_block, new_block, variety_conflicts '
			'		OPTIONAL MATCH '
			'			(tree)-[:IS_IN*]->(ancestor: Item) '
			'	WITH '
			'		uid, value, tree, field, prior_block, new_block, variety_conflicts, '
			'		collect(ancestor) as ancestors '
			'		OPTIONAL MATCH '
			'			(tree)<-[:FROM*]-(descendant: Item) '
			'	WITH '
			'		uid, value, tree, field, prior_block, new_block, variety_conflicts, '
			'		ancestors + collect(descendant) as lineage '
			'		UNWIND lineage as kin '
			'			OPTIONAL MATCH '
			'				(kin)-[:IS_IN | FROM *]->(kin_ancestor: Item) '
			'			WITH '
			'				uid, value, tree, field, prior_block, new_block, variety_conflicts, '
			'				kin, collect(kin_ancestor) as kin_ancestors '
			'			OPTIONAL MATCH '
			'				(kin)<-[:IS_IN | FROM *]-(kin_descendant: Item) '
			'			WITH '
			'				uid, value, tree, field, prior_block, new_block, variety_conflicts, '
			'				kin, kin_ancestors + collect(kin_descendant) as kin_lineage '
			'			UNWIND kin_lineage as kin_of_kin '
			'			WITH '
			'				uid, value, tree, field, prior_block, new_block, variety_conflicts, '
			'				kin, collect(distinct kin_of_kin.variety) as kin_varieties '
			'			SET kin.varieties = CASE WHEN kin.variety IS NOT NULL THEN [kin.variety] ELSE kin_varieties END '
			'	WITH '
			'		uid, value, tree, field, prior_block, new_block, variety_conflicts, '
			'		collect(distinct kin.variety) as varieties, '
			'		collect(distinct [kin.uid, kin.variety]) as variety_sources '
			'	SET tree.varieties = CASE WHEN tree.variety IS NOT NULL THEN [tree.variety] ELSE varieties END '
			'	RETURN  { '
			'		UID: uid, '
			'		value: value, '
			'		item_name: tree.name, '
			'		item_uid: tree.uid, '
			'		prior_source_details: [[prior_block.uid, prior_block.name]], '
			'		new_source_details: [[new_block.uid, new_block.name]], '
			'		unmatched_sources:	CASE WHEN new_block IS NULL THEN [value] END, '
			'		invalid_sources: CASE '
			'			WHEN prior_block IS NOT NULL AND prior_block <> new_block '
			'			THEN [[new_block.uid, new_block.name]] '
			'			END, '
			'		unrepresented_sources: CASE '
			'			WHEN prior_block IS NOT NULL AND prior_block <> new_block '
			'			THEN [[prior_block.uid, prior_block.name]] '
			'			END, '
			'		variety_conflicts: variety_conflicts, '
			'		tree_varieties_error: CASE '
			'			WHEN size(tree.varieties) > 1 '
			'			THEN '
			'				collect([tree.uid,variety_sources]) '
			'			END '
			'	} '
		)
		result = self.tx.run(
			statement,
			uid_value_list=self.updates[input_variable]
		)
		self.item_source_error_check(
			result,
			'block'
		)

	def assign_sample_to_sources(self, input_variable):
		if 'to block' in input_variable:
			source_level = "Block"
		elif 'to tree' in input_variable:
			source_level = "Tree"
		elif 'to sample' in input_variable:
			source_level = "Sample"
		statement = (
			' UNWIND $uid_value_list AS uid_value '
			'	WITH '
			'		uid_value[0] as uid,'
			'		uid_value[1] as value '
			'	OPTIONAL MATCH '
			'		(sample: Sample { '
			'			uid: uid '
			'		}) '
			'	OPTIONAL MATCH '
			'		(field: Field {uid: '
			'			CASE '
			'			WHEN toInteger(uid) IS NOT NULL '
			'				THEN uid '
			'			ELSE '
			'				toInteger(split(uid, "_")[0]) '
			'			END '
			'		}) '
			'	WITH '
			'		uid, value, sample, field '
			'	OPTIONAL MATCH '
			'		(sample)-[prior_primary_sample_from: FROM]->(:ItemSamples)-[: FROM]->(prior_primary_source: Item) '
			'	OPTIONAL MATCH '
			'		(sample)-[prior_secondary_sample_from: FROM]->(prior_secondary_source: Sample) '
			'	WITH '
			'		uid, value, sample, field, '
			'		collect(coalesce(prior_primary_source, prior_secondary_source)) as prior_sources, '
			'		collect(coalesce(prior_primary_sample_from, prior_secondary_sample_from)) as prior_source_rels '
			'	UNWIND value as source_identifier '
			'		OPTIONAL MATCH '
		)
		if source_level == 'Block':
			statement += (
				' (new_source: Block)-[: IS_IN *2]->(field) '
			)
		elif source_level == 'Tree':
			statement += (
				' (new_source: Tree)-[: IS_IN *2]->(field) '
			)
		elif source_level == 'Sample':
			statement += (
				' (new_source: Sample)-[: IS_IN | FROM*]->(field) '
			)
		statement += (
			'		WHERE $source_level in labels(new_source) AND '
		)
		if 'name' in input_variable:
			statement += (
				'		new_source.name_lower = source_identifier '
			)
		else:
			statement += (
				'		new_source.id = toInteger(source_identifier) '
			)
		statement += (
			'		UNWIND prior_sources as prior_source '
			'			OPTIONAL MATCH '
			'				lineage_respected = (new_source)-[: IS_IN | FROM*]->(prior_source) '
			'	WITH  '
			'		uid, value, sample, '
			'		prior_source_rels, '
			'		collect(distinct new_source) as new_sources, '
			'		collect(distinct prior_source) as prior_sources, '
			'		[ '
			'			x in collect(distinct [source_identifier, new_source]) '
			'			WHERE x[1] IS NULL | x[0] '
			'		] as unmatched_sources, '  # where the new source was not found by name/id in this field
			'		[ '
			'			x in collect(distinct [new_source, lineage_respected]) '
			'			WHERE x[1] IS NOT NULL | x[0] '
			'		] as valid_sources, '  # where the new source is a direct descendant of a prior source
			'		[ '
			'			x in collect(distinct [prior_source, lineage_respected])'
			'			WHERE x[1] IS NOT NULL | x[0] '
			'		] as represented_sources '  # where the prior source has a direct descendant among the new sources
			'	WITH '
			'		uid, value, sample, '
			'		prior_source_rels, '
			'		new_sources, '
			'		prior_sources, '
			'		unmatched_sources, '
			'		[ '
			'			x in new_sources '
			'			WHERE NOT x IN valid_sources '
			'			AND NOT x IN prior_sources '
			'			| [x.uid, x.name] '
			'		]  as invalid_sources, '
			'		[ '
			'			x in prior_sources '
			'			WHERE NOT x IN represented_sources '
			'			AND NOT x IN new_sources '
			'			| [x.uid, x.name] '
			'		]  as unrepresented_sources '
			# Need to check for variety conflicts that would be created in new lineage 
			# we also want to update varieties property in all members of new lineage
			' 	UNWIND new_sources as new_source '
			'		OPTIONAL MATCH '
			'			(new_source)-[:IS_IN | FROM *]->(ancestor: Item) '
			'		OPTIONAL MATCH '
			'			(sample)<-[:IS_IN | FROM *]-(descendant: Item) '
			# The above matches make a cartesian product 
			# this may be reasonable as we want to check for any conflict among these products
			'	WITH '
			'		uid, value, sample, '
			'		prior_source_rels, '
			'		collect(distinct new_source) as new_sources, '
			'		prior_sources, '
			'		unmatched_sources, invalid_sources, unrepresented_sources, '
			'		collect(distinct ancestor) + collect(distinct descendant) + collect(distinct new_source) as lineage, '	
			'		CASE '
			'			WHEN '
			'				sample.variety IS NOT NULL AND '
			'				new_source.variety IS NOT NULL AND '
			'				sample.variety <> new_source.variety '
			'			THEN collect(distinct { '
			'				ancestor: new_source.uid,'
			'				ancestor_variety: new_source.variety, '
			'				descendant: sample.uid, '
			'				descendant_variety: sample.variety '
			'			}) '
			'			ELSE []'
			'		END + ' 
			'		[ '	
			'			x in collect(distinct { '
			'				ancestor: ancestor.uid,'
			'				ancestor_variety: ancestor.variety, '
			'				descendant: sample.uid, '
			'				descendant_variety: sample.variety'
			'			}) WHERE '
			'				x["descendant_variety"] IS NOT NULL AND '
			'				x["ancestor_variety"] IS NOT NULL AND '
			'				x["descendant_variety"] <> x["ancestor_variety"] '
			'		] + '
			'		[ '	
			'			x in collect(distinct { '
			'				ancestor: new_source.uid,'
			'				ancestor_variety: new_source.variety, '
			'				descendant: descendant.uid, '
			'				descendant_variety: descendant.variety'
			'			}) WHERE '
			'				x["descendant_variety"] IS NOT NULL AND '
			'				x["ancestor_variety"] IS NOT NULL AND '
			'				x["descendant_variety"] <> x["ancestor_variety"] '
			'		] + '
			'		[ '	
			'			x in collect(distinct { '
			'				ancestor: ancestor.uid,'
			'				ancestor_variety: ancestor.variety, '
			'				descendant: descendant.uid, '
			'				descendant_variety: descendant.variety '
			'			}) WHERE '
			'				x["descendant_variety"] IS NOT NULL AND '
			'				x["ancestor_variety"] IS NOT NULL AND '
			'				x["descendant_variety"] <> x["ancestor_variety"] '
			'		] '
			'			as variety_conflicts  '
			'	UNWIND '
			'		new_sources as new_source '
			'		FOREACH (n IN CASE WHEN '
			'			sample <> new_source AND '	
			'			new_source IS NOT NULL AND '
			'			NOT new_source IN prior_sources AND '
			'			sample IS NOT NULL AND '
			'			size(unmatched_sources) = 0 AND '
			'			size(invalid_sources) = 0 AND '
			'			size(unrepresented_sources) = 0 AND '
			'			size(variety_conflicts) = 0 '
			'		THEN [1] ELSE [] END | '
			'			FOREACH (n in prior_source_rels | delete n) '
		)
		if source_level == "Sample":
			statement += (
				'		MERGE (sample)-[:FROM]->(new_source) '
			)
		else:
			statement += (
				'		MERGE (is:ItemSamples)-[:FROM]->(new_source) '
				'		MERGE (sample)-[:FROM]->(is) '
			)
		statement += (
			'		) '
			'	WITH '
			'		uid, value, sample, '
			'		prior_source_rels, '
			'		prior_sources, collect(new_source) as new_sources, '
			'		unmatched_sources, invalid_sources, unrepresented_sources, variety_conflicts, '
			'		lineage '
			'	WITH '
			'		uid, value, sample, '
			'		[x in prior_sources | [x.uid, x.name]] as prior_source_details, '
			'		[x in new_sources | [x.uid, x.name]] as new_source_details, '
			'		unmatched_sources, invalid_sources, unrepresented_sources, variety_conflicts, '
			'		lineage '
			'	UNWIND lineage as kin '
			'		OPTIONAL MATCH '
			'			(kin)-[:IS_IN | FROM *]->(kin_ancestor: Item) '
			'		WITH '
			'			uid, value, sample, '
			'			prior_source_details, new_source_details, '
			'			unmatched_sources, invalid_sources, unrepresented_sources, variety_conflicts, '
			'			kin, collect(distinct kin_ancestor) as kin_ancestors '
			'		OPTIONAL MATCH '
			'			(kin)<-[:IS_IN | FROM *]-(kin_descendant: Item) '
			'		WITH '
			'			uid, value, sample, '
			'			prior_source_details, new_source_details, '
			'			unmatched_sources, invalid_sources, unrepresented_sources, variety_conflicts, '
			'			kin, kin_ancestors + collect(distinct kin_descendant) as kin_lineage '
			'		UNWIND kin_lineage as kin_of_kin '
			'		WITH '
			'			uid, value, sample, '
			'			prior_source_details, new_source_details, '
			'			unmatched_sources, invalid_sources, unrepresented_sources, variety_conflicts, '
			'			kin, collect(distinct kin_of_kin.variety) as kin_of_kin_varieties, '
			'			[ '
			'				x in collect(distinct [kin_of_kin.uid, kin_of_kin.variety]) WHERE x[1] IS NOT NULL'
			'			] as kin_variety_sources '
			'		SET kin.varieties = CASE WHEN kin.variety IS NOT NULL THEN [kin.variety] ELSE kin_of_kin_varieties END '
			'	WITH '
			'		uid, value, sample, '
			'		prior_source_details, new_source_details, '
			'		unmatched_sources, invalid_sources, unrepresented_sources, variety_conflicts, '
			'		collect(distinct kin.variety) as kin_varieties, '
			'		[x in collect(distinct [[kin.uid, kin_variety_sources], labels(kin), kin.varieties]) '
			'			WHERE "Tree" IN x[1] AND size(x[2]) > 1 '
			'			| [x[0]] '
			'		] as tree_varieties_error '
			'	SET sample.varieties = CASE WHEN sample.variety IS NOT NULL THEN [sample.variety] ELSE kin_varieties END '
			'	RETURN  { '
			'		UID: uid, '
			'		value: value, '
			'		item_name: sample.name, '
			'		item_uid: sample.uid, '
			'		prior_source_details: prior_source_details,'
			'		new_source_details: new_source_details, '
			'		unmatched_sources: unmatched_sources, '
			'		invalid_sources: invalid_sources, '
			'		unrepresented_sources: unrepresented_sources, '
			'		variety_conflicts: variety_conflicts, '
			'		tree_varieties_error: tree_varieties_error '
			'	} '
		)
		for item in self.updates[input_variable]:
			if 'name' in input_variable:
				item[1] = Parsers.parse_name_list(item[1])
			else:
				item[1] = Parsers.parse_range_list(item[1])
		result = self.tx.run(
			statement,
			uid_value_list=self.updates[input_variable],
			source_level=source_level
		)
		self.item_source_error_check(
			result,
			source_level
		)




	def variety_error_check(
				self,
				result,
				property_name='variety',
		):
			row_errors = []
			for record in result:
				if not record[0]['item_uid']:  # this shouldn't happen as uids are already checked
					logging.debug(
						'A variety assignment was attempted but item was not found: ' + str(record[0]['UID'])
					)
					row_errors.append(
						'Item not found (' + str(record[0]['UID']) + ')'
					)
				elif not record[0]['assigned_variety']:  # this shouldn't happen as values are already checked
					logging.debug(
						'A variety assignment was attempted but variety was not found: ' + str(record[0]['value'])
					)
					row_errors.append(
						'Variety not found (' + str(record[0]['value']) + ')'
					)
				else:
					if all([
						record[0]['existing_variety'],
						record[0]['existing_variety'] != record[0]['assigned_variety']
					]):
						row_errors.append(
							'Item (' +
							str(record[0]['item_uid']) +
							') cannot be assigned this variety '
							' (' +
							str(record[0]['assigned_variety']) +
							') as it is already assigned a different variety (' +
							str(record[0]['existing_variety']) +
							')'
						)
					elif record[0]['kin_conflicts']:
						row_errors.append(
							'Item (' +
							str(record[0]['item_uid']) +
							') cannot be assigned this variety '
							' (' +
							str(record[0]['assigned_variety']) +
							') as directly linked items already have a different variety assigned (' +
							','.join(
								[
									'(UID:' +
									str(i[0]) +
									', variety:' +
									str(i[1]) +
									')' for i in record[0]['kin_conflicts']
								]
							) + ')'
						)
					elif record[0]['tree_varieties_error']:
						row_errors.append(
							'Sample (' +
							str(record[0]['item_uid']) +
							') cannot be assigned this variety '
							' (' +
							str(record[0]['assigned_variety']) +
							') as another sample from the same tree already has a different variety assigned (' +
							','.join([
								'(uid: ' + str(j[0]) + ', variety: ' + str(j[1]) + ')'
								for j in i[1][0:5]  # silently only reporting the first 5 items
							]) for i in record[0]['tree_varieties_error'][0:2]
						)
			if row_errors:
				if property_name not in self.errors:
					self.errors[property_name] = []
				self.errors[property_name] += row_errors

		def item_source_error_check(
				self,
				result,
				property_name='source',
		):
			errors = []
			for record in result:
				# If we don't find the item
				if not record[0]['item_uid']:
					errors.append(
						'Item source assignment failed. The item (' + str(record[0]['UID']) + ')' + ') was not found.'
					)
				# if new source is direct match for self
				elif record[0]['item_uid'] in [ns[0] for ns in record[0]['new_source_details']]:
					errors.append(
						'Item source assignment failed. An attempt was made to assign item ('
						+ str(record[0]['UID'])
						+ ') to itself. '
					)
				# If we don't find the source
				elif len(record[0]['new_source_details']) == 0:
					errors.append(
						'Item (' +
						'UID: ' + record[0]['item_uid']
					)
					if record[0]['item_name']:
						errors[-1] += ', name: ' + record[0]['item_name']
					if len(record[0]['value']) >= 1:
						errors[-1] += (
							') source assignment failed. The sources were not found: '
						)
					else:
						errors[-1] += (
							') source assignment failed. The source was not found: '
						)
					errors[-1] += (
						', '.join([
							str(i) for i in record[0]['value']
						])
					)
				elif record[0]['unmatched_sources']:
					errors.append(
						'Item (' +
						'UID: ' + record[0]['item_uid']
					)
					if record[0]['item_name']:
						errors[-1] += ', name: ' + record[0]['item_name']
					if len(record[0]['unmatched_sources']) >= 1:
						errors[-1] += (
							') source assignment failed. Some sources were not found: '
						)
					else:
						errors[-1] += (
							') source assignment failed. A source was not found: '
						)
					errors[-1] += (
						', '.join([
							str(i) for i in record[0]['unmatched_sources']
						])
					)
				elif record[0]['invalid_sources']:
					errors.append(
						'Item (' +
						'UID: ' + record[0]['item_uid']
					)
					if record[0]['item_name']:
						errors[-1] += ', name: ' + record[0]['item_name']
					if len(record[0]['invalid_sources']) >= 1:
						errors[-1] += ') source assignment failed. Proposed sources ['
					else:
						errors[-1] += ') source assignment failed. Proposed source '
					errors[-1] += ', '.join([
						'(uid: ' + str(i[0]) + ', name: ' + str(i[1]) + ')' if i[1]
						else '(uid: ' + str(i[0]) + ')'
						for i in record[0]['invalid_sources']
					])
					if len(record[0]['invalid_sources']) >= 1:
						errors[-1] += (
							'] are not themselves sourced (either directly or indirectly) from '
						)
					else:
						errors[-1] += (
							' is not itself sourced (either directly or indirectly) from '
						)
					if len(record[0]['prior_source_details']) >= 1:
						errors[-1] += (
							'any of the existing assigned sources: '
						)
					else:
						errors[-1] += (
							'the existing assigned source: '
						)
					errors[-1] += ', '.join([
						'(uid: ' + str(i[0]) + ', name: ' + str(i[1]) + ')' if i[1]
						else '(uid: ' + str(i[0]) + ')'
						for i in record[0]['prior_source_details']
					])
				elif record[0]['unrepresented_sources']:
					# this occurs when not all prior sources are represented by the new sources
					# this occurs in the case of attempting to re-assign to new block(s)/tree(s) without deleting an existing record
					# and also in re-assigning pooled samples with greater detail
					errors.append(
						'Item (' +
						'UID: ' + record[0]['item_uid']
					)
					if record[0]['item_name']:
						errors[-1] += ', name: ' + record[0]['item_name']
					if len(record[0]['unrepresented_sources']) >= 1:
						errors[-1] += ') source assignment failed. Existing sources ['
					else:
						errors[-1] += ') source assignment failed. Existing source '
					errors[-1] += ', '.join([
						'(uid: ' + str(i[0]) + ', name: ' + str(i[1]) + ')' if i[1]
						else '(uid: ' + str(i[0]) + ')'
						for i in record[0]['unrepresented_sources']
					])
					if len(record[0]['unrepresented_sources']) >= 1:
						errors[-1] += (
							'] would not be represented (either directly or indirectly) by '
						)
					else:
						errors[-1] += (
							' would not be represented (either directly or indirectly) by '
						)
					if len(record[0]['new_source_details']) >= 1:
						errors[-1] += (
							'the proposed sources: '
						)
					else:
						errors[-1] += (
							'the proposed source: '
						)
					errors[-1] += ', '.join([
						'(uid: ' + str(i[0]) + ', name: ' + str(i[1]) + ')' if i[1]
						else '(uid: ' + str(i[0]) + ')'
						for i in record[0]['new_source_details']
					])
				elif record[0]['variety_conflicts']:
					errors.append(
						'Item (' +
						'UID: ' + record[0]['item_uid']
					)
					if record[0]['item_name']:
						errors[-1] += ', name: ' + record[0]['item_name']
					errors[-1] += (
						') source assignment failed. '
					)
					errors[-1] += (
						'The variety assigned to '
						'proposed source(s) conflicts with the variety that is specified for the item '
						'or its samples: '
					)
					if len(record[0]['variety_conflicts']) >= 2:
						errors[-1] += (
							'The assignment would create many such conflicts so '
							'only the first two are being reported. '
						)
					errors[-1] += ', '.join([
						'(source uid: ' + str(i['ancestor']) + ', source variety:' + str(i['ancestor_variety']) + ', ' +
						'descendant item: ' + str(i['descendant']) + ', ' +
						'descendant variety: ' + str(i['descendant_variety']) + ')'
						for i in record[0]['variety_conflicts'][0:2]
					])
				elif record[0]['tree_varieties_error']:
					errors.append(
						'Item (' +
						'UID: ' + record[0]['item_uid']
					)
					if record[0]['item_name']:
						errors[-1] += ', name: ' + record[0]['item_name']
					errors[-1] += (
						') source assignment failed. '
					)
					if len(record[0]['tree_varieties_error']) >= 2:
						errors[-1] += (
							'The proposed source assignment would create ambiguous definitions for many trees, '
							'only the first two are being reported. '
						)
					errors[-1] += '. '.join([
						'The proposed source assignment would create an ambiguous definition '
						'for the variety of a tree (' + str(i[0]) + '). ' +
						'The conflicts are between varieties assigned to the following items: ' +
						','.join([
							'(uid: ' + str(j[0]) + ', variety: ' + str(j[1]) + ')'
							for j in i[1][0:5]  # silently only reporting the first 5 items
						]) for i in record[0]['tree_varieties_error'][0:2]
					])
			if errors:
				if property_name not in self.errors:
					self.errors[property_name] = []
				self.errors[property_name] += errors
