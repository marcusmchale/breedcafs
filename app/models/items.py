# import os
# import cStringIO
from app import app
from app.cypher import Cypher
from .neo4j_driver import (
	bolt_result
)
from flask import session
# from datetime import datetime
from .neo4j_driver import get_driver


class FindLocations:
	def __init__(self, country):
		self.country = country

	def find_country(self):
		parameters = {
			'country': self.country
		}
		statement = (
			' MATCH '
			'	(country:Country { '
			'		name_lower : toLower(trim($country))'
			' 	}) '
			' RETURN '
			'	country.name '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(bolt_result, statement, parameters).single()
			if result:
				return result.value()
			else:
				return None

	def find_region(self, region):
		parameters = {
			'country': self.country,
			'region': region
		}
		statement = (
			' MATCH '
			'	(:Country { '
			'		name_lower : toLower(trim($country)) '
			' 	}) '
			'	<-[:IS_IN]-(region:Region { '
			'		name_lower: toLower(trim($region)) '
			' 	}) '
			' RETURN region.name '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(bolt_result, statement, parameters).single()
			if result:
				return result.value()
			else:
				return None

	def find_farm(self, region, farm):
		parameters = {
			'country': self.country,
			'region': region,
			'farm': farm
		}
		statement = (
			' MATCH '
			'	(:Country { '
			'		name_lower : toLower(trim($country)) '
			' 	}) '
			'	<-[:IS_IN]-(:Region { '
			'		name_lower: toLower(trim($region)) '
			'	}) '
			'	<-[:IS_IN]-(farm:Farm { '
			'		name_lower: toLower(trim($farm)) '
			'	}) '
			' RETURN farm.name '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(bolt_result, statement, parameters).single()
			if result:
				return result.value()
			else:
				return None

	def find_field(self, region, farm, field):
		parameters = {
			'country': self.country,
			'region': region,
			'farm': farm,
			'field': field
		}
		statement = (
			' MATCH '
			'	(:Country { '
			'		name_lower: toLower(trim($country)) '
			' 	}) '
			'	<-[:IS_IN]-(:Region { '
			'		name_lower: toLower(trim($region)) '
			'	}) '
			'	<-[:IS_IN]-(:Farm { '
			'		name_lower: toLower(trim($farm)) '
			'	}) '
			'	<-[:IS_IN]-(field:Field { '
			'		name_lower: toLower(trim($field)) '
			'	}) '
			' RETURN [ '
			'	field.uid, '
			'	field.name '
			' ] '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(bolt_result, statement, parameters).single()
			if result:
				return result.value()
			else:
				return None


class AddLocations:
	def __init__(self, username, country):
		self.username = username
		self.country = country

	def add_country(self):
		parameters = {
			'username': self.username,
			'country': self.country
		}
		statement = (
			' MATCH '
			'	(user: User  { '
			'		username_lower: toLower(trim($username)) '
			' 	}) '
			' MERGE '
			'	(country: Country { '
			'		name_lower: toLower(trim($country)) '
			' 	}) '
			'	ON MATCH SET '
			'		country.found = True '
			'	ON CREATE SET '
			'		country.found = False, '
			'		country.name = trim($country) '
			' FOREACH (n IN CASE WHEN country.found = False THEN [1] ELSE [] END | '
			'	MERGE '
			'		(user)-[:SUBMITTED]->(us: Submissions) '
			'	MERGE '
			'		(us)-[:SUBMITTED]->(ul:Locations) '
			'	MERGE '
			'		(ul)-[:SUBMITTED]->(uc:Countries) '
			'	CREATE '
			'		(uc)-[s:SUBMITTED {time: datetime.transaction().epochMillis}]->(country) '
			' ) '
			' RETURN country.name '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.write_transaction(bolt_result, statement, parameters).single()
			if result:
				return result.value()
			else:
				return None

	def add_region(self, region):
		parameters = {
			'username': self.username,
			'country': self.country,
			'region': region
		}
		statement = (
			' MATCH '
			'	(user: User  { '
			'		username_lower: toLower(trim($username)) '
			' 	}) '
			' MATCH '
			'	(country:Country { '
			'		name_lower: toLower(trim($country)) '
			' 	}) '
			' MERGE '
			'	(country) '
			'	<-[:IS_IN]-(region: Region { '
			'		name_lower: toLower(trim($region)) '
			' 	}) '
			'	ON MATCH SET '
			'		region.found = True '
			'	ON CREATE SET '
			'		region.found = False, '
			'		region.name = trim($region) '
			' FOREACH (n IN CASE WHEN region.found = False THEN [1] ELSE [] END | '
			'	MERGE '
			'		(user)-[:SUBMITTED]->(us: Submissions) '
			'	MERGE '
			'		(us)-[:SUBMITTED]->(ul:Locations) '
			'	MERGE '
			'		(ul)-[:SUBMITTED]->(ur:Regions) '
			'	CREATE '
			'		(ur)-[s:SUBMITTED {time: datetime.transaction().epochMillis}]->(region) '
			' ) '
			' RETURN region.name '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.write_transaction(bolt_result, statement, parameters).single()
			if result:
				return result.value()
			else:
				return None

	def add_farm(self, region, farm):
		parameters = {
			'username': self.username,
			'country': self.country,
			'region': region,
			'farm': farm
		}
		statement = (
			' MATCH '
			'	(user: User  { '
			'		username_lower: toLower(trim($username)) '
			' 	}) '
			' MATCH '
			'	(:Country { '
			'		name_lower: toLower(trim($country)) '
			' 	}) '
			'	<-[:IS_IN]-(region: Region { '
			'		name_lower: toLower(trim($region)) '
			' 	}) '
			' MERGE '
			'	(region) '
			'	<-[:IS_IN]-(farm:Farm {'
			'		name_lower: toLower(trim($farm)) '
			'	}) '
			'	ON MATCH SET '
			'		farm.found = True '
			'	ON CREATE SET '
			'		farm.found = False, '
			'		farm.name = trim($farm) '
			' FOREACH (n IN CASE WHEN farm.found = False THEN [1] ELSE [] END | '
			'	MERGE '
			'		(user)-[:SUBMITTED]->(us: Submissions) '
			'	MERGE '
			'		(us)-[:SUBMITTED]->(ul:Locations) '
			'	MERGE '
			'		(ul)-[:SUBMITTED]->(uf:Farms) '
			'	CREATE '
			'		(uf)-[s:SUBMITTED {time: datetime.transaction().epochMillis}]->(farm) '
			' ) '
			' RETURN farm.name '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.write_transaction(bolt_result, statement, parameters).single()
			if result:
				return result.value()
			else:
				return None

	def add_field(self, region, farm, field):
		parameters = {
			'username': self.username,
			'country': self.country,
			'region': region,
			'farm': farm,
			'field': field
		}
		statement = (
			' MATCH '
			'	(user: User  { '
			'		username_lower: toLower(trim($username)) '
			' 	}) '
			' MATCH '
			'	(:Country { '
			'		name_lower: toLower(trim($country)) '
			' 	}) '
			'	<-[:IS_IN]-(: Region { '
			'		name_lower: toLower(trim($region)) '
			' 	}) '
			'	<-[:IS_IN]-(farm:Farm {'
			'		name_lower: toLower(trim($farm)) '
			'	}) '
			' MERGE '
			'	(counter: Counter { '
			'		name: "field" '
			'	}) '
			'	ON CREATE SET '
			'		counter.count = 0 '
			' SET counter._LOCK_ = true '
			' MERGE '
			'	(farm) '
			'		<-[:IS_IN]-(field: Field: Item { '
			'			name_lower: toLower(trim($field)) '
			'	}) '
			'	ON MATCH SET '
			'		field.found = True '
			'	ON CREATE SET '
			'		counter.count = counter.count + 1, '
			'		counter._LOCK_ = false, '
			'		field.found = False, '
			'		field.name = trim($field), '
			'		field.uid = counter.count '
			' FOREACH (n IN CASE WHEN field.found = False THEN [1] ELSE [] END | '
			'	MERGE '
			'		(user)-[:SUBMITTED]->(us: Submissions) '
			'	MERGE '
			'		(us)-[:SUBMITTED]->(ul:Items) '
			'	MERGE '
			'		(ul)-[:SUBMITTED]->(uf:Fields) '
			'	CREATE '
			'		(uf)-[s:SUBMITTED {time: datetime.transaction().epochMillis}]->(field) '
			' ) '
			' RETURN [ '
			'	field.uid,'
			'	field.name '
			' ] '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.write_transaction(bolt_result, statement, parameters).single()
			if result:
				return result.value()
			else:
				return None


class FindFieldItems:
	def __init__(self, field_uid):
		self.field_uid = field_uid

	def find_block(self, block_name):
		with get_driver().session() as neo4j_session:
			parameters = {
				'field_uid': self.field_uid,
				'block_name': block_name,
			}
			statement = (
				' MATCH '
				'	(block:Block { '
				'		name_lower : toLower(trim($block_name)) '
				'	}) '
				'	-[:IS_IN]->(:FieldBlocks) '
				'	-[:IS_IN]->(:Field { '
				'		uid : toInteger($field_uid) '
				'	}) '
				' RETURN [ '
				'	block.uid, '
				'	block.name '
				' ] '
			)
			result = neo4j_session.read_transaction(bolt_result, statement, parameters).single()
			if result:
				return result.value()
			else:
				return None


class AddFieldItems:
	def __init__(self, username, field_uid):
		self.username = username
		self.field_uid = field_uid

	def add_block(self, block_name):
		with get_driver().session() as neo4j_session:
			parameters = {
				'username': self.username,
				'field_uid': self.field_uid,
				'block_name': block_name
			}
			statement = (
				' MATCH '
				'	(user: User { '
				'		username_lower: toLower(trim($username)) '
				'	}) '
				' MATCH '
				'	(field: Field { '
				'		uid: toInteger($field_uid) '
				'	}) '
				' MERGE '
				'	(fb: FieldBlocks) '
				'	-[:IS_IN]->(field) '
				' MERGE '
				'	(counter:Counter { '
				'		name: "block", '
				'		uid: (toInteger($field_uid) + "_block") '
				'	}) '
				'	-[:FOR]->(fb) '
				'	ON CREATE SET counter.count = 0 '
				' SET '
				'	counter._LOCK_ = true '
				' MERGE '
				'	(block: Block: Item { '
				'		name_lower: toLower(trim($block_name)) '
				'	}) '
				'	-[:IS_IN]->(fb) '
				'	ON MATCH SET '
				'		block.found = True '
				' 	ON CREATE SET '
				'		block.found = False, '
				'		counter.count = counter.count + 1, '
				'		block.name = trim($block_name), '
				'		block.uid = (field.uid + "_B" + counter.count), '
				'		block.id = counter.count, '
				'		block.varieties = CASE WHEN field.variety IS NOT NULL THEN [field.variety] END '
				' REMOVE '
				'	counter._LOCK_ '
				' FOREACH (n IN CASE WHEN block.found = False THEN [1] ELSE [] END | '
				'	MERGE '
				'		(user)-[:SUBMITTED]->(us: Submissions) '
				'	MERGE '
				'		(us)-[:SUBMITTED]->(ui:Items) '
				'	MERGE '
				'		(ui)-[:SUBMITTED]->(ub:Blocks) '
				'	CREATE '
				'		(ub)-[s:SUBMITTED {time: datetime.transaction().epochMillis}]->(block) '
				' ) '
				' RETURN [ '
				'	block.uid, '
				'	block.name '
				' ] '
			)
			result = neo4j_session.write_transaction(bolt_result, statement, parameters).single()
			if result:
				return result.value()
			else:
				return None

	def add_trees(self, tree_count, block_uid=None):
		parameters = {
			'username': self.username,
			'field_uid': self.field_uid,
			'tree_count': tree_count
		}
		statement = (
			' MATCH '
			'	(user: User { '
			'		username_lower: toLower(trim($username)) '
			'	}) '
			'  MATCH '
			'	(country: Country) '
			'	<-[:IS_IN]-(region: Region) '
			'	<-[:IS_IN]-(farm: Farm) '
			'	<-[:IS_IN]-(field: Field { '
			'		uid: $field_uid '	
			'	}) '
		)
		if block_uid:
			parameters['block_uid'] = block_uid
			statement += (
				' MATCH (block: Block { '
				'	uid: $block_uid'
				' }) '
				' MATCH '
				'	(input: Input {name_lower: "assign tree to block by name"}) '
				' MERGE '
				'	(input) '
				'	<-[: FOR_INPUT]-(ff: FieldInput) '
				'	<-[: FROM_FIELD]->(field) '
				' MERGE '
				'	(block_trees: BlockTrees)'
				'	-[:IS_IN]-> (block) '
				' MERGE '
				'	(block_tree_counter: Counter { '
				'		name: "tree", '
				'		uid: (block.uid + "_tree") '
				'	})-[:FOR]-> '
				'	(block_trees) '
				' ON CREATE SET '
				'	block_tree_counter.count = 0 '
				' SET block_tree_counter._LOCK_ = True '
			)
		statement += (
			' MERGE '
			'	(field_trees: FieldTrees)-[:IS_IN]-> '
			'	(field) '
			' MERGE '
			'	(field_tree_counter:Counter { '
			'		name: "tree", '
			'		uid: (field.uid + "_tree") '
			'	})-[:FOR]-> '
			'	(field_trees) '
			'	ON CREATE SET field_tree_counter.count = 0 '
			' MERGE '
			'	(user)-[: SUBMITTED]->(us: Submissions) '
			' MERGE '
			'	(us)-[: SUBMITTED]->(ui: Items) '
			' MERGE '
			'	(ui)-[: SUBMITTED]->(ut: Trees) '
			# create per user per field trees node (UserFieldTrees) linking these
			' MERGE '
			'	(ut)-[:SUBMITTED]-> '
			'	(uft:UserFieldTrees)-[:CONTRIBUTED]-> '
			'	(field_trees) '
			' SET '
			'	field_tree_counter._LOCK_ = true '
		)
		if block_uid:
			statement += (
				' MERGE '
				'	(us)-[: SUBMITTED]->(ur: Records) '
				' MERGE '
				'	(ur)-[: SUBMITTED]->(uff:UserFieldInput) '
				'	-[:CONTRIBUTED]->(ff) '
			)
		statement += (
			' WITH '
			'	user, uft, '
			'	country.name as country, '
			'	region.name as region, '
			'	farm.name as farm, '
			'	field, '
			'	field_tree_counter, field_trees '
		)
		if block_uid:
			statement += (
				' , block, block_tree_counter, block_trees, ff, uff, '
				' collect(field) + collect(block) as ancestors'
				' UNWIND ancestors as ancestor '
				' WITH '
				'	user, uft, '
				'	country, '
				'	region, '
				'	farm, '
				'	field, '
				'	field_tree_counter, field_trees, '
				'	block, block_tree_counter, block_trees, ff, uff, '
				'	COLLECT(DISTINCT ancestor.variety) as varieties '
			)
		else:
			statement += (
				' , collect(field.variety) as varieties '
			)
		statement += (
			' UNWIND range(1, $tree_count) as tree_count '
			'	SET '
			'		field_tree_counter.count = field_tree_counter.count + 1 '
			'	CREATE '
			'		(uft)-[:SUBMITTED {time: datetime.transaction().epochMillis}]-> '
			'		(tree: Tree: Item { '
			'			uid: (field.uid + "_T" + field_tree_counter.count), '
			'			id: field_tree_counter.count , '
			'			varieties: varieties '
			'		})-[:IS_IN]-> '
			'		(field_trees) '
			'	SET '
			'		field_tree_counter._LOCK_ = false '
		)
		if block_uid:
			statement += (
				' CREATE '
				'	(tree)-[:IS_IN]-> '
				'	(block_trees) '
				' SET block_tree_counter.count = block_tree_counter.count + 1 '
				' SET block_tree_counter._LOCK_ = false '
				' CREATE '
				'	(ff) '
				'	<-[:FOR_INPUT]-(if: ItemInput) '
				'	-[:FOR_ITEM]->(tree) '
				' CREATE '
				'	(if) '
				'	<-[:RECORD_FOR]-(r: Record { '
				'		found: False, '
				'		value: toLower(block.name), '
				'		person: user.name '
				'	}) '
				' CREATE '
				'	(uff)-[:SUBMITTED {time: datetime.transaction().epochMillis}]->(r) '
			)
		statement += (
			' RETURN { '
			'	Country: country, '
			'	Region: region, '
			'	Farm: farm, '
			'	Field: field.name, '
			'	`Field UID`: field.uid, '
		)
		if block_uid:
			statement += (
				'	Block: block.name, '
				'	`Block ID`: block.id, '
				'	source_level: "Block", '
				'	source_id: block.id, '
				'	Time: coalesce(field.time, block.time), '
		)
		else:
			statement += (
				'	source_level: "Field", '
				'	source_id: field.uid, '
				'	Time: field.time, '
			)
		statement += (
			'	UID: tree.uid,	'
			'	Varieties: tree.varieties '
			' } '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.write_transaction(bolt_result, statement, parameters)
			return result

# Samples are lowest "level" of item with a relationship "FROM" to any other item/s
	# The FROM relationship may be directed to other samples, allowing:
	#  - sub-sampling
	#  - relationships among samples (e.g. Sample from Leaf from Branch)
	# Not done here as only creating not updating "FROM" but need to consider the following:
	#  The FROM relationship can be updated, but only to provide greater precision:
	#   - Hierarchy of precision: Field/Block/Tree/Sample
	#     - i.e. Field to Block or from Block to Tree or from Tree to Sample
	#     - always ensure that the new parent item has FROM relationship directed toward any existing parent
	#       - with a longer path for new parent (i.e. greater precision)
	#   - This is important to not lose the information from initial registration
	#     - We only record user and time on registering, then all updates are recorded as data from trait "Assign to..."
	#   - To ensure no cycles created with updates to Inter-Sample relationships (Sample)-[:FROM]->(Sample):
	#     - FROM required on sample creation
	#     - FROM updates must create longer path to current parent item
	#       - i.e. new parent must have FROM/IS_IN path to current parent that does not include self node
	# All the above are handled by effectively only allowing a Field level sample to be reassigned
	@staticmethod
	# can have multiple field UIDs so not true to class
	def add_samples(
			username,
			level,
			country,
			region,
			farm,
			field_uid,
			field_uid_list,
			block_uid,
			block_id_list,
			tree_id_list,
			sample_id_list,
			per_item_count
	):
		parameters= {
			'username': username,
			'per_item_count': per_item_count
		}
		id_types = ('name', 'uid', 'uid_list', 'id_list')
		field_uid = int(field_uid) if field_uid else None
		filters = {
			'country': (country, id_types[0]),
			'region': (region, id_types[0]),
			'farm': (farm, id_types[0]),
			'field': (field_uid, id_types[1]) if field_uid else (field_uid_list, id_types[2]),
			'block': (block_uid, id_types[1]) if block_uid else(block_id_list, id_types[3]),
			'tree': (tree_id_list, id_types[3]),
			'sample': (sample_id_list, id_types[3])
		}
		statement = (
			' MATCH '
			'	(user: User { '
			'		username_lower: toLower(trim($username)) '
			'	}) '
			' MERGE '
			'	(user)-[: SUBMITTED]->(us: Submissions) '
			' MERGE '
			'	(us)-[: SUBMITTED]->(ui: Items) '
			' MERGE '
			'	(us)-[: SUBMITTED]->(user_records: Records) '
			' MERGE '
			'	(ui)-[: SUBMITTED]->(user_samples: Samples) '
			' WITH user, user_records, user_samples '
		)
		if level != 'field':
			statement += (
				' MATCH (input: Input { name_lower:  '
			)
			if level == 'block':
				statement += (
					' "assign sample to block(s) by name" '
				)
			elif level == 'tree':
				statement += (
					' "assign sample to tree(s) by id" '
				)
			elif level == 'sample':
				statement += (
					' "assign sample to sample(s) by id" '
				)
			statement += (
				' }) '

			)
			' WITH user, user_records, user_samples, input '
		statement += (
			' MATCH '
			' (country: Country) '
			' <-[: IS_IN]-(region: Region) '
			' <-[: IS_IN]-(farm: Farm) '
			' <-[: IS_IN]-(field: Field) '
		)
		if filters['block'][0]:
			statement += (
				' <-[:IS_IN]-(: FieldBlocks) '
				' <-[:IS_IN]-(block: Block) '
			)
		if filters['tree'][0]:
			statement += (
				' <-[:IS_IN*2]-(tree: Tree) '
			)
		if filters['sample'][0]:
			statement += (
				' <-[:FROM | IS_IN*]-(sample:Sample) '
			)
		if any([i[0] for i in list(filters.values())]):
			statement += (
				' WHERE '
			)
			filter_count = 0
			for key, value in list(filters.items()):
				if value:
					id_value = value[0]
					id_type = value[1]
					if id_value:
						parameters[key] = id_value
						filter_count += 1
						if filter_count != 1:
							statement += ' AND '
						if id_type == id_types[0]:
							statement += (
								key + '.name_lower = $' + key
							)
						elif id_type == id_types[1]:
							statement += (
								key + '.uid = $' + key
							)
						elif id_type == id_types[2]:
							statement += (
								key + '.uid IN $' + key
							)
						elif id_type == id_types[3]:
							statement += (
									key + '.id IN $' + key
							)
		if level == 'field':
			statement += (
				' WITH '
				' user, '	
				' user_samples, '
				' country, region, farm, '
				' field AS item, '
				' COLLECT(field.variety) as varieties '
			)
		elif level == 'block':
			if not filters['block'][0]:
				statement += (
					' MATCH (field) '
					'	<-[:IS_IN]-(:FieldBlocks) '
					'	<-[:IS_IN]-(block: Block) '
				)
			statement += (
				' MERGE '
				'	(input) '
				'	<-[: FOR_INPUT]-(ff: FieldInput) '
				'	<-[: FROM_FIELD]->(field) '			
				' WITH '
				'	user, '
				'	user_samples, '
				'	user_records, input, '
				'	country, region, farm, field, '
				'	block AS item, '
				'	COLLECT(field) + COLLECT(block) as ancestors '
				' UNWIND '
				'	ancestors as ancestor '
				' WITH '
				'	user, '
				' 	user_samples, '
				'	user_records, input, '
				' 	country, region, farm, field, '
				' 	item, '
				'	COLLECT(DISTINCT ancestor.variety) as varieties '
			)
		elif level == 'tree':
			if not filters['tree'][0]:
				if filters['block'][0]:
					statement += (
						' MATCH '
						'	(block) '
						'	<-[: IS_IN]-(: BlockTrees) '
						'	<-[: IS_IN]-(tree: Tree) '
					)
				else:
					statement += (
						' MATCH '
						'	(field) '
						'	<-[: IS_IN]-(: FieldTrees) '
						'	<-[: IS_IN]-(tree: Tree) '
					)
			if not filters['block'][0]:
				statement += (
					' OPTIONAL MATCH '
					'	(block: Block) '
					'	<-[:IS_IN]-(:BlockTrees) '
					'	<-[:IS_IN]-(tree) '
				)
			statement += (
				' MERGE '
				'	(input) '
				'	<-[: FOR_INPUT]-(ff: FieldInput) '
				'	<-[: FROM_FIELD]->(field) '
				' WITH '
				'	user, '
				'	user_samples, '
				'	user_records, input, '
				'	country, region, farm, field, block, '
				'	tree AS item, '
				'	COLLECT(field) + COLLECT(block) + COLLECT(tree) as ancestors '
				' UNWIND '
				'	ancestors as ancestor '
				' WITH '
				'	user, '
				'	user_samples, '
				'	user_records, input, '
				'	country, region, farm, field, block, '
				'	item,  '
				'	COLLECT(DISTINCT ancestor.variety) as varieties '
			)
		elif level == 'sample':
			if not filters['sample'][0]:
				if filters['tree'][0]:
					statement += (
						' MATCH '
						'	(tree) '
						'	<-[:FROM*]-(sample: Sample) '
					)
				elif filters['block'][0]:
					statement += (
						' MATCH '
						'	(block) '
						'	<-[:FROM | IS_IN *]-(sample: Sample) '
					)
				else:
					statement += (
						' MATCH '
						'	(field) '
						'	<-[:FROM | IS_IN *]-(sample: Sample) '
					)
			statement += (
				' MATCH '
				'	sample_path = (sample) '
				'		-[:FROM*]->(:ItemSamples) '
			)
			if not filters['tree'][0]:
				statement += (
					' OPTIONAL MATCH '
					'	(tree: Tree) '
					'	<-[:FROM*]-(sample) '
				)
			if not filters['block'][0]:
				statement += (
					' OPTIONAL MATCH '
					'	(block: Block) '
					'	<-[:FROM | IS_IN *]-(sample) '
				)
			statement += (
				' WITH '
				'	user, '
				'	user_samples, '
				'	user_records, input, '
				'	country, region, farm, field, '
				'	COLLECT(DISTINCT block) as blocks, '
				'	COLLECT(DISTINCT tree) as trees, '
				'	[s in nodes(sample_path) WHERE "Sample" in labels(s)] as source_samples, '
				'	sample AS item, '
				'	nodes(sample_path)[-2] AS item_samples, '
				'	COLLECT(field) + COLLECT(block) + COLLECT(tree) + '
				'		[s in nodes(sample_path) WHERE "Sample" in labels(s)] as ancestors '
				' UNWIND ancestors as ancestor '
				' WITH '
				'	user, '
				'	user_samples, item_samples, '
				'	user_records, input, '
				'	country, region, farm, field, '
				'	blocks, trees, source_samples, '
				'	item, '
				'	COLLECT(DISTINCT ancestor.variety) as varieties '
			)
		if level != 'sample':
			statement += (
				' MERGE '
				'	(item)<-[: FROM]-(item_samples: ItemSamples) '
			)
		statement += (
			' MERGE '
			'	(user_samples) '
			'	-[: SUBMITTED]->(user_item_samples: UserItemSamples)'
			'	-[: CONTRIBUTED]->(item_samples) '
			' MERGE '
		)
		if level == 'field':
			statement += (
				' (item) '
			)
		else:
			statement += (
				' (field) '
			)
		statement += (
			'	<-[:FOR]-(field_sample_counter: Counter { '
			'		name: "sample", '
		)
		if level == 'field':
			statement += (
				'		uid: (item.uid + "_sample") '
			)
		else:
			statement += (
				'		uid: (field.uid + "_sample") '
			)
		statement += (
			' 	}) '
			' ON CREATE SET field_sample_counter.count = 0 '
			' SET field_sample_counter._LOCK_ = true '
		)

		statement += (
			' WITH '
			'	user, '
			'	user_item_samples, '
			'	item_samples, '
			'	field_sample_counter, '
			'	country, region, farm, '
			'	item, varieties '
		)
		if level != 'field':

			statement += (
				'	,user_records, input '
			)
			if level == 'block':
				statement += (

					' ,field '
				)
			elif level == 'tree':
				statement += (
					' ,field, block '
				)
			elif level == 'sample':
				statement += (
					' ,field, '
					' blocks, trees, source_samples '
				)
			statement += (
				' ORDER BY field.uid, item.id '
				' MERGE '
				'	(input)<-[: FOR_INPUT]-(fi:FieldInput) '
				'	-[: FROM_FIELD]->(field) '
				' MERGE '
				'	(user_records)-[: SUBMITTED]->(ufi: UserFieldInput) '
				'	-[: CONTRIBUTED]->(fi) '
				' WITH '
				'	user, '
				'	input, '
				'	user_item_samples, '
				'	item_samples, '
				'	fi, ufi, '
				'	field_sample_counter, '
				'	country, region, farm, field, '
				'	item, varieties '
			)
			if level == 'tree':
				statement += (
					' , block '
				)
			elif level == 'sample':
				statement += (
					' , blocks, trees, source_samples '
				)
		else:
			statement += (
				' ORDER BY item.uid '
			)
		statement += (
			' UNWIND range(1, $per_item_count) as per_item_count '
			'	SET field_sample_counter.count = field_sample_counter.count + 1 '
			'	CREATE '
			'		(user_item_samples)-[:SUBMITTED {time: datetime.transaction().epochMillis}]-> '
			'		(sample: Sample: Item { '
		)
		if level == 'field':
			statement += (
				' uid: (item.uid + "_S" + field_sample_counter.count), '
			)
		else:
			statement += (
				' uid: (field.uid + "_S" + field_sample_counter.count), '
			)
		statement += (
			'			id: field_sample_counter.count '
			'		})-[:FROM]->(item_samples) '
			'	'
			'	SET field_sample_counter._LOCK_ = false '
		)
		if level != 'field':
			statement += (
				' CREATE '
				'	(fi)<-[:FOR_INPUT]-(ii: ItemInput)-[:FOR_ITEM]->(sample), '
				'	(ii)<-[:RECORD_FOR]-(r:Record { '
				'		found: False, '
				'		value: '
				'			CASE '
				'				WHEN input.name_lower CONTAINS "(s)" AND input.name_lower CONTAINS "by name" THEN '
				'					[item.name_lower] '
				' 				WHEN input.name_lower CONTAINS "(s)" AND input.name_lower CONTAINS "by id" THEN '
				'					[item.id] '			
				' 				WHEN input.name_lower CONTAINS "by id" THEN '
				'					item.id '
				' 				ELSE '  # input.name_lower CONTAINS "by name" THEN '
				'					item.name_lower '
				'			END '
				'		, '
				'		person: user.name '
				'	})'
				' CREATE (ufi)-[: SUBMITTED {time: datetime.transaction().epochMillis}]->(r) '
			)
		statement += (
			' RETURN { '
			'	UID: sample.uid, '
			'	Country: country.name, '
			'	Region: region.name, '
			'	Farm: farm.name, '
			'	Varieties: varieties, '
		)
		if level == 'field':
			statement += (
				' Field: item.name, '
				' `Field UID`: item.uid, '
				' source_level: "Field", '
				' source_id: item.uid '
			)
		else:
			statement += (
				'	Field: field.name, '
				'	`Field UID`: field.uid, '
			)
			if level == 'block':
				statement += (
					' Block: item.name, '
					' `Block ID`: item.id, '
					' source_level: "Block", '
					' source_id: item.id '
				)
			elif level == 'tree':
				statement += (
					' Block: block.name, '
					' `Block ID` : block.id, '
					' `Tree ID`: item.id, '
					' `Tree Name`: item.name, '
					' source_level: "Tree", '
					' source_id: item.id '
					)
			elif level == 'sample':
				statement += (
					' Blocks: [x IN blocks | x.name], '
					' `Block IDs` : [x IN blocks | x.id], '
					' `Tree IDs`: [x IN trees | x.id], '
					' `Tree Names`: [x IN trees | x.name], '
					' `Source Sample IDs` : [x IN source_samples | x.id], '
					' `Source Sample Names` : [x IN source_samples | x.name], ' 
					' source_level: "Sample", '
					' source_id: item.id, '
					' Unit: item.unit '
				)
		statement += (
			' } '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.write_transaction(bolt_result, statement, parameters)
			return result
