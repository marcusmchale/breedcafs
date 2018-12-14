# import os
# import unicodecsv as csv
# import cStringIO
# from app import app
from app.cypher import Cypher
from neo4j_driver import (
	# get_driver,
	neo4j_query
)
from flask import session
# from datetime import datetime
from neo4j_driver import get_driver


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
			'	country '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(neo4j_query, statement, parameters)
			return [record[0] for record in result]

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
			' RETURN region '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(neo4j_query, statement, parameters)
			return [record[0] for record in result]

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
			' RETURN farm '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(neo4j_query, statement, parameters)
			return [record[0] for record in result]

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
			' RETURN '
			'	field '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(neo4j_query, statement, parameters)
			return [record[0] for record in result]


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
			'		(uc)-[s:SUBMITTED {time: timestamp()}]->(country) '
			' ) '
			' RETURN country.name '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.write_transaction(neo4j_query, statement, parameters)
			return [record[0] for record in result]

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
			'		(ur)-[s:SUBMITTED {time: timestamp()}]->(region) '
			' ) '
			' RETURN region.name '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.write_transaction(neo4j_query, statement, parameters)
			return [record[0] for record in result]

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
			'		(uf)-[s:SUBMITTED {time: timestamp()}]->(farm) '
			' ) '
			' RETURN farm.name '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.write_transaction(neo4j_query, statement, parameters)
			return [record[0] for record in result]

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
			'		<-[:IS_IN]-(field: Field { '
			'			name_lower: $field '
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
			'		(us)-[:SUBMITTED]->(ul:Locations) '
			'	MERGE '
			'		(ul)-[:SUBMITTED]->(uf:Fields) '
			'	CREATE '
			'		(uf)-[s:SUBMITTED {time: timestamp()}]->(field) '
			' ) '
			' RETURN { '
			'	uid: field.uid,'
			'	name: field.name '
			' } '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.write_transaction(neo4j_query, statement, parameters)
			return [record[0] for record in result]


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
				'		name : $block_name'
				'	}) '
				'	-[:IS_IN]->(:FieldBlocks) '
				'	-[:IS_IN]->(:Field { '
				'		uid : toInteger($field_uid) '
				'	}) '
				' RETURN '
				'	block '
			)
			result = neo4j_session.read_transaction(neo4j_query, statement, parameters)
			return [record[0] for record in result]


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
				'		block.id = counter.count '
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
				'		(ub)-[s:SUBMITTED {time: timestamp()}]->(block) '
				' ) '
				' RETURN { '
				'	uid: block.uid, '
				'	name: block.name '
				' } '
			)
			result = neo4j_session.write_transaction(neo4j_query, statement, parameters)
			return [record[0] for record in result]

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
			'	(field: Field { '
			'		uid: $field_uid '	
			'	}) '
		)
		if block_uid:
			parameters['block_uid'] = block_uid
			statement += (
				' MATCH (block: Block { '
				'	uid: $block_uid'
				' }) '
				' MERGE '
				'	(block_trees: BlockTrees)-[:IS_IN]-> '
				'	(block) '
				' MERGE '
				'	(block_tree_counter: Counter { '
				'		name: "tree", '
				'		uid: (block.uid + "_tree") '
				'	})-[:FOR]-> '
				'	(block_trees) '
				' ON CREATE SET block_tree_counter.count = 0 '
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
			'	(user)-[:SUBMITTED]->(us: Submissions) '
			' MERGE '
			'	(us)-[:SUBMITTED]->(ui: Items) '
			' MERGE '
			'	(ui)-[:SUBMITTED]->(ut: Trees) '
			# create per user per field trees node (UserFieldTrees) linking these
			' MERGE '
			'	(ut)-[:SUBMITTED]-> '
			'	(uft:UserFieldTrees)-[:CONTRIBUTED]-> '
			'	(field_trees) '
			' SET '
			'	field_tree_counter._LOCK_ = true '
			' WITH '
			'	field_tree_counter, field_trees, field '
		)
		if block_uid:
			statement += (
				' , block_tree_counter, block_trees '
			)
		statement += (
			' UNWIND range(1, $tree_count) as tree_count '
			'	SET '
			'		field_tree_counter.count = field_tree_counter.count + 1 '
			'	CREATE '
			'		(uft)-[:SUBMITTED {time: timestamp()}]-> '
			'		(tree: Tree: Item { '
			'			uid: (field.uid + "_T" + field_tree_counter.count), '
			'			id: field_tree_counter.count '
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
			)
		statement += (
			'	RETURN count(tree) '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.write_transaction(neo4j_query, statement, parameters)
			return [record[0] for record in result]

	# Samples are lowest level of item with a relationship "FROM" to any other item/s
	# Samples are any subset of a tree,including:
	#  - those that are still developing (growing on the tree, e.g. Branch, Leaf)
	#  - those that are harvested; e.g. harvested Tissue for analyses, beans.
	# The FROM relationship may be directed to other samples, allowing:
	#  - sub-sampling
	#  - relationships among samples (e.g. Sample from Leaf from Branch)
	# The FROM relationship can be updated, but only to provide greater precision:
	#  - Hierarchy of precision: Field/Block/Tree/Sample
	#    - i.e. Field to Block or from Block to Tree or from Tree to Sample
	#    - always ensure that the new parent item has FROM relationship directed toward any existing parent
	#      - with a longer path for new parent (i.e. greater precision)
	#  - This is important to not lose the information from initial registration
	#    - We only record user and time on registering, then all updates are recorded as data from trait "Assign to..."
	#  - To ensure no cycles created with updates to Inter-Sample relationships (Sample)-[:FROM]->(Sample):
	#    - FROM required on sample creation
	#    - FROM updates must create longer path to current parent item
	#      - i.e. new parent must have FROM/IS_IN path to current parent that does not include self node
	def add_samples(self, level, replicates, sample_id_list=None, tree_id_list=None, block_uid=None):
		parameters= {
			'username': self.username,
			'field_uid': self.field_uid,
			'replicates': replicates
		}
		statement = (
			' MATCH '
			'	(user: User { '
			'		username_lower: toLower(trim($username)) '
			'	}) '
			'  MATCH '
			'	(field : Field { '
			'		uid: $field_uid '
			'	}) '
		)
		if level == 'block':
			statement += (
				' <-[: IS_IN]-(: FieldBlocks) '
				' <-[: IS_IN]-(item: Block '
			)
			if block_uid:
				statement += ' {uid: $block_uid} '
			statement += ' ) '
		elif level == 'tree':
			if block_uid:
				statement += (
					' <-[: IS_IN]-(: FieldBlocks) '
					' <-[: IS_IN]-(: Block ( '
					'	{uid: $block_uid} '
					' )<-[:IS_IN]-(: BlockTrees) '
					' <-[:IS_IN]-(item: Tree) '
				)
			else:
				statement += (
					' <-[:IS_IN]-(: FieldTrees) '
					' <-[:IS_IN]-(item: Tree) '
				)
			if tree_id_list:
				statement += (
					' WHERE item.id in $tree_id_list '
				)
		elif level == 'sample':
			if not any([block_uid, tree_id_list]):
				statement += (
					' <-[: FROM* | IS_IN* ]-(item_samples: ItemSamples) '
					' <-[: FROM]-(item: Sample) '
				)
			elif block_uid:
				parameters['block_uid'] = block_uid
				statement += (
					' <-[: IS_IN]-(: FieldBlocks) '
					' <-[: IS_IN]-(block: Block ( '
					'	{uid: $block_uid} '
					' ) '
				)
				if tree_id_list:
					parameters['tree_id_list'] = tree_id_list
					statement += (
						' <-[: IS_IN]-(: BlockTrees) '
						' <-[: IS_IN]-(tree: Tree) '
						' <-[:FROM]-(item_samples: ItemSamples) '
						' <-[:FROM]-(item: Sample) '
						' WHERE tree.id in $tree_id_list '
					)
				else:
					statement += (
						' <-[:FROM]-(item_samples: ItemSamples) '
						' <-[: FROM]-(item: Sample) '
					)
			elif tree_id_list:
				parameters['tree_id_list'] = tree_id_list
				statement += (
					' <-[:IS_IN]-(: FieldTrees) '
					' <-[:IS_IN]-(tree: Tree) '
					' <-[:FROM]-(item_samples: ItemSamples) '
					' <-[:FROM]-(item: Sample) '
					' WHERE tree.id in $tree_id_list '
				)
			if sample_id_list:
				parameters['sample_id_list'] = sample_id_list
				if tree_id_list:
					statement += (
						' AND '
					)
				else:
					statement += (
						' WHERE '
					)
				statement += (
					' item.id IN $sample_id_list '
				)
		statement += (
			' MERGE '
			'	(user)-[: SUBMITTED]->(us: Submissions) '
			' MERGE '
			'	(us)-[: SUBMITTED]->(ui: Items) '
			' MERGE '
			'	(ui)-[: SUBMITTED]->(user_samples: Samples) '
			' MERGE '
			'	(user_samples)-[: SUBMITTED]->(user_item_samples: UserItemSamples)'
			'	-[:CONTRIBUTED]->(item_samples) '
			' MERGE '
			'	(field)'
			'	<-[:FOR]-(field_sample_counter: Counter { '
			'		name :"sample", '
			'		uid: (field.uid + "_sample" '
			' 	}) '
			' ON CREATE SET field_sample_counter.count = 0 '
			' SET field_sample_counter._LOCK_ = true '
		)
		if level == 'field':
			statement += (
				' MERGE '
				'	(field) '
				'	<-[:FROM]-(item_samples: ItemSamples) '
			)
		elif level in ['block', 'tree']:
			statement += (
				' MERGE '
				' (item)'
				' <-[:FROM]-(item_samples: ItemSamples) '
			)
		if level == 'sample':
			statement += (
			' WITH '
			'	field.uid as field_uid, '
			'	user_item_samples, '
			'	item as item_samples, '
			'	field_sample_counter '
			)
		else:
			statement += (
				' WITH '
				'	field.uid as field_uid, '
				'	user_item_samples, '
				'	item_samples, '
				'	field_sample_counter '
			)
		if level == 'field':
			statement += (
				' ORDER BY field.uid '
			)
		else:
			statement += (
				' ORDER BY field.uid, item.id '
			)
		statement += (
			' UNWIND $replicates as replicate '
			'	SET field_sample_counter.count = field_sample_counter.count + 1 '
			'	CREATE '
			'		(sample: Sample: Item { '
			'			uid: (field.uid + "_S" + field_sample_counter.count), '
			'			id: field_sample_counter.count, '
			'		})-[:FROM]->(item_samples) '
			'	SET field_sample_counter._LOCK_ = false '
			' RETURN { '
			'	uid: sample.uid '
			' } '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.write_transaction(neo4j_query, statement, parameters)
			return [record[0] for record in result]
