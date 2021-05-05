from app import logging

from .neo4j_driver import (
	get_driver,
	single_record,
	list_records
)


class SelectionList:
	def __init__(
			self
	):
		pass

	@staticmethod
	def get_input_group_levels(
			input_group,
			partner=None,
			username=None
	):
		parameters = {
			"input_group": input_group,
			"username": username,
			"partner": partner
		}
		statement = (
			' MATCH (ig:InputGroup {id: $input_group}) '
		)
		if partner or username:
			if partner:
				statement += (
					' OPTIONAL MATCH (: Partner {name_lower: toLower(trim($partner))}) '
				)
			elif username:
				statement += (
					' MATCH (: User {username_lower: toLower(trim($username))}) '
					'	-[: AFFILIATED {data_shared: True}]->(partner: Partner) '
					' OPTIONAL MATCH (partner) '
				)
			statement += (
				'	<-[: AFFILIATED {data_shared: True}]-(: User) '
				'	-[: SUBMITTED]->(: Submissions) '
				'	-[: SUBMITTED]->(: InputGroups) '
				'	-[sub: SUBMITTED]->(ig) '
				' WITH ig ORDER BY sub LIMIT 1 '
			)
		else:
			statement += (
				' OPTIONAL MATCH (ig)<-[sub:SUBMITTED]-() '
				' WITH ig WHERE sub IS NULL '
			)
		statement += (
			' MATCH (ig)-[: AT_LEVEL]->(level: ItemLevel) '
			' RETURN DISTINCT level.name_lower as name_lower, level.name as name '
		)
		with get_driver().session() as neo4j_session:
			records = neo4j_session.read_transaction(
				list_records,
				statement,
				**parameters
			)
			result = [tuple(record.values()) for record in records]
			return sorted(result, key=lambda x: ["field", "block", "tree", "sample"].index(x[0]))

	@staticmethod
	def get_inputs(
			input_group=None,
			record_type=None,
			item_level=None,
			inverse=False,
			inverse_filter=None,
			details=False
	):
		parameters = {
			"input_group": input_group,
			"record_type": record_type,
			"item_level": item_level,
			"inverse_filter": inverse_filter
		}
		if input_group:
			# these are used in relation to filtering groups
			# but want to still provide default groups "un-submitted" for username/partner filters
			statement = (
				' MATCH'
				'	(input:Input)-[position:IN_GROUP]->(:InputGroup {id: $input_group}) '
				' WITH input ORDER BY position.position '
			)
		else:
			statement = (
				' MATCH '
				'	(input:Input) '
				' WITH input ORDER BY input.name_lower '
			)
		if inverse:
			if input_group:
				statement += (
					' WITH collect(input) as selected_inputs '
					' MATCH (input:Input) WHERE NOT input IN selected_inputs '
				)
			if inverse_filter:
				statement += (
					' MATCH '
					'	(input)-[position:IN_GROUP]->(:InputGroup {id: $inverse_filter}) '
					' WITH input ORDER BY position.position '
				)
			else:
				statement += (
					' WITH input '
					' ORDER BY input.name_lower '
				)
		if record_type:
			statement += (
				' MATCH '
				'	(input) '
				'	-[:OF_TYPE]->(: RecordType { '
				'		name_lower: toLower($record_type) '
				'	}) '
			)
		if item_level:
			statement += (
				' MATCH '
				'	(input) '
				'	-[:AT_LEVEL]->(: ItemLevel { '
				'		name_lower: toLower($item_level) '
				'	}) '
			)
		if details:
			statement += (
				' OPTIONAL MATCH '
				'	(input)-[: AT_LEVEL]->(level: ItemLevel) '
				' OPTIONAL MATCH '
				'	(input)-[: OF_TYPE]->(record_type: RecordType) '
				' WITH input, collect(DISTINCT level.name_lower) as levels, record_type '
			)
			statement += (
				' RETURN '
				'	input.name as name, '
				'	input.name_lower as name_lower, '
				'	input.format as format, '
				'	input.minimum as minimum, '
				'	input.maximum as maximum, '
				'	input.details as details, '
				'	input.category_list as category_list, '
				'	record_type.name_lower as record_type, '
				'	levels as levels '
			)
		else:
			statement += (
				' RETURN '
				'	input.name_lower as name_lower, '
				'	input.name as name '
			)
		with get_driver().session() as neo4j_session:
			records = neo4j_session.read_transaction(
				list_records,
				statement,
				**parameters
			)
		if details:
			return records
		else:
			return [tuple(record.values()) for record in records]

	@staticmethod
	def get_partners():
		parameters = {}
		query = (
			' MATCH (partner:Partner) '
			' RETURN '
			'	partner.name as name, '
			'	partner.fullname as fullname'
			' ORDER BY partner.fullname'
		)
		with get_driver().session() as neo4j_session:
			records = neo4j_session.read_transaction(
				list_records,
				query,
				**parameters
			)
		return [tuple(record.values()) for record in records]

	@staticmethod
	def get_countries():
		parameters = {}
		query = (
			' MATCH (country:Country) '
			' RETURN '
			'	country.name_lower as name_lower, '
			'	country.name as name '
			' ORDER BY country.name'
		)
		with get_driver().session() as neo4j_session:
			records = neo4j_session.read_transaction(
				list_records,
				query,
				**parameters
			)
		return [tuple(record.values()) for record in records]

	@staticmethod
	def get_regions(country=None):
		parameters = {}
		query = (
			' MATCH (country:Country) '
			'	<-[:IS_IN]-(region:Region) '
		)
		if country:
			query += (
				' WHERE country.name_lower = $country'
			)
			parameters['country'] = country
		query += (
			' RETURN '
			'	region.name_lower as name_lower, '
			'	region.name as name'
			' ORDER BY country.name '
		)
		with get_driver().session() as neo4j_session:
			records = neo4j_session.read_transaction(
				list_records,
				query,
				**parameters
			)
		return [tuple(record.values()) for record in records]

	@staticmethod
	def get_farms(
			country=None,
			region=None
	):
		parameters = {}
		query = 'MATCH (:Country '
		if country:
			query += '{name_lower: toLower($country)}'
			parameters['country'] = country
		query += ')<-[:IS_IN]-(:Region '
		if region:
			query += '{name_lower: toLower($region)}'
			parameters['region'] = region
		query += (
			')<-[:IS_IN]-(farm:Farm)'
			' RETURN '
			' 	farm.name_lower as name_lower, '
			'	farm.name as name '
			' ORDER BY farm.name'
		)
		with get_driver().session() as neo4j_session:
			records = neo4j_session.read_transaction(
				list_records,
				query,
				**parameters)
		return [tuple(record.values()) for record in records]

	@staticmethod
	def get_fields(
			country=None,
			region=None,
			farm=None
	):
		parameters = {}
		query = 'MATCH (:Country '
		if country:
			query += '{name_lower: toLower($country)}'
			parameters['country'] = country
		query += ')<-[:IS_IN]-(:Region '
		if region:
			query += '{name_lower: toLower($region)}'
			parameters['region'] = region
		query += ')<-[:IS_IN]-(:Farm '
		if farm:
			query += '{name_lower: toLower($farm)}'
			parameters['farm'] = farm
		query += (
			' )<-[:IS_IN]-(field:Field) '
			' RETURN '
			' 	toString(field.uid) as uid, '
			'	field.name as name'
			' ORDER BY field.name '
		)
		with get_driver().session() as neo4j_session:
			records = neo4j_session.read_transaction(
				list_records,
				query,
				**parameters)
		return [tuple(record.values()) for record in records]

	@staticmethod
	def get_blocks(
			country=None,
			region=None,
			farm=None,
			field_uid=None
	):
		parameters = {}
		query = 'MATCH (:Country '
		if country:
			query += '{name_lower: toLower($country)} '
			parameters['country'] = country
		query += ')<-[:IS_IN]-(:Region '
		if region:
			query += '{name_lower: toLower($region)} '
			parameters['region'] = region
		query += ')<-[:IS_IN]-(:Farm '
		if farm:
			query += '{name_lower: toLower($farm)} '
			parameters['farm'] = farm
		query += ')<-[:IS_IN]-(:Field '
		if field_uid:
			query += ' {uid: toInteger($field_uid)} '
			parameters['field_uid'] = field_uid
		query += (
			' )<-[:IS_IN]-(:FieldBlocks) '
			' <-[:IS_IN]-(block:Block)'
			' RETURN '
			' 	block.uid as uid, '
			'	block.name as name '
			' ORDER BY block.name '
		)
		with get_driver().session() as neo4j_session:
			records = neo4j_session.read_transaction(
				list_records,
				query,
				**parameters)
		return [tuple(record.values()) for record in records]

	@staticmethod
	def get_input_groups(
			item_level=None,
			record_type=None,
			partner=None,
			username=None,
			include_defaults=False
	):
		parameters = {
			"item_level": item_level,
			"record_type": record_type,
			"partner": partner,
			"username": username
		}
		statement = (
			' MATCH '
			'	(ig: InputGroup) '
		)
		if item_level:
			statement += (
				' MATCH (ig)-[:AT_LEVEL]->(:ItemLevel {name_lower:toLower($item_level)}) '
			)
		if record_type:
			statement += (
				' MATCH '
				'	(ig) '
				'	<-[:IN_GROUP]-(: Input) '
				'	-[:OF_TYPE]->(: RecordType {name_lower: toLower($record_type)}) '
			)
		if partner or username:
			match = ' MATCH '
			if include_defaults:
				match = ' OPTIONAL MATCH '
			statement += match + (
				'	(ig)'
				'	<-[: SUBMITTED]-(: InputGroups) '
				'	<-[: SUBMITTED]-(: Submissions) '
				'	<-[: SUBMITTED]-(: User) '
				'	-[:AFFILIATED {data_shared: True}]->(partner: Partner) '
			)
			if partner:
				statement += (
					' WHERE partner.name_lower = toLower($partner) '
				)
			if username:
				statement += match + (
					'	(partner) '
					'	<-[:AFFILIATED {data_shared: True}]-(user: User { '
					'		username_lower: toLower($username) '
					'	}) '
				)
			if include_defaults:
				statement += (
					' WITH ig, partner '
					'	WHERE partner IS NULL '
					'	OR user IS NOT NULL '
				)
			else:
				statement += (
					' WITH ig, partner '
				)
		else:
			statement += (
				' OPTIONAL MATCH (ig)<-[sub: SUBMITTED]-() '
				' WITH ig WHERE sub IS NULL '
			)
		if partner or username and include_defaults:
			statement += (
				' RETURN '
				'	toString(ig.id) as id, '	
				'	CASE '
				'		WHEN partner IS NULL THEN ig.name + " (default)" '
				'		ELSE ig.name '
				'	END as name '
				' ORDER BY ig.name_lower '
			)
		else:
			statement += (
				' RETURN '
				'	toString(ig.id) as id, '
				'	ig.name as name '
				' ORDER BY ig.name_lower '
		)
		with get_driver().session() as neo4j_session:
			records = neo4j_session.read_transaction(
				list_records,
				statement,
				**parameters
			)
		return [tuple(record.values()) for record in records]

	@staticmethod
	def get_record_types():
		statement = (
			' MATCH '
			'	(record_type: RecordType) '
			' RETURN '
			'	record_type.name_lower as name_lower, '
			'	record_type.name as name '
			' ORDER BY record_type.name_lower '
		)
		with get_driver().session() as neo4j_session:
			records = neo4j_session.read_transaction(
				list_records,
				statement
			)
		return [tuple(record.values()) for record in records]

	@staticmethod
	def get_item_levels():
		statement = (
			' MATCH '
			'	(item_level: ItemLevel) '
			' RETURN item_level.name_lower as name_lower, item_level.name as name '
		)
		with get_driver().session() as neo4j_session:
			records = neo4j_session.read_transaction(
				list_records,
				statement
			)
		result = [tuple(record.values()) for record in records]
		return sorted(result, key=lambda x: ["field", "block", "tree", "sample"].index(x[0]))


class ItemList:
	def __init__(self):
		pass

	@staticmethod
	def build_match_item_statement(record_data):
		clause_tuples = [
			(record_data['field_uid_list'] and not record_data['field_uid'], ' field.uid IN $field_uid_list '),
			(record_data['block_id_list'] and not record_data['block_uid'], ' block.id IN $block_id_list '),
			(record_data['tree_id_list'], ' tree.id IN $tree_id_list '),
			(record_data['sample_id_list'], ' sample.id IN $sample_id_list')
		]
		parameters = {
			'item_level': record_data['item_level'],
		}
		if clause_tuples[0][0]:
			parameters['field_uid_list'] = record_data['field_uid_list']
		if clause_tuples[1][0]:
			parameters['block_id_list'] = record_data['block_id_list']
		if clause_tuples[2][0]:
			parameters['tree_id_list'] = record_data['tree_id_list']
		if clause_tuples[3][0]:
			parameters['sample_id_list'] = record_data['sample_id_list']
		statement = (
			' MATCH (country: Country '
		)
		if record_data['country']:
			parameters['country'] = record_data['country']
			statement += (
				' {name_lower: toLower($country)} '
			)
		statement += (
			' )<-[: IS_IN]-(region: Region '
		)
		if record_data['region']:
			parameters['region'] = record_data['region']
			statement += (
				' {name_lower: toLower($region)} '
			)
		statement += (
			' )<-[: IS_IN]-(farm: Farm '
		)
		if record_data['farm']:
			parameters['farm'] = record_data['farm']
			statement += (
				' {name_lower: toLower($farm)} '
			)
		statement += (
			' )<-[: IS_IN]-(field: Field '
		)
		if record_data['field_uid']:
			parameters['field_uid'] = record_data['field_uid']
			statement += (
				' {uid: $field_uid} '
			)
		statement += (
			' ) '
		)
		if record_data['item_level'] == 'field':
			if clause_tuples[0][0]:
				statement += ' WHERE ' + clause_tuples[0][1]
			statement += (
				' WITH '
				' country, region, farm, field as item '
			)
		if any([
			record_data['item_level'] == 'block',
			record_data['block_uid'],
			record_data['block_id_list']
		]):
			statement += (
				' <-[: IS_IN]-(:FieldBlocks) '
				' <-[: IS_IN]-(block: Block '
			)
			if record_data['block_uid']:
				parameters['block_uid'] = record_data['block_uid']
				statement += (
					' {uid: $block_uid} '
				)
			statement += (
				' ) '
			)
			if record_data['item_level'] == 'block':
				if any([i[0] for i in clause_tuples[0:2]]):
					statement += (
						' WHERE '
					)
					statement += (
						' AND '.join(
							[i[1] for i in clause_tuples[0:2] if i[0]]
						)
					)
				statement += (
					' WITH '
					' country, region, farm, field, block as item '
				)
		if any([
			record_data['item_level'] == 'tree',
			record_data['tree_id_list']
		]):
			if record_data['block_uid'] or record_data['block_id_list']:
				parameters['block_uid'] = record_data['block_uid']
				statement += (
					' <-[: IS_IN]-(: BlockTrees)<-[: IS_IN]- '
				)
			else:
				statement += (
					' <-[ :IS_IN]-(: FieldTrees)<-[: IS_IN]- '
				)
			statement += (
				' (tree: Tree) '
			)
			if record_data['item_level'] == 'tree':
				if any([i[0] for i in clause_tuples[0:3]]):
					statement += (
						' WHERE '
					)
					statement += (
						' AND '.join(
							[i[1] for i in clause_tuples[0:3] if i[0]]
						)
					)
				if any([record_data['block_uid'], record_data['block_id_list']]):
					statement += (
						' WITH '
						' country, region, farm, field, block, tree as item '
					)
				else:
					statement += (
						' WITH '
						' country, region, farm, field, tree as item '
					)
		if record_data['item_level'] == 'sample':
			statement += (
				' <-[: FROM | IS_IN* ]-(: ItemSamples) '
				' <-[: FROM* ]-(sample: Sample) '
			)
			if record_data['item_level'] == 'sample':
				if any([i[0] for i in clause_tuples[0:4]]):
					statement += (
						' WHERE '
					)
					statement += (
						' AND '.join(
							[i[1] for i in clause_tuples[0:4] if i[0]]
						)
					)
				statement += (
					' WITH '
					'	country, region, farm, field, sample as item '
				)
				if any([record_data['block_uid'], record_data['block_id_list']]):
					statement += (
						', block '
					)
				if record_data['tree_id_list']:
					statement += (
						', tree'
					)
		# Optional matches
		if record_data['item_level'] in ['tree', 'sample']:
			if not any([record_data['block_uid'], record_data['block_id_list']]):
				statement += (
					' OPTIONAL MATCH '
					'	(item)-[: FROM | IS_IN*]->(block: Block) '
				)
			if record_data['item_level'] == 'sample':
				statement += (
					' WITH '
					'	item, '
					'	country, region, farm, '
					'	field,'
					'	block '
					' ORDER BY field.uid, block.id '
					' WITH '
					'	item, '
					'	country, region, farm, '
					'	field, '
					'	collect(distinct block) as blocks '
				)
				if not record_data['tree_id_list']:
					statement += (
						' OPTIONAL MATCH '
						'	(item)-[:FROM*]->(tree:Tree) '
						' WITH '
						' 	item, '
						'	country, region, farm, '
						'	field, blocks, '
						'	tree '
						' ORDER BY field.uid, tree.id '
						' WITH '
						' 	item, '
						'	country, region, farm, '
						'	field, blocks, '
						'	collect(distinct tree) as trees '
					)
				statement += (
					' OPTIONAL MATCH '
					'	p = (item)-[:FROM*]->(sample: Sample) '
					' WITH '
					'	item, '
					'	country, region, farm,'
					'	field, blocks, trees,'
					'	sample '
					' ORDER BY field.uid, item.id, length(p), sample.id '
					' WITH '
					'	item, '
					'	country, region, farm, '
					'	field, blocks, trees, '
					'	collect(distinct sample) as samples '
					' OPTIONAL MATCH '
					'	(item)-[:FROM]->(source_sample: Sample) '
					' WITH '
					'	item, '
					'	country, region, farm, '
					'	field, blocks, trees, samples, '
					'	source_sample '
					' ORDER BY field.uid, item.id, source_sample.id '
					' WITH '
					'	item, '
					'	country, region, farm, '
					'	field, blocks, trees, samples, '
					'	collect(distinct source_sample) as source_samples '
				)
		return statement, parameters

	def generate_id_list(self, record_data):
		statement, parameters = self.build_match_item_statement(record_data)
		statement += (
			' RETURN '
			'	item.uid as UID, '
			'	item.name as Name, '
			'	item.varieties as Varieties, '
			'	country.name as Country, '
			'	region.name as Region, '
			'	farm.name as Farm, '
		)
		if parameters['item_level'] == 'field':
			statement += (
				' item.name as Field, '
				' item.elevation as Elevation, '
				' apoc.date.format(item.time, "ms", "yyyy-MM-dd HH:mm") as Time'
			)
		else:
			statement += (
				' field.name as Field, '
				' field.uid as `Field UID`, '
			)
			if parameters['item_level'] == 'block':
				statement += (
					' item.name as Block, '
					' apoc.date.format(coalesce(item.time, field.time), "ms", "yyyy-MM-dd HH:mm") as Time'
				)
			elif parameters['item_level'] == 'tree':
				statement += (
					' block.name as Block, '
					' block.id as `Block ID`, '
					' item.id as `Tree ID`, '
					' item.row as Row, '
					' item.column as Column, '
					' apoc.date.format(coalesce(item.time, block.time, field.time), "ms", "yyyy-MM-dd HH:mm") as Time'
				)
			elif parameters['item_level'] == 'sample':
				statement += (
					' [x in blocks | x.name] as Blocks, '
					' [x in blocks | x.id] as `Block IDs`, '
					' [x in trees | x.id] as `Tree IDs`, '
					' [x in trees | x.name] as `Tree Names`, '
					' [x in samples | x.id] as `Source Sample IDs`, '
					' [x in samples | x.name] as `Source Sample Names`, '
					' item.unit as Unit, '
					' CASE '
					'	WHEN size(source_samples) <> 0 THEN "Sample" '
					'	WHEN size(trees) <> 0 THEN "Tree" '
					'	WHEN size(blocks) <> 0 THEN "Block" '
					'	ELSE "Field" '
					'	END as source_level, '
					' CASE '
					'	WHEN size(source_samples) <> 0 THEN [x in source_samples | x.id ] '
					'	WHEN size(trees) <> 0 THEN [x in trees | x.id ] '
					'	WHEN size(blocks) <> 0 THEN [x in blocks | x.id ] '
					'	ELSE [field.uid]  '
					'	END as source_ids, '
					' apoc.date.format(coalesce(item.time, samples[0].time), "ms", "yyyy-MM-dd HH:mm") as Time '
				)
		statement += (
			' ORDER BY '
		)
		if parameters['item_level'] == 'field':
			statement += (
				' item.uid '
			)
		else:
			statement += (
				' field.uid, item.id '
			)
		with get_driver().session() as neo4j_session:
			records = neo4j_session.read_transaction(
				list_records,
				statement,
				**parameters)
		return records
