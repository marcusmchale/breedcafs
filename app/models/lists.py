from app import logging

from neo4j_driver import (
	get_driver,
	bolt_result
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
			' MATCH (ig:InputGroup {name_lower: toLower(trim($input_group))}) '
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
			' RETURN DISTINCT [level.name_lower, level.name] '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				bolt_result,
				statement,
				parameters
			)
			result = [tuple(record[0]) for record in result]
			return sorted(result, key=lambda x: ["field", "block", "tree", "sample"].index(x[0]))

	@staticmethod
	def get_inputs(
			username=None,
			partner=None,
			input_group=None,
			inputs=None,
			record_type=None,
			item_level=None,
			inverse=False,
			details=False
	):
		parameters = {
			"username": username,
			"partner": partner,
			"input_group": input_group,
			"inputs": inputs,
			"record_type": record_type,
			"item_level": item_level
		}
		if username or partner or input_group:
			# these are used in relation to filtering groups
			# but want to still provide default groups "un-submitted" for username/partner filters
			statement = (
				' MATCH (ig:InputGroup) '
			)
			if username or partner:
				if username:
					statement += (
						' MATCH '
						'	(: User { '
						'		username_lower: toLower(trim($username)) '
						'	})-[:AFFILIATED { '
						'		data_shared: True '
						'	}]->(partner: Partner) '
					)
				else:  # partner is defined
					statement += (
						' MATCH '
						'	(partner: Partner { '
						'		name_lower: toLower(trim($partner)) '
						'	}) '
					)
				statement += (
						' OPTIONAL MATCH '
						'	(partner)<-[: AFFILIATED { '
						'		data_shared: True '
						'	}]-(: User) '
						'	-[: SUBMITTED]->(: Submissions) '
						'	-[: SUBMITTED]->(: InputGroups) '
						'	-[sub: SUBMITTED]->(ig) '
						' OPTIONAL MATCH '
						'	(ig)<-[unsub: SUBMITTED]-() '
						' WITH '
						'	[x IN collect([ig, sub]) WHERE x[1] IS NOT NULL | x[0]] as submitted, '
						'	[x IN collect([ig, sub]) WHERE x[1] IS NOT NULL | x[0].name_lower] as submitted_names, '
						'	[x IN collect([ig, unsub]) WHERE x[1] IS NULL | x[0]] as defaults '
						' WITH submitted + [x in defaults WHERE NOT x.name_lower IN submitted_names] as input_groups '
						' UNWIND input_groups as ig '
						' OPTIONAL MATCH (ig)<-[sub: SUBMITTED]-() '
					)
			else:
				statement += (
					' OPTIONAL MATCH '
					' (ig)<-[sub: SUBMITTED]-() '
				)
			if input_group:  # prefer the unregistered one here so that don't get another partners registered group
				statement += (
					' WITH ig, sub '
					' WHERE ig.name_lower = toLower(trim($input_group)) '
					' WITH ig, sub '
					' ORDER BY sub DESC LIMIT 1 '
				)
			else:
				statement += (
					' WITH ig, sub '
				)
			statement += (
				' MATCH '
				'	(ig)<-[in_group:IN_GROUP]-(input:Input) '
			)
		else:
			# in this case there is no association with groups requested
			# so we don't match the group or the in-group rel for order
			statement = (
				' MATCH (input:Input) '
			)
		if inputs:
			statement += (
				' WHERE input.name_lower IN extract(item IN $inputs | toLower(trim(item))) '
			)
		if inverse:
			# also losing the in_group properties here
			statement += (
				' WITH collect(input) as selected_inputs '
				' MATCH (input:Input) WHERE NOT input IN selected_inputs '

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
			if not inverse or (username or partner or input_group):
				# Here there is no sense to order by group
				statement += (
					' WITH input, in_group '
					' OPTIONAL MATCH '
					'	(input)-[: AT_LEVEL]->(level: ItemLevel) '
					' OPTIONAL MATCH '
					'	(input)-[: OF_TYPE]->(rt: RecordType) '
					' WITH input, in_group, level, rt '
					' ORDER BY in_group.position, input.name_lower '
					' WITH DISTINCT(input), collect(DISTINCT level.name_lower) as levels, rt '
				)
			else:
				statement += (
					' OPTIONAL MATCH '
					'	(level: ItemLevel)<-[: AT_LEVEL]-(input)-[: OF_TYPE]->(rt: RecordType) '
					' WITH DISTINCT input, collect(DISTINCT level.name_lower) as levels, rt '
					' ORDER BY input.name_lower '
				)
			statement += (
				' RETURN { '
				'	name: input.name, '
				'	name_lower: input.name_lower, '
				'	format: input.format, '
				'	minimum: input.minimum, '
				'	maximum: input.maximum, '
				'	details: input.details, '
				'	record_type: rt.name_lower, '
				'	levels: levels, '
				'	category_list: input.category_list '
				' } '
			)
		else:
			statement += (
				' RETURN [ '
				'	input.name_lower, '
				'	input.name '
				' ] '
			)
			if input_group and not inverse:
				statement += (
					' ORDER BY in_group.position, input.name_lower '
				)
			else:
				statement += (
					' ORDER BY input.name_lower '
				)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				bolt_result,
				statement,
				parameters
			)
		return [record[0] for record in result]

	@staticmethod
	def get_partners():
		parameters = {}
		query = (
			' MATCH (partner:Partner) '
			' RETURN [ '
			'	partner.name, '
			'	partner.fullname'
			' ] '
			' ORDER BY partner.fullname'
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				bolt_result,
				query,
				parameters
			)
		return [tuple(record[0]) for record in result]

	@staticmethod
	def get_countries():
		parameters = {}
		query = (
			' MATCH (country:Country) '
			' RETURN [ '
			'	country.name_lower, '
			'	country.name'
			' ] '
			' ORDER BY country.name'
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				bolt_result,
				query,
				parameters
			)
		return [tuple(record[0]) for record in result]

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
			' RETURN [ '
			'	region.name_lower, '
			'	region.name'
			' ] '
			' ORDER BY country.name '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				bolt_result,
				query,
				parameters
			)
		return [tuple(record[0]) for record in result]

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
			' RETURN [ '
			' 	farm.name_lower, '
			'	farm.name '
			' ] '
			' ORDER BY farm.name'
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				bolt_result,
				query,
				parameters)
		return [tuple(record[0]) for record in result]

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
			' RETURN [ '
			' 	toString(field.uid), '
			'	field.name '
			' ] '
			' ORDER BY field.name '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				bolt_result,
				query,
				parameters)
		return [tuple(record[0]) for record in result]

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
			' RETURN [ '
			' 	block.uid, '
			'	block.name '
			' ] '
			' ORDER BY block.name '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				bolt_result,
				query,
				parameters)
		return [tuple(record[0]) for record in result]

	@staticmethod
	def get_input_groups(
			item_level=None,
			record_type=None,
			username=None,
			partner=None
	):
		parameters = {
			"item_level": item_level,
			"record_type": record_type,
			"username": username,
			"partner": partner
		}
		statement = ' MATCH '
		if any([username, partner]):
			if username:
				statement += (
					' (p:Partner) '
					' <-[:AFFILIATED { '
					'	data_shared: True '
					' }]-(:User { '
					'	username_lower: toLower(trim($username)) '
					' }) '
				)
			else:
				statement += (
					' (p:Partner) '
				)
			statement += (
				' MATCH '
				'	(p) '
				'	<-[: AFFILIATED {data_shared: True}]-(: User) '
				'	-[: SUBMITTED]->(: Submissions) '
				'	-[: SUBMITTED]->(: InputGroups) '
				'	-[: SUBMITTED]->(ig: InputGroup) '
			)
			if partner:
				statement += (
					' WHERE p.name_lower = toLower(trim($partner)) '
				)
		else:
			statement += (
				' (ig: InputGroup) '
				' OPTIONAL MATCH '
				'	(ig)<-[s:SUBMITTED]-() '
				' WITH ig WHERE s IS NULL '
			)
		if item_level and record_type:
			statement += (
				' MATCH '
				'	(ig) '
				'	-[:AT_LEVEL]->(: ItemLevel {name_lower: toLower($item_level)}), '
				'	(ig)'
				'	<-[:IN_GROUP]-(input: Input)'
				'	-[:OF_TYPE]->(: RecordType { '
				'		name_lower: toLower($record_type) '
				'	}) '
			)
		elif item_level:
			statement += (
				' MATCH '
				'	(ig) '
				'	-[:AT_LEVEL]->(: ItemLevel {name_lower: toLower($item_level)}) '
			)
		elif record_type:
			statement += (
				' MATCH '
				'	(ig) '
				'	<-[:IN_GROUP]-(: Input) '
				'	-[:OF_TYPE]->(: RecordType {name_lower: toLower($record_type)}) '
			)
		statement += (
				' WITH DISTINCT ig '
				' RETURN [ '
				'	ig.name_lower, '
				'	ig.name '
				' ] '
				' ORDER BY ig.name_lower '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				bolt_result,
				statement,
				parameters
			)
		return [tuple(record[0]) for record in result]

	@staticmethod
	def get_record_types():
		statement = (
			' MATCH '
			'	(record_type: RecordType) '
			' RETURN [ '
			'	record_type.name_lower, '
			'	record_type.name'
			' ] '
			' ORDER BY record_type.name_lower '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				bolt_result,
				statement,
				None
			)
		return [tuple(record[0]) for record in result]

	@staticmethod
	def get_item_levels():
		statement = (
			' MATCH '
			'	(item_level: ItemLevel) '
			' RETURN [item_level.name_lower, item_level.name] '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				bolt_result,
				statement,
				None
			)
		result = [tuple(record[0]) for record in result]
		return sorted(result, key=lambda x: ["field", "block", "tree", "sample"].index(x[0]))

	@staticmethod
	def get_trials(
			country=None,
			region=None,
			farm=None,
			field_uid=None
	):
		statement = (
			' MATCH '
			'	(trial: Trial) '
		)
		parameters = {}
		# checking against string u'None' because wtforms is still broken
		# https://github.com/wtforms/wtforms/pull/288
		if field_uid and field_uid != 'None':
			parameters['field_uid'] = field_uid
			statement += (
				' -[:PERFORMED_IN]->(:Field {uid:$field_uid})'
			)
		elif farm and farm != 'None':
			parameters['farm'] = farm
			parameters['region'] = region
			parameters['country'] = country
			statement += (
				' -[:PERFORMED_IN | IS_IN*..2]->(:Farm {name_lower: toLower($farm)}) '
				' -[:IS_IN]->(:Region {name_lower: toLower($region)}) '
				' -[:IS_IN]->(:Country {name_lower: toLower($country)}) '
			)
		elif region and region != 'None':
			parameters['region'] = region
			parameters['country'] = country
			statement += (
				' -[:PERFORM_IN | IS_IN*..3]->(:Region {name_lower: toLower($region)}) '
				' -[:IS_IN]->(:Country {name_lower: toLower($country)}) '
			)
		elif country and country != 'None':
			parameters['country'] = country
			statement += (
				' -[:PERFORM_IN | IS_IN*..4]->(:Region {name_lower: toLower($region)}) '
				' -[:IS_IN]->(:Country {name_lower: toLower($country)}) '
			)
		statement += (
			' RETURN [ '
			'	trial.uid, '
			'	toString(trial.uid) + " - " + trial.name '
			' ] '
			' ORDER BY trial.uid '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				bolt_result,
				statement,
				parameters
			)
		return [tuple(record[0]) for record in result]


class ItemList:
	def __init__(self):
		pass

	@staticmethod
	def build_match_item_statement(record_data):
		parameters = {
			'item_level': record_data['item_level']
		}
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
		if record_data['item_level'] == 'field':
			statement += (
				' )<-[:IS_IN]-(item: Field '
			)
		else:
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
		if any([
			record_data['item_level'] == 'block',
			record_data['block_uid']
		]):
			statement += (
				' <-[: IS_IN]-(:FieldBlocks) '
			)
			if record_data['item_level'] == 'block':
				statement += (
					' <-[: IS_IN]-(item: Block '
				)
			else:
				statement += (
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
		if any([
			record_data['item_level'] == 'tree',
			record_data['tree_id_list']
		]):
			if record_data['block_uid']:
				parameters['block_uid'] = record_data['block_uid']
				statement += (
					' <-[: IS_IN]-(: BlockTrees)<-[: IS_IN]- '
				)
			else:
				statement += (
					' <-[ :IS_IN]-(: FieldTrees)<-[: IS_IN]- '
				)
			if record_data['item_level'] == 'tree':
				statement += (
					' (item: Tree) '
				)
				if record_data['tree_id_list']:
					parameters['tree_id_list'] = record_data['tree_id_list']
					statement += (
						' WHERE '
						' item.id in $tree_id_list '
					)
			else:
				statement += (
					' (tree:Tree) '
				)
		if record_data['item_level'] == 'sample':
			statement += (
				' <-[: FROM | IS_IN* ]-(: ItemSamples) '
				' <-[: FROM* ]-(item: Sample) '
			)
			if record_data['tree_id_list']:
				parameters['tree_id_list'] = record_data['tree_id_list']
				statement += (
					' WHERE tree.id in $tree_id_list '
				)
			if record_data['sample_id_list']:
				parameters['sample_id_list'] = record_data['sample_id_list']
				if record_data['tree_id_list']:
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
		# Optional matches
		if record_data['item_level'] in ['tree', 'sample']:
			if not record_data['block_uid']:
				statement += (
					' OPTIONAL MATCH '
					'	(item)-[: FROM | IS_IN*]->(block: Block) '
				)
			if record_data['item_level'] == 'sample':
				statement += (
					' WITH '
					'	item, '
					'	country, region, farm, '
					'	field, collect(distinct block) as blocks '
				)
				if not record_data['tree_id_list']:
					statement += (
						' OPTIONAL MATCH '
						'	(item)-[:FROM*]->(tree:Tree) '
						' WITH '
						' 	item, '
						'	country, region, farm, '
						'	field, blocks, '
						'	collect(distinct tree) as trees '
					)
				statement += (
					' OPTIONAL MATCH '
					'	(item)-[:FROM*]->(sample: Sample) '
					' WITH '
					'	item, '
					'	country, region, farm, '
					'	field, blocks, trees, '
					'	collect(distinct sample) as samples '
				)
		return statement, parameters

	def generate_id_list(self, record_data):
		# Some properties are thought to be particularly relevant to the user so are presented in item_details
		#  - Sources (where relevant): Country, Region, Farm, Field, Block, Tree, Sample
		# Further, some are retrieved along with inheritance from sources (e.g. Variety) and coalesced for item_details
		#  - Variety:
		#    Variety inheritance (unless set for item):
		#    - if a variety is set for a field:
		#      - inherit that value for any block/tree/sample in this field
		#    - if a variety is set for a block or a single variety is set for a collection of blocks:
		#      - inherit that value for any tree in this block or sample representing the block or collection
		#    - if a variety is set for a tree or a single variety is set for a collection of trees:
		#      - inherit that value for any sample representing this tree or collection
		#    - if a variety is set for a sample or a single variety is set for a collection of samples:
		#      - inherit that value for any sample representing this sample or collection
		# TODO consider preventing unexpected assignments in assign property *Upload models:
		#  - i.e. if source(s) contain(s) defined variety(ies)
		#    - only allow item specification to this (one of these)
		#
		# Time inheritance:
		#  - If time is set for a field:
		#    - inherit time (planted) for blocks in this field
		#    - inherit time (planted) for trees in this field (unless tree is in block and time is set for this block)
		#  - If time is set for a block or a single time is set for a collection of blocks:
		#    - inherit time (planted) for trees in this block or collection
		#  - If time is set for a sample or a single time is set for a collection of samples:
		#    - inherit time (harvest) for sub-samples from this sample
		#
		# Unit inheritance (samples only):
		#  - if a unit is set for a sample or a common unit is set for a collection of samples:
		#    - inherit that value for any sample representing this sample or collection
		statement, parameters = self.build_match_item_statement(record_data)
		statement += (
			' OPTIONAL MATCH '
			'	(item) '
			'	-[: OF_VARIETY]->(: FieldVariety) '
			'	-[: OF_VARIETY]->(variety: Variety) '
		)
		if parameters['item_level'] == 'block':
			statement += (
				' OPTIONAL MATCH '
				'	(item) '
				'	-[: OF_VARIETY]->(:FieldVariety) '
				'	-[: OF_VARIETY]->(variety:Variety) '
				' OPTIONAL MATCH '
				'	(field) '
				'	-[:OF_VARIETY]->(:FieldVariety) '
				'	-[:OF_VARIETY]->(field_variety: Variety) '
			)
			statement += (
				' WITH country, region, farm, field, item, '
				' coalesce(variety, field_variety) as variety '
			)
		if parameters['item_level'] == 'tree':
			statement += (
				' OPTIONAL MATCH '
				'	(item) '
				'	-[: OF_VARIETY]->(:FieldVariety) '
				'	-[: OF_VARIETY]->(variety:Variety) '
				' OPTIONAL MATCH '
				'	(block) '
				'	-[:OF_VARIETY]->(:FieldVariety) '
				'	-[:OF_VARIETY]->(block_variety: Variety) '
				' OPTIONAL MATCH '
				'	(field) '
				'	-[:OF_VARIETY]->(:FieldVariety) '
				'	-[:OF_VARIETY]->(field_variety: Variety) '
			)
			statement += (
				' WITH country, region, farm, field, block, item, '
				' coalesce(variety, block_variety, field_variety) as variety '
			)
		elif parameters['item_level'] == 'sample':
			statement += (
				' OPTIONAL MATCH '
				'	(item) '
				'	-[: OF_VARIETY]->(:FieldVariety) '
				'	-[: OF_VARIETY]->(variety:Variety) '
				' OPTIONAL MATCH '
				'	(tree) '
				'	-[:OF_VARIETY]->(:FieldVariety) '
				'	-[:OF_VARIETY]->(block_variety: Variety) '
				' OPTIONAL MATCH '
				'	(block) '
				'	-[:OF_VARIETY]->(:FieldVariety) '
				'	-[:OF_VARIETY]->(block_variety: Variety) '
				' OPTIONAL MATCH '
				'	(field) '
				'	-[:OF_VARIETY]->(:FieldVariety) '
				'	-[:OF_VARIETY]->(field_variety: Variety) '
			)



			statement += (
				' OPTIONAL MATCH '
				'	(item)-[:FROM*]->(source_sample: Sample) '
				''
				' OPTIONAL MATCH '
				'	(item) '
				'	<-[:FOR_ITEM]-(location_input: ItemInput) '
				'	-[:FOR_INPUT*]->(:Input {name_lower: "location"}), '
				'	(location_input) '
				'	<-[:RECORD_FOR]-(location_record: Record) '
				'	WHERE ( '
				'		location_record.start <= timestamp() '
				'		OR location_record.start IS NULL '
				'	) AND ( '
				'		location_record.end >= timestamp() '
				'		OR location_record.end IS NULL '
				'	) '
				' OPTIONAL MATCH '
				'	(item) '
				'	<-[:FOR_ITEM]-(coordinate_input: ItemInput) '
				'	-[:FOR_INPUT*]->(:Input {name_lower: "location (gps)"}), '
				'	(coordinate_input) '
				'	<-[:RECORD_FOR]-(coordinate_record: Record) '
				'	WHERE ( '
				'		coordinate_record.start <= timestamp() '
				'		OR coordinate_record.start IS NULL '
				'	) AND ( '
				'		coordinate_record.end >= timestamp() '
				'		OR coordinate_record.end IS NULL '
				'	) '
				' OPTIONAL MATCH '
				'	(item) '
				'	<-[:FOR_ITEM]-(temperature_input: ItemInput) '
				'	-[:FOR_INPUT*]->(:Input {name_lower: "storage temperature"}), '
				'	(temperature_input) '
				'	<-[:RECORD_FOR]-(temperature_record: Record) '
				'	WHERE ( '
				'		temperature_record.start <= timestamp() '
				'		OR temperature_record.start IS NULL '
				'	) AND ( '
				'		temperature_record.end >= timestamp() '
				'		OR temperature_record.end IS NULL '
				'	) '
				' OPTIONAL MATCH '
				'	(item) '
				'	<-[:FOR_ITEM]-(processing_input: ItemInput) '
				'	-[:FOR_INPUT*]->(:Input {name_lower: "processed state"}), '
				'	(processing_input) '
				'	<-[:RECORD_FOR]-(processing_record: Record) '
				'	WHERE ( '
				'		processing_record.start <= timestamp() '
				'		OR processing_record.start IS NULL '
				'	) AND ( '
				'		processing_record.end >= timestamp() '
				'		OR processing_record.end IS NULL '
				'	) '
				' WITH DISTINCT '
				' item, '
				' country, region, farm, field, '
				' collect(DISTINCT coalesce(variety.name, field_variety.name)) as varieties, '
				' collect(DISTINCT block.id) as block_ids, '
				' collect(DISTINCT block.name) as blocks, '
				' collect(DISTINCT tree.id) as tree_ids, '
				' collect(DISTINCT tree.name) as tree_names, '
				' collect(DISTINCT source_sample.id) as source_sample_ids, '
				
				
				
				
				
				# need to ensure these values are consistent in submission. taking first entry anyway
				' collect(source_sample.unit)[0] as source_sample_unit, '
				' collect(source_sample.time)[0] as source_sample_time '
			)
		statement += (
			' RETURN { '
			'	UID: item.uid, '
			'	Name: item.name, '
			'	Country: country.name, '
			'	Region: region.name, '
			'	Farm: farm.name, '
			'	Variety: varieties, '
		)
		if parameters['item_level'] == 'field':
			statement += (
				' Field: item.name '
			)
		else:
			statement += (
				' Field: field.name, '
				' `Field UID`: field.uid, '
			)
			if parameters['item_level'] == 'block':
				statement += (
					' Block: item.name '
				)
			elif parameters['item_level'] == 'tree':
				statement += (
					' Block: block.name, '
					' `Block ID` : block.id, '
					' `Tree ID`: item.id '
				)
			elif parameters['item_level'] == 'sample':
				statement += (
					' Block: blocks, '
					' `Block ID` : block_ids, '
					' `Tree ID`: tree_ids, '
					' `Tree Name(s)`: tree_names, '
					# first entry will be immediate parent sample value (item), subsequent are in no particular order
					' `Source Sample IDs`: source_sample_ids, '
					' Unit: coalesce(item.unit, source_sample_unit), '
					' `Harvest Time`: apoc.date.format(coalesce(item.harvest_time, source_sample_harvest_time)) '
				)
		statement += (
			' } '
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
			result = neo4j_session.read_transaction(
				bolt_result,
				statement,
				parameters)
		return result

	@staticmethod
	def get_fields(
			country=None,
			region=None,
			farm=None
	):
		parameters = {}
		query = 'MATCH (country:Country '
		if country:
			query += '{name_lower: toLower($country)}'
			parameters['country'] = country
		query += ')<-[:IS_IN]-(region:Region '
		if region:
			query += '{name_lower: toLower($region)}'
			parameters['region'] = region
		query += ')<-[:IS_IN]-(farm:Farm '
		if farm:
			query += '{name_lower: toLower($farm)}'
			parameters['farm'] = farm
		query += (
			' )<-[IS_IN]-(field:Field) '
			' OPTIONAL MATCH '
			'	(field) '
			'	<-[: FROM_FIELD]-(fit: FieldItemTreatment) '
			'	-[: FOR_TREATMENT]->(treatment:Treatment), '
			'	(fit)<-[: FOR_TREATMENT]-(tc:TreatmentCategory) ' 
			' WITH '      
			'	country, region, farm, field, '
			'	treatment, '
			'	collect(tc.category) as categories '
			' WITH { '
			'	Country : country.name, '
			'	Region : region.name, '
			'	Farm : farm.name, '
			'	Field : field.name, '
			'	UID : field.uid, '
			'	Treatments : collect({ '
			'		name: treatment.name, '
			'		categories: categories '
			'	})'
			' } as result '
			' RETURN result '
			' ORDER BY result["UID"] '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				bolt_result,
				query,
				parameters)
		return [record[0] for record in result]

	@staticmethod
	def get_blocks(
			country=None,
			region=None,
			farm=None,
			field_uid=None
	):
		parameters = {}
		query = 'MATCH (country:Country '
		if country:
			query += '{name_lower: toLower($country)}'
			parameters['country'] = country
		query += ')<-[:IS_IN]-(region:Region '
		if region:
			query += '{name_lower: toLower($region)}'
			parameters['region'] = region
		query += ')<-[:IS_IN]-(farm:Farm '
		if farm:
			query += '{name_lower: toLower($farm)}'
			parameters['farm'] = farm
		query += ')<-[:IS_IN]-(field:Field '
		if field_uid:
			query += '{uid: toInteger($field_uid)}'
			parameters['field_uid'] = field_uid
		query += (
			' )<-[:IS_IN]-(:FieldBlocks) '
			' <-[:IS_IN]-(block:Block) '
			' OPTIONAL MATCH '
			'	(field) '
			'	<-[: FROM_FIELD]-(fit: FieldItemTreatment) '
			'	-[: FOR_TREATMENT]->(treatment:Treatment), '
			'	(fit)<-[: FOR_TREATMENT]-(tc:TreatmentCategory), '
			'	(block) '
			'	<-[:IS_IN]-(:BlockTrees) '
			'	<-[:IS_IN]-(:Tree) '
			'	-[:IN_TREATMENT_CATEGORY]->(tc) '
			' WITH '
			'	country, region, farm, field, block, '
			'	treatment, '
			'	collect (distinct tc.category) as categories '
			' WITH { '
			'	UID: block.uid, '
			'	Block: block.name, '
			'	`Field UID` : field.uid, '
			'	Field: field.name, '
			'	Farm: farm.name, '
			'	Region: region.name, '
			'	Country: country.name, '
			'	Treatments: collect({ '
			'		name: treatment.name, '
			'		categories: categories '
			'	}) '
			' } as result, field.uid as field_uid, block.id as block_id  '
			' RETURN result '
			' ORDER BY field_uid, block_id '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				bolt_result,
				query,
				parameters)
		return [record[0] for record in result]
