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
	def get_inputs(
			record_type=None,
			item_level=None,
			input_group=None,
			inputs=None,
			partner=None,
			username=None,
			inverse=False,
			details=False
	):
		import pdb;
		pdb.set_trace()
		parameters = {
			"record_type": record_type,
			"item_level": item_level,
			"input_group": input_group,
			"inputs": inputs,
			"partner": partner,
			"username": username
		}
		if record_type and item_level:
			statement = (
				' MATCH '
				'	(: RecordType {name_lower: toLower($record_type)})'
				'	<-[:OF_TYPE]-(input: Input) '
				'	-[:AT_LEVEL]->(: ItemLevel {name_lower: toLower($item_level)}) '
			)
		elif record_type:
			statement = (
				' MATCH '
				'	(: RecordType {name_lower: toLower($record_type)})'
				'	<-[:OF_TYPE]-(input: Input) '
			)
		elif item_level:
			statement = (
				' MATCH '
				'	(input: Input) '
				'	-[:AT_LEVEL]->(: ItemLevel { '
				'		name_lower: toLower($item_level) '
				'	}) '
			)
		else:
			statement = (
				' MATCH (input: Input) '
			)
		if partner and input_group:
			statement += (
				' MATCH '
				'	(partner: Partner { '
				'		name_lower: toLower(trim($partner)) '
				'	}) '
				'	<-[:AFFILIATED { '
				'		data_shared: True '
				'	}]-(: User) '
				'	-[: SUBMITTED]->(:  Submissions) '
				'	-[: SUBMITTED]->(: InputGroups) '
				'	-[: SUBMITTED]->(: InputGroup {'
				'		name_lower: toLower(trim($input_group)) '
				'	}) '
				'	<-[in_group:IN_GROUP]-(input) '
			)
		elif partner:
			statement += (
				' MATCH '
				'	(partner: Partner {'
				'		name_lower: toLower(trim($partner)) '
				'	}) '
				'	<-[:AFFILIATED { '
				'		data_shared: True '
				'	}]-(: User) '
				'	-[: SUBMITTED]->(:  Submissions) '
				'	-[: SUBMITTED]->(: InputGroups) '
				'	-[: SUBMITTED]->(: InputGroup) '
				'	<-[in_group:IN_GROUP]-(input) '
			)
		elif username and input_group:
			# handle defaulting to generic "non-partner submitted" group where not found
			statement += (
				' MATCH '
				'	(: User { '
				'		username_lower: toLower(trim($username)) '
				'	})-[:AFFILATED {data_shared: True})->(partner :Partner) '
				' MATCH '
				'	(input) '
				'	-[in_group: IN_GROUP]->(ig:InputGroup {'
				'		name_lower: toLower(trim($input_group)) '
				'	}) '
				' OPTIONAL MATCH '
				'	(partner)<-[:AFFILIATED { '
				'		data_shared:True'
				'	}]-(:User) '
				'	-[:SUBMITTED]->(:Submissions) '
				'	-[:SUBMITTED]->(:InputGroups) '
				'	-[:SUBMITTED]->(ig) '
				' WITH '
				'	input, '

			)
		elif username:
			statement += (

				# TODO this is broken , need to first find partner then the partner associated input groups
				' MATCH '
				'	(: User {username_lower: toLower(trim($username))}) '
				'	-[: SUBMITTED]->(:  Submissions) '
				'	-[: SUBMITTED]->(: InputGroups) '
				'	-[: SUBMITTED]->(: InputGroup) '
				'	<-[in_group:IN_GROUP]-(input) '
			)
		elif input_group:
			statement += (
				' MATCH '
				'	(ig: InputGroup { '
				'		name_lower: toLower(trim($input_group)) '
				'	}) '
				'	<-[in_group:IN_GROUP]-(input) '
				' OPTIONAL MATCH '
				'	()-[s:SUBMITTED]->(ig) '
				' WITH input, in_group WHERE s IS NULL '
			)
		if inputs:
			parameters['inputs'] = inputs
			statement += (
				' WHERE input.name_lower IN extract(item IN $inputs | toLower(trim(item))) '
			)
		if inverse:
			statement += (
				' WITH collect(input) as selected_inputs '
				' MATCH (input: Input) '
				'	WHERE NOT input in selected_inputs '
			)
		if details:
			statement += (
				' WITH input '
				' MATCH '
				'	(level: ItemLevel)<-[: AT_LEVEL]-(input)-[: OF_TYPE]->(rt: RecordType) '
				' RETURN { '
				'	name: input.name, '
				'	name_lower: input.name_lower, '
				'	format: input.format, '
				'	details: input.details, '
				'	record_type: rt.name_lower, '
				'	levels: collect(level.name) '
				' } '
			)
		else:
			statement += (
				' RETURN [ '
				'	input.name_lower, '
				'	input.name '
				' ] '
			)
		if input_group and not inverse and not details:
			statement += (
				' ORDER BY in_group.position, input.name_lower '
			)
		elif not details:
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
				'	<-[:IN_GROUP]-(input: Input) '
				'	-[:AT_LEVEL]->(: ItemLevel {name_lower: toLower($item_level)}), '
				'	(input)-[:OF_TYPE]->(: RecordType {name_lower: toLower($record_type)}) '
			)
		elif item_level:
			statement += (
				' MATCH '
				'	(ig) '
				'	<-[:IN_GROUP]-(: Input) '
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
			' )<-[:IS_IN]-(region: Region '
		)
		if record_data['region']:
			parameters['region'] = record_data['region']
			statement += (
				' {name_lower: toLower($region)} '
			)
		statement += (
			' )<-[:IS_IN]-(farm: Farm '
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
				' )<-[:IS_IN]-(field: Field '
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
				' <-[:IS_IN]-(:FieldBlocks) '
			)
			if record_data['item_level'] == 'block':
				statement += (
					' <-[:IS_IN]-(item :Block '
				)
			else:
				statement += (
					' <-[:IS_IN]-(block: Block '
				)
			if record_data['block_uid']:
				parameters['block_uid'] = record_data['block_uid']
				statement += (
					' {uid: $block_uid} '
				)
			statement += (
				')'
			)
		if any([
			record_data['item_level'] == 'tree',
			record_data['tree_id_list']
		]):
			if record_data['block_uid']:
				parameters['block_uid'] = record_data['block_uid']
				statement += (
					' <-[:IS_IN]-(:BlockTrees)<-[IS_IN]- '
				)
			else:
				statement += (
					' <-[:IS_IN]-(:FieldTrees)<-[:IS_IN]- '
				)
			if record_data['item_level'] == 'tree':
				statement += (
					' (item :Tree) '
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
				' <-[: FROM*]-(item: Sample) '
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
		return statement, parameters

	def generate_id_list(self, record_data):
		statement, parameters = self.build_match_item_statement(record_data)
		if parameters['item_level'] == 'field':
			statement += (
				' OPTIONAL MATCH '
				'	(item)-[: OF_VARIETY | CONTAINS_VARIETY]->(variety:Variety) '
				' WITH '
				'	country, region, farm, item, '
				'	collect(DISTINCT variety.name) as varieties '
			)
		if parameters['item_level'] in ['tree', 'sample']:
			statement += (
				' OPTIONAL MATCH '
				'	(item)-[: FROM | OF_VARIETY *]->(variety: Variety) '
				' OPTIONAL MATCH '
				'	(item)-[: IS_IN | OF_VARIETY *]->(field_variety: Variety) '
			)
			if "block_uid" not in parameters:
				statement += (
					' OPTIONAL MATCH '
					'	(item)-[:IS_IN | FROM*]->(:BlockTrees) '
					'	-[:IS_IN]->(block :Block) '
				)
			if parameters['item_level'] == 'tree':
				statement += (
					' WITH country, region, farm, field, block, item, '
					' collect(DISTINCT coalesce(variety.name, field_variety.name)) as varieties '
				)
			if parameters['item_level'] == 'sample':
				if 'tree_id_list' not in parameters:
					statement += (
						' OPTIONAL MATCH '
						'	(item)-[:FROM*]->(tree: Tree) '
					)
				statement += (
					' OPTIONAL MATCH '
					'	(item)-[:FROM*]->(source_sample: Sample) '
					' WITH DISTINCT '
					' item, '
					' country, region, farm, field, '
					' collect(DISTINCT coalesce(variety.name, field_variety.name)) as varieties, '
					' collect(DISTINCT block.id) as block_ids, '
					' collect(DISTINCT block.name) as blocks, '
					' collect(DISTINCT tree.id) as tree_ids, '
					' collect(DISTINCT tree.custom_id) as tree_custom_ids, '
					' collect(DISTINCT source_sample.id) as source_sample_ids, '
					' collect(DISTINCT source_sample.storage_condition) as source_sample_storage_conditions, '
					# need to ensure these values are consistent in submission. taking first entry anyway
					' collect(source_sample.tissue)[0] as source_sample_tissue, '
					' collect(source_sample.harvest_time)[0] as source_sample_harvest_time '
				)
		statement += (
			' RETURN { '
			'	UID: item.uid, '
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
				'	Field: field.name, '
				'	`Field UID`: field.uid, '
			)
			if parameters['item_level'] == 'block':
				statement += (
					' Block: item.name '
				)
			elif parameters['item_level'] == 'tree':
				statement += (
					' Block: block.name, '
					' `Block ID` : block.id, '
					' `Tree ID`: item.id, '
					' `Tree Custom ID`: item.custom_id '
				)
			elif parameters['item_level'] == 'sample':
				statement += (
					' Block: blocks, '
					' `Block ID` : block_ids, '
					' `Tree ID`: tree_ids, '
					' `Tree Custom ID`: tree_custom_ids, '
					' `Sample Custom ID`: item.custom_id, '
					# first entry will be immediate parent sample value (item), subsequent are in no particular order
					' `Source Sample IDs`: source_sample_ids, '
					' Tissue: coalesce(item.tissue, source_sample_tissue), '
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
