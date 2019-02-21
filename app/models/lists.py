from neo4j_driver import (
	get_driver,
	neo4j_query
)


class SelectionList:
	def __init__(
			self
	):
		pass

	@staticmethod
	def get_partners():
		parameters = {}
		query = (
			' MATCH (partner:Partner) '
			' RETURN [ '
			'	partner.name, '
			'	partner.fullname'
			' ] '
			' ORDER BY partner.name'
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				neo4j_query,
				query,
				parameters
			)
		return [(record[0][0], record[0][1]) for record in result]

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
				neo4j_query,
				query,
				parameters
			)
		return [(record[0][0], record[0][1]) for record in result]

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
				neo4j_query,
				query,
				parameters
			)
		return [(record[0][0], record[0][1]) for record in result]

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
				neo4j_query,
				query,
				parameters)
		return [record[0] for record in result]

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
				neo4j_query,
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
				neo4j_query,
				query,
				parameters)
		return [record[0] for record in result]

	@staticmethod
	def get_feature_groups(item_level, record_type):
		parameters = {
			"item_level": item_level,
			"record_type": record_type
		}
		if item_level and record_type:
			statement = (
				' MATCH '
				'	(feature_group: FeatureGroup) '
				'	<-[:IN_GROUP]-(feature: Feature) '
				'	-[:AT_LEVEL]->(: ItemLevel {name_lower: toLower($item_level)}), '
				'	(feature)-[:OF_TYPE]->(: RecordType {name_lower: toLower($record_type)}) '
			)
		elif item_level:
			statement = (
				' MATCH '
				'	(feature_group: FeatureGroup) '
				'	<-[:IN_GROUP]-(: Feature) '
				'	-[:AT_LEVEL]->(: ItemLevel {name_lower: toLower($item_level)}) '
			)
		elif record_type:
			statement = (
				' MATCH '
				'	(feature_group: FeatureGroup) '
				'	<-[:IN_GROUP]-(: Feature) '
				'	-[:OF_TYPE]->(: RecordType {name_lower: toLower($record_type)}) '
			)
		else:
			statement = (
				' MATCH '
				'	(feature_group: FeatureGroup) '
			)
		statement += (
				' WITH DISTINCT (feature_group) '
				' RETURN [ '
				'	feature_group.name_lower, '
				'	feature_group.name '
				' ] '
				' ORDER BY feature_group.name_lower '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				neo4j_query,
				statement,
				parameters
			)
		return [record[0] for record in result]

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
				neo4j_query,
				statement,
				parameters
			)
		return [record[0] for record in result]


class ItemList:
	def __init__(self):
		pass

	@staticmethod
	def build_match_item_statement(record_data):
		parameters = {
			'record_type': record_data['record_type'],
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
					' <-[:IS_IN]-(:BlockTrees) '
				)
			else:
				statement += (
					' <-[:IS_IN]-(:FieldTrees) '
				)
			if record_data['item_level'] == 'tree':
				statement += (
					' <-[:IS_IN]-(item :Tree) '
				)
				if record_data['tree_id_list']:
					parameters['tree_id_list'] = record_data['tree_id_list']
					statement += (
						' WHERE '
						' item.id in $tree_id_list '
					)
			else:
				statement += (
					' <-[:IS_IN]-(tree:Tree) '
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
		if parameters['item_level'] in ['tree', 'sample']:
			if "block_uid" not in parameters:
				statement += (
					' OPTIONAL MATCH '
					'	(item)-[:IS_IN | FROM*]->(block :Block) '
				)
			if parameters['item_level'] == 'sample':
				if 'tree_id_list' not in parameters:
					statement += (
						' OPTIONAL MATCH '
						'	(item)-[:FROM*]->(tree: Tree) '
					)
				if 'sample_id_list' not in parameters:
					statement += (
						' OPTIONAL MATCH '
						'	(item)-[:FROM*]->(parent_sample: Sample) '
					)
				statement += (
					' WITH DISTINCT '
					' item, '
					' country, region, farm, field, '
					' collect(DISTINCT block.uid) as block_uids, '
					' collect(DISTINCT block.name) as block_names, '
					' collect(DISTINCT tree.uid) as tree_uids, '
					' collect(DISTINCT tree.custom_id) as tree_custom_ids, '
					' collect(DISTINCT tree.variety) as tree_varieties, '
					' collect(DISTINCT parent_sample.uid) as parent_sample_uids, '
					' collect(DISTINCT parent_sample.storage_condition) as parent_sample_storage_conditions, '
					# need to ensure these values are consistent in submission. taking first entry anyway
					' collect(parent_sample.tissue)[0] as parent_sample_tissue, '
					' collect(parent_sample.harvest_condition)[0] as parent_sample_harvest_condition, '
					' collect(parent_sample.harvest_time)[0] as parent_sample_harvest_time '
				)
		statement += (
			' RETURN { '
			'	UID: item.uid, '
			'	Country: country.name, '
			'	Region: region.name, '
			'	Farm: farm.name, '
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
					' `Block UID` : block.uid, '
					' `Tree UID`: item.uid, '
					' `Tree Custom ID`: item.custom_id, '
					' Variety: item.variety '
				)
			elif parameters['item_level'] == 'sample':
				statement += (
					' Block: block_names, '
					' `Block UID` : block_uids, '
					' `Tree UID`: tree_uids, '
					' `Tree Custom ID`: tree_custom_ids, '
					' Variety: tree_varieties, '
					# first entry will be immediate parent sample value (item), subsequent are in no particular order
					' `Parent Sample UID`: item.uid + parent_sample_uids, '
					' `Storage Condition`: item.storage_condition + parent_sample_storage_conditions, '
					' Tissue: coalesce(item.tissue, parent_sample_tissue), '
					' `Harvest Condition`: coalesce(item.harvest_condition, parent_sample_harvest_condition), '
					' `Harvest Time`: apoc.date.format(coalesce(item.harvest_time, parent_sample_harvest_time)) '
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
				neo4j_query,
				statement,
				parameters)
		return [record[0] for record in result]

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
				neo4j_query,
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
				neo4j_query,
				query,
				parameters)
		return [record[0] for record in result]

	@staticmethod
	def get_trees(
			country=None,
			region=None,
			farm=None,
			field_uid=None,
			block_uid=None,
			tree_id_list=None
	):
		parameters = {}
		filters = []
		optional_matches = [(
			' (tree)-[: IN_TREATMENT_CATEGORY]->(tc: TreatmentCategory) '
			' -[: FOR_TREATMENT]->(: FieldItemTreatment)'
			' -[: FOR_TREATMENT]->(treatment: Treatment)'
		)]
		query = 'MATCH (c:Country '
		if country:
			query += '{name_lower: toLower($country)}'
			parameters['country'] = country
		query += ')<-[:IS_IN]-(r:Region '
		if region:
			query += '{name_lower: toLower($region)}'
			parameters['region'] = region
		query += ')<-[:IS_IN]-(f:Farm '
		if farm:
			query += '{name_lower: toLower($farm)}'
			parameters['farm'] = farm
		query += ')<-[:IS_IN]-(field:Field '
		if field_uid:
			query += '{uid: $field_uid}'
			parameters['field_uid'] = field_uid
		query += ')'
		if block_uid:
			query += (
				' MATCH '
				'	(field) '
				'	<-[:IS_IN]-(fb: FieldBlocks) '
				'	<-[:IS_IN]-(block:Block {uid: $block_uid}) '
				'	<-[:IS_IN]-(bt: BlockTrees) '
				'	<-[:IS_IN]-(tree: Tree) '
			)
			parameters['block_uid'] = block_uid
		else:
			query += (
				' MATCH '
				'	(field) '
				'	<-[:IS_IN]-(ft: FieldTrees) '
				'	<-[:IS_IN]-(tree: Tree) '
			)
			optional_matches.append(
				'	(tree)-[:IS_IN]->(bt:BlockTrees) '
				'	-[:IS_IN]->(block:Block) '
			)
		if tree_id_list:
			filters.append(
				' tree.id in $tree_id_list '
			)
			parameters['tree_id_list'] = tree_id_list
		if filters:
			query += (
				' WHERE '
			)
			filter_count = len(filters)
			for f in filters:
				query += f
				filter_count -= 1
				if filter_count != 0:
					query += ' AND '
		if optional_matches:
			query += (
				' OPTIONAL MATCH '
			)
			query += ' OPTIONAL MATCH '.join(optional_matches)
		query += (
			' WITH '
			'	c, r, f, field, block, tree, '
			'	treatment, '
			'	collect (distinct tc.category) as categories '
			' WITH { '
			'	UID: tree.uid, '
			'	`Tree Custom ID`: tree.custom_id, '
			'	`Variety`: tree.variety, '
			'	`Block UID`: block.uid, '
			'	Block: block.name, '
			'	`Field UID` : field.uid, '
			'	Field: field.name, '
			'	Farm: f.name, '
			'	Region: r.name, '
			'	Country: c.name, '
			'	Treatments: collect({ '
			'		name: treatment.name, '
			'		categories: categories '
			'	}) '
			' } as result, field.uid as field_uid, tree.id as tree_id '
			' RETURN result '
			' ORDER BY field_uid, tree_id '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				neo4j_query,
				query,
				parameters)
		return [record[0] for record in result]

	@staticmethod
	def get_samples(
			country=None,
			region=None,
			farm=None,
			field_uid=None,
			block_uid=None,
			tree_id_list=None,
			sample_id_list=None,
			tissue=None,
			harvest_condition=None,
			start_time=None,
			end_time=None
	):
		parameters = {}
		filters = []
		optional_matches = []
		query = 'MATCH (c:Country '
		if country:
			query += '{name_lower: toLower($country)}'
			parameters['country'] = country
		query += ')<-[:IS_IN]-(r:Region '
		if region:
			query += '{name_lower: toLower($region)}'
			parameters['region'] = region
		query += ')<-[:IS_IN]-(f:Farm '
		if farm:
			query += '{name_lower: toLower($farm)}'
			parameters['farm'] = farm
		query += ')<-[:IS_IN]-(field:Field '
		if field_uid:
			query += '{uid: $field_uid}'
			parameters['field_uid'] = field_uid
		query += (
			' )<-[:FROM_FIELD]-(:FieldSamples) '
			' <-[:FROM_FIELD]-(sample:Sample) '
		)
		if block_uid:
			query += (
				'	-[:FROM_TREE]->(: TreeSamples) '
				'	-[:FROM_TREE]->(tree: Tree) '
				'	-[:IS_IN]->(block: Block { '
				'		uid: $block_uid) '
				'	}) '
			)
			parameters['block_uid'] = block_uid
		elif tree_id_list:
			query += (
				'	-[:FROM_TREE]->(: TreeSamples) '
				'	-[:FROM_TREE]->(tree: Tree) '
			)
			optional_matches.append(
				'	(tree)-[:IS_IN]->(:BlockTrees) '
				'	-[:IS_IN]->(block:Block) '
			)
			filters.append(
				' tree.id IN $tree_id_list '
			)
			parameters['tree_id_list'] = tree_id_list
		else:
			optional_matches.append(
				'	(sample) '
				'	-[:FROM_TREE]->(: TreeSamples) '
				'	-[:FROM_TREE]->(tree: Tree) '
			)
			optional_matches.append(
				'	(tree)-[:IS_IN]->(:BlockTrees) '
				'	-[:IS_IN]->(block:Block) '
			)
		if sample_id_list:
			filters.append(
				' sample.id IN $sample_id_list '
			)
			parameters['sample_id_list'] = sample_id_list
		if tissue:
			filters.append(
				' sample.tissue = $tissue '
			)
			parameters['tissue'] = tissue
		if harvest_condition:
			filters.append(
				' sample.harvest_condition = $harvest_condition '
			)
			parameters['harvest_condition'] = harvest_condition
		if start_time:
			filters.append(
				' sample.harvest_time >= $start_time '
			)
			parameters['start_time'] = start_time
		if end_time:
			filters.append(
				' sample.harvest_time <= $end_time '
			)
			parameters['end_time'] = end_time
		if filters:
			query += (
				' WHERE '
			)
			query += ' AND '.join(filters)
		optional_matches.append(
				' (tree)-[: IN_TREATMENT_CATEGORY]->(tc: TreatmentCategory) '
				' -[: FOR_TREATMENT]->(: FieldItemTreatment) '
				' -[: FOR_TREATMENT]->(treatment: Treatment) '
			)
		if optional_matches:
			query += (
				' OPTIONAL MATCH '
			)
			query += ' OPTIONAL MATCH '.join(optional_matches)
		query += (
			' WITH '
			'	sample, '
			'	tree, '
			'	block, '
			'	field, '
			'	f,r,c, '
			'	treatment, '
			'	collect(distinct tc.category) as categories '
			' ORDER BY field.uid, tree.id '
			' WITH { '
			'	UID: sample.uid, '
			'	`Tree UID`: collect(distinct tree.uid), '
			'	Variety: collect(distinct(tree.variety)), '
			'	`Tree Custom ID`: collect(distinct(tree.custom_id)), '
			'	`Block UID`: collect(distinct(block.uid)), '
			'	Block: collect(distinct(block.name)), '
			'	`Field UID` : field.uid, '
			'	Field: field.name, '
			'	Farm: f.name, '
			'	Region: r.name, '
			'	Country: c.name, '
			'	Treatments: collect({ '
			'		name: treatment.name, '
			'		categories: categories '
			'	}), '
			'	`Harvest condition`: sample.harvest_condition, '
			'	`Harvest time`: apoc.date.format(sample.harvest_time), '
			'	Tissue: sample.tissue '
			' } as result, field.uid as field_uid, sample.id as sample_id'
			' RETURN result '
			' ORDER BY field_uid, sample_id '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				neo4j_query,
				query,
				parameters)
		return [record[0] for record in result]


class FeatureList:
	def __init__(self, item_level, record_type):
		self.item_level = item_level
		self.record_type = record_type

	def get_features(
			self,
			feature_group=None,
			features=None
	):
		parameters = {
			"record_type": self.record_type,
			"item_level": self.item_level
		}
		if self.record_type and self.item_level:
			statement = (
				' MATCH '
				'	(: RecordType {name_lower: toLower($record_type)})'
				'	<-[:OF_TYPE]-(feature: Feature) '
				'	-[:AT_LEVEL]->(: ItemLevel {name_lower: toLower($item_level)}) '
			)
		elif self.record_type:
			statement = (
				' MATCH '
				'	(: RecordType {name_lower: toLower($record_type)})'
				'	<-[:OF_TYPE]-(feature: Feature) '
			)
		elif self.item_level:
			statement = (
				' MATCH '
				'	(feature:Feature) '
				'	-[:AT_LEVEL]->(: ItemLevel {name_lower: toLower($item_level)}) '
			)
		else:
			statement = (
				' MATCH (feature: Feature) '
			)
		if feature_group:
			parameters['feature_group'] = feature_group
			statement += (
				' , '
				'	(feature)'
				'	-[:IN_GROUP]->(: FeatureGroup { '
				'		name_lower: toLower($feature_group) '
				'	}) '
			)
		if features:
			parameters['features'] = features
			statement += (
				' WHERE feature.name_lower IN extract(item IN $features | toLower(trim(item))) '
			)
		statement += (
			' RETURN properties(feature) '
			' ORDER BY feature.name_lower '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				neo4j_query,
				statement,
				parameters
			)
		return [record[0] for record in result]
