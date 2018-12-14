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
	def get_feature_groups(data_type, level):
		parameters = {
			"level": level
		}
		query = (
			' MATCH '
			'	(feature_group: '
		)
		if data_type == 'trait':
			query += (
				' TraitGroup '
				' )<-[:IN_GROUP]-(: Trait) '
			)
		elif data_type == 'condition':
			query += (
				' ConditionGroup '
				' )<-[:IN_GROUP]-(: Condition) '
			)
		query += (
				'		-[:AT_LEVEL]->(:Level {name_lower: $level}) '
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
				query,
				parameters
			)
		return [record[0] for record in result]

	@staticmethod
	def get_tissues():
		parameters = {}
		query = (
			' MATCH (trait:Trait {name_lower: "tissue type"}) '
			' RETURN trait.category_list '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				neo4j_query,
				query,
				parameters
			)
		tissue_list = [record[0] for record in result][0]
		return [(tissue.lower(), tissue) for tissue in tissue_list]

	@staticmethod
	def get_harvest_conditions():
		parameters = {}
		query = (
			' MATCH (trait:Trait {name_lower: "harvest condition"}) '
			' RETURN trait.category_list '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				neo4j_query,
				query,
				parameters
			)
		condition_list = [record[0] for record in result][0]
		return [(condition.lower(), condition) for condition in condition_list]


class ItemList:
	def __init__(self):
		pass

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
	def get_branches(
			country=None,
			region=None,
			farm=None,
			field_uid=None,
			block_uid=None,
			tree_id_list=None,
			branch_id_list=None
	):
		parameters = {}
		filters = []
		optional_matches = [(
			' (tree)-[: IN_TREATMENT_CATEGORY]->(tc: TreatmentCategory) '
			' -[: FOR_TREATMENT]->(: FieldItemTreatment) '
			' -[: FOR_TREATMENT]->(treatment: Treatment) '
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
				' tree.id IN $tree_id_list '
			)
			parameters['trees_start'] = tree_id_list
		query += (
			' MATCH (tree) '
			'	<-[:FROM_TREE]-(tb:TreeBranches) '
			'	<-[:FROM_TREE]-(branch:Branch) '
		)
		if branch_id_list:
			filters.append(
				' branch.id IN $branch_id_list '
			)
			parameters['branch_id_list'] = branch_id_list
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
			'	c, r, f, field, block, tree, branch, '
			'	treatment, '
			'	collect (distinct tc.category) as categories '
			' WITH { '
			' 	UID: branch.uid, '
			'	`Tree UID`: tree.uid, '
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
			'} as result, field.uid as field_uid, branch.id as branch_id '
			' RETURN result '
			' ORDER BY field_uid, branch_id '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				neo4j_query,
				query,
				parameters)
		return [record[0] for record in result]

	@staticmethod
	def get_leaves(
			country=None,
			region=None,
			farm=None,
			field_uid=None,
			block_uid=None,
			tree_id_list=None,
			leaf_id_list=None
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
				' tree.id IN $tree_id_list '
			)
			parameters['tree_id_list'] = tree_id_list
		query += (
			' MATCH (tree) '
			'	<-[:FROM_TREE]-(tl:TreeLeaves) '
			'	<-[:FROM_TREE]-(leaf:Leaf) '
		)
		if leaf_id_list:
			filters.append(
				' leaf.id IN $leaf_id_list '
			)
			parameters['leaf_id_list'] = leaf_id_list
		optional_matches.append(
			' (leaf)-[:FROM_BRANCH]->(branch:Branch) '
		)
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
			'	c, r, f, field, block, tree, branch, leaf, '
			'	treatment, '
			'	collect (distinct tc.category) as categories '
			' WITH { '
			'	UID: leaf.uid, '
			'	`Branch UID`: branch.uid, '
			'	`Tree UID`: tree.uid, '
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
			'} as result, field.uid as field_uid, leaf.id as leaf_id '
			' RETURN result '
			' ORDER BY field_uid, leaf_id '
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


class FeaturesList:
	def __init__(self):
		pass

	@staticmethod
	def get_features_details(data_type, level, group):
		parameters = {
			"data_type": data_type,
			"level": level,
			"group": group
		}
		query = (
			' MATCH '
			'	(feature_group: '
		)
		if data_type == 'trait':
			query += (
				'TraitGroup'
			)
		elif data_type == 'condition':
			query += (
				'ConditionGroup'
			)
		query += (
			'	{ '
			'		name_lower: toLower($group) '
			'	}) '
			'	<-[:IN_GROUP]-(feature: '
		)
		if data_type == 'trait':
			query += (
				'Trait'
			)
		if data_type == 'condition':
			query += (
				'Condition'
			)
		query += (
			'	)-[:AT_LEVEL]-(:Level { '
			'		name_lower: toLower($level)'
			'	}) '
			' RETURN properties(feature) '
			' ORDER BY feature.name_lower '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				neo4j_query,
				query,
				parameters
			)
		return [record[0] for record in result]


class TraitList:
	def __init__(self):
		pass

	@staticmethod
	# also used to confirm list of traits from web form selections are real/found at the selected level
	def get_traits(
			level,
			traits=None
	):
		parameters = {
			'level': level,
			'traits': traits
		}
		query = (
			' MATCH (trait:Trait {level:$level}) '
		)
		if traits:
			query += (
				' WHERE trait.name_lower in $traits '
			)
		query += (
			' RETURN properties(trait) '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				neo4j_query,
				query,
				parameters)
		return [record[0] for record in result]