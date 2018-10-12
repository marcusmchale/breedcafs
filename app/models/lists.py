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
	def get_tissues():
		parameters = {}
		query = (
			' MATCH (tissue:Tissue) '
			' RETURN [ '
			'	tissue.name_lower, '
			'	tissue.name'
			' ] '
			' ORDER BY tissue.name'
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				neo4j_query,
				query,
				parameters
			)
		return [(record[0][0], record[0][1]) for record in result]

	@staticmethod
	def get_storage_types():
		parameters = {}
		query = (
			' MATCH (storage:Storage) '
			' RETURN [ '
			'	storage.name_lower, '
			'	storage.name'
			' ] '
			' ORDER BY storage.name'
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				neo4j_query,
				query,
				parameters
			)
		return [(record[0][0], record[0][1]) for record in result]


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
			' RETURN { '
			'	Country : country.name, '
			'	Region : region.name, '
			'	Farm : farm.name, '
			'	Field : field.name, '
			'	UID : field.uid '
			' } '
			' ORDER BY field.uid '
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
			query += '{uid: toLower($field_uid)}'
			parameters['field_uid'] = field_uid
		query += (
			' )<-[:IS_IN]-(:FieldBlocks) '
			' <-[:IS_IN]-(block:Block) '
			' RETURN {'
			' UID: block.uid, '
			' Block: block.name, '
			' `Field UID` : field.uid, '
			' Field: field.name, '
			' Farm: f.name, '
			' Region: r.name, '
			' Country: c.name }'
			' ORDER BY block.id'
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
			trees_start=None,
			trees_end=None
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
			query += '{uid: $field_uid})'
			parameters['field_uid'] = field_uid
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
		if trees_start:
			filters.append(
				' tree.id >= $trees_start '
			)
			parameters['trees_start'] = trees_start
		if trees_end:
			filters.append(
				' tree.id <= $trees_end '
			)
			parameters['trees_end'] = trees_end
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
			count = len(optional_matches)
			for optional_match in optional_matches:
				query += optional_match
				count -= 1
				if count != 0:
					query += ' , '
		query += (
			' RETURN {'
			' UID: tree.uid, '
			' `Block UID`: block.uid, '
			' Block: block.name, '
			' `Field UID` : field.uid, '
			' Field: field.name, '
			' Farm: f.name, '
			' Region: r.name, '
			' Country: c.name } '
			' ORDER BY tree.id '
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
			trees_start=None,
			trees_end=None,
			branches_start=None,
			branches_end=None
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
			query += '{uid: $field_uid})'
			parameters['field_uid'] = field_uid
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
		if trees_start:
			filters.append(
				' tree.id >= $trees_start '
			)
			parameters['trees_start'] = trees_start
		if trees_end:
			filters.append(
				' tree.id <= $trees_end '
			)
			parameters['trees_end'] = trees_end
		query += (
			' MATCH (tree) '
			'	<-[:FROM_TREE]-(tb:TreeBranches) '
			'	<-[:FROM_TREE]-(branch:Branch) '
		)
		if branches_start:
			filters.append(
				' branch.id >= $branches_start '
			)
			parameters['branches_start'] = branches_start
		if branches_end:
			filters.append(
				' branch.id <= $branches_end '
			)
			parameters['branches_end'] = branches_end
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
			count = len(optional_matches)
			for optional_match in optional_matches:
				query += optional_match
				count -= 1
				if count != 0:
					query += ' , '
		query += (
			' RETURN {'
			' UID: branch.uid, '
			' `Tree UID`: tree.uid, '
			' `Block UID`: block.uid, '
			' Block: block.name, '
			' `Field UID` : field.uid, '
			' Field: field.name, '
			' Farm: f.name, '
			' Region: r.name, '
			' Country: c.name } '
			' ORDER BY branch.id '
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
			trees_start=None,
			trees_end=None,
			branches_start=None,
			branches_end=None,
			leaves_start=None,
			leaves_end=None
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
			query += '{uid: $field_uid})'
			parameters['field_uid'] = field_uid
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
		if trees_start:
			filters.append(
				' tree.id >= $trees_start '
			)
			parameters['trees_start'] = trees_start
		if trees_end:
			filters.append(
				' tree.id <= $trees_end '
			)
			parameters['trees_end'] = trees_end
		query += (
			' MATCH (tree) '
			'	<-[:FROM_TREE]-(tl:TreeLeaves) '
			'	<-[:FROM_TREE]-(leaf:Leaf) '
		)
		if leaves_start:
			filters.append(
				' leaf.id >= $leaves_start '
			)
			parameters['leaves_start'] = leaves_start
		if leaves_end:
			filters.append(
				' leaf.id <= $leaves_end '
			)
			parameters['leaves_end'] = leaves_end
		if branches_start or branches_end:
			query += (
				' MATCH '
				' (leaf)-[:FROM_BRANCH]->(branch:Branch) '
			)
			if branches_start:
				filters.append(
					' branch.id >= $branches_start '
				)
				parameters['branches_start'] = branches_start
			if branches_end:
				filters.append(
					' branch.id <= $branches_end '
				)
				parameters['branches_end'] = branches_end
		else:
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
			count = len(optional_matches)
			for optional_match in optional_matches:
				query += optional_match
				count -= 1
				if count != 0:
					query += ' , '
		query += (
			' RETURN {'
			' UID: leaf.uid, '
			' `Branch UID`: branch.uid, '
			' `Tree UID`: tree.uid, '
			' `Block UID": block.uid, '
			' Block: block.name, '
			' `Field UID` : field.uid, '
			' Field: field.name, '
			' Farm: f.name, '
			' Region: r.name, '
			' Country: c.name } '
			' ORDER BY leaf.id '
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
			trees_start=None,
			trees_end=None,
			samples_start=None,
			samples_end=None,
			tissue=None,
			storage=None,
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
		elif any([trees_start, trees_end]):
			query += (
				'	-[:FROM_TREE]->(: TreeSamples) '
				'	-[:FROM_TREE]->(tree: Tree) '
			)
			optional_matches.append(
				'	(tree)-[:IS_IN]->(:BlockTrees) '
				'	-[:IS_IN]->(block:Block) '
			)
			if trees_start:
				filters.append(
					' tree.id >= $trees_start '
				)
				parameters['trees_start'] = trees_start
			if trees_end:
				filters.append(
					' tree.id <= $trees_end '
				)
				parameters['trees_end'] = trees_end
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
		if samples_start:
			filters.append(
				' sample.id >= $samples_start '
			)
			parameters['samples_start'] = samples_start
		if samples_end:
			filters.append(
				' sample.id <= $samples_end '
			)
			parameters['samples_end'] = samples_end
		if tissue:
			filters.append(
				' tissue = $tissue '
			)
			parameters['tissue'] = tissue
		if storage:
			filters.append(
				' storage = $storage '
			)
			parameters['storage'] = storage
		if start_time:
			filters.append(
				' sample.time >= $start_time '
			)
			parameters['start_time'] = start_time
		if end_time:
			filters.append(
				' sample.time <= $end_time '
			)
			parameters['end_time'] = end_time
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
			count = len(optional_matches)
			for optional_match in optional_matches:
				query += optional_match
				count -= 1
				if count != 0:
					query += ' , '
		query += (
			' WITH '
			'	sample, '
			'	tree, '
			'	block, '
			'	field, '
			'	f,r,c '
			' ORDER BY field, tree, sample '
			' WITH '
			'	sample.uid as UID, '
			'	collect(tree.uid) as `Tree UID`, '
			'	collect(distinct(tree.variety)) as Variety, '
			'	collect(distinct(tree.custom_id)) as `Tree Custom ID`, '
			'	collect(distinct(block.uid)) as `Block UID`, '
			'	collect(distinct(block.name)) as Block, '
			'	field.uid as `Field UID`, '
			'	field.name as Field, '
			'	f.name as Farm, '
			'	r.name as Region, '
			'	c.name as Country, '
			'	[field.uid, sample.id] as id '
			' RETURN { '
			'	UID: UID, '
			'	`Tree UID`: `Tree UID`, '
			'	Variety: Variety, '
			'	`Tree Custom ID`: `Tree Custom ID`, '
			'	`Block UID`: `Block UID`, '
			'	Block: Block, '
			'	`Field UID` : `Field UID`, '
			'	Field: Field, '
			'	Farm: Farm, '
			'	Region: Region, '
			'	Country: Country '
			' } '
			' ORDER BY id '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				neo4j_query,
				query,
				parameters)
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