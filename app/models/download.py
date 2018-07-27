from app import app, os
import grp
from app.cypher import Cypher
from lists import Lists
from samples import Samples
from neo4j_driver import (
	get_driver,
	neo4j_query
)
from flask import (
	session,
	url_for
)
import unicodecsv as csv
from datetime import datetime
from app.models import (
	Fields)


# User class related (all uploads are tied to a user) yet more specifically regarding uploads
class Download:
	def __init__(self, username):
		self.username = username

	def make_csv_file(self, fieldnames, id_list, filename, with_time = True):
		# create user download path if not found
		download_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], self.username)
		if not os.path.isdir(download_path):
			os.mkdir(download_path)
			gid = grp.getgrnam(app.config['CELERYGRPNAME']).gr_gid
			os.chown(download_path, -1, gid)
			os.chmod(download_path, 0775)
		# prepare variables to write the file
		time = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
		if with_time:
			filename = time + '_' + filename
		file_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username'], filename)
		# handle case where user is making more than one file a minute (per second filenames)
		if os.path.isfile(file_path):
			time = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
			filename = time + '_' + filename
			file_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username'], filename)
		# make the file
		with open(file_path, 'w') as csv_file:
			writer = csv.DictWriter(
				csv_file,
				fieldnames = fieldnames,
				quoting = csv.QUOTE_ALL,
				extrasaction = 'ignore'
			)
			writer.writeheader()
			for row in id_list:
				for item in row:
					if isinstance(row[item], list):
						row[item] = [str(i).encode() for i in row[item]]
				# for key, value in row:
				writer.writerow(row)
			file_size = csv_file.tell()
		# return file details
		return {
			"filename": filename,
			"file_path": file_path,
			"file_size": file_size,
			"url": url_for(
				'download_file',
				username = self.username,
				filename = filename,
				_external = True
			)
		}

	def get_csv(
			self,
			country, 
			region, 
			farm, 
			field_uid,
			block_uid,
			level, 
			traits, 
			data_format, 
			start_time, 
			end_time):
		# First limit the get the confirmed affiliated institutes and all data nodes submitted by these
		# TODO put this filter at the end of the query, might be less db-hits
		user_data_query = (
			' MATCH (:User {username_lower: toLower($username)}) '
			' -[:AFFILIATED {confirmed :true}] '
			' ->(partner:Partner) '
			' WITH partner '
			' MATCH (partner)<-[:AFFILIATED {data_shared :true}] '
			' -(user:User) '
			' -[:SUBMITTED*5]->(data:Data) '
			' WITH user.name as user_name, partner.name as partner_name, data '
		)
		# this section first as not dependent on level but part of all queries
		if farm != "":
			frc = (
				' -[:IS_IN]->(farm:Farm {name_lower: toLower($farm)}) '
				' -[:IS_IN]->(region:Region {name_lower: toLower($region)}) '
				' -[:IS_IN]->(country:Country {name_lower: toLower($country)}) '
			)
		elif region != "":
			frc = (
				' <-[:IS_IN]->(farm:Farm) '
				' -[:IS_IN]->(region:Region {name_lower: toLower($region)}) '
				' -[:IS_IN]->(country:Country {name_lower: toLower($country)}) '
			)
		elif country != "":
			frc = (
				' -[:IS_IN]->(farm: Farm) '
				' -[:IS_IN]->(region: Region) '
				' -[:IS_IN]->(country: Country {name_lower: toLower($country)}) '
			)
		else:
			frc = (
				' -[:IS_IN]->(farm: Farm) '
				' -[:IS_IN]->(region: Region) '
				' -[:IS_IN]->(country: Country) '
			)
		# then build the level dependent query string
		if level == 'sample':
			index_fieldnames = [
				'Country',
				'Region',
				'Farm',
				'Field',
				'Field UID',
				'Block',
				'Block UID',
				'Tree UID',
				'Tree Custom ID',
				'Variety',
				'Branch UID',
				'Leaf UID',
				'UID',
				'Tissue',
				'Storage',
				'Date Sampled'
			]
			td = (
				' MATCH '
				'	(trait:SampleTrait) '
				'	<-[FOR_TRAIT*2]-(sample_trait) '
				'	<-[DATA_FOR]-(data)'
				' WHERE (trait.name) IN $traits' 
				' WITH user_name, partner_name, data, trait, sample_trait '
				' MATCH '
				'	(sample_trait) '
				'	-[:FOR_ITEM]->(sample:Sample) '
				'	-[:FROM_TREE*2]->(tree:Tree), '
				'	(sample)'
				'	-[:COLLECTED_AS*2]->(tist) '
				'	-[:OF_TISSUE]->(tissue), '
				'	(tist)-[:STORED_IN]->(storage), '
				'	(tree) '
			)
			# if block_uid is defined
			if block_uid != "":
				tdp = td + (
					'-[:IS_IN]->(: BlockTrees) '
					' -[:IS_IN]->(block: Block {uid: $block_uid}) '
					' -[:IS_IN]->(: FieldBlocks) '
					' -[:IS_IN]->(field: Field) '
				)
				optional_block = ''
			# if field_uid is defined
			elif field_uid != "":
				tdp = td + (
					' -[:IS_IN]->(: FieldTrees) '
					' -[:IS_IN]->(field: Field {uid: $field_uid}) '
				)
				optional_block = (
					' OPTIONAL MATCH (tree) '
					' -[:IS_IN]->(: BlockTrees) '
					' -[:IS_IN]->(block: Block) '
				)
			# else no field selected
			else:
				tdp = td + (
					' -[:IS_IN]->(: FieldTrees) '
					' -[:IS_IN]->(field:Field) '
				)
				optional_block = (
					' OPTIONAL MATCH (tree) '
					' -[:IS_IN]->(: BlockTrees) '
					' -[:IS_IN]->(block: Block) '
				)
			optional_block += (
				' OPTIONAL MATCH '
				'	(sample)-[:FROM_BRANCH {current: True}]->(branch:Branch) '
				' OPTIONAL MATCH '
				'	(sample)-[:FROM_LEAF {current:True}]->(leaf:Leaf) '
			)
			# generate the return statement
			if data_format == 'table':
				response = (
					# need a with statement to allow order by with COLLECT
					' WITH '
					' 	country.name as Country, '
					' 	region.name as Region, '
					' 	farm.name as Farm, '
					' 	field.name as Field, '
					'	field.uid as `Field UID`, '
					' 	block.name as Block, '
					' 	block.uid as `Block UID`, '
					' 	tree.uid as `Tree UID`, '
					' 	tree.custom_id as `Tree Custom ID`, '
					' 	tree.variety as Variety, '
					' 	branch.uid as `Branch UID`, '
					' 	leaf.uid as `Leaf UID`, '
					' 	sample.uid as `Sample UID`, '
					'	tissue.name as Tissue, '
					'	storage.name as Storage, '
					'	sample.date as `Date Sampled`, '
					' 	sample.id as `Sample ID`, '
					'	COLLECT([trait.name, data.value]) as Traits '
					' RETURN '
					'	{ ' 
					'		Country: Country, '
					'		Region: Region, '
					'		Farm: Farm, '
					'		Field: Field, '
					'		`Field UID` : `Field UID`, '
					'		Block : Block, '
					'		`Block UID`: `Block UID`, '
					'		`Tree UID` : `Tree UID`, '
					'		`Tree Custom ID`: `Tree Custom ID`, '
					'		Variety: Variety, '
					'		`Branch UID`: `Branch UID`, '
					'		`Leaf UID`: `Leaf UID`, '
					'		UID : `Sample UID` ,'
					'		Tissue: Tissue, '
					'		Storage: Storage, '
					'		`Date Sampled`: `Date Sampled`, '
					'		`Sample ID` : `Sample ID`, '
					'		Traits : Traits } '
					' ORDER BY '
					'	`Field UID`, `Sample ID` '
				)
			else:  # if data_format == 'db':
				response = (
					' RETURN {'
					'	User: user_name, '
					'	Partner: partner_name,'
					'	Country: country.name, ' 
					'	Region: region.name, '
					'	Farm: farm.name, '
					'	Field: field.name, '
					'	`Field UID`: field.uid, '
					'	Block: block.name, '
					'	`Block UID`: block.uid, '
					'	`Tree UID`: tree.uid, '
					'	`Tree Custom ID`: tree.custom_id,	'
					'	Variety: tree.variety, '
					'	`Sample UID`: sample.uid, '
					'	`Branch UID`: branch.uid, '
					'	`Leaf UID`: leaf.uid, '
					'	UID : sample.uid, '
					'	Tissue: tissue.name, '
					'	Storage: storage.name, '
					'	`Date Sampled`: sample.date, '
					'	`Sample ID`: sample.id, '
					'	Trait: trait.name, '
					'	Value: data.value, '
					'	Location: data.location, '
					'	`Recorded at`: apoc.date.format(data.time), '
					'	`Recorded by`: data.person '
					' } '
					' ORDER BY '
					'	field.uid, '
					'	sample.id, '
					'	trait.name, '
					'	apoc.date.format(data.time) '
					)
		elif level == 'leaf':
			index_fieldnames = [
				'Country',
				'Region',
				'Farm',
				'Field',
				'Field UID',
				'Block',
				'Block UID',
				'Tree UID',
				'Tree Custom ID',
				'Variety',
				'Branch UID',
				'UID'
			]
			td = (
				' MATCH '
				'	(trait:LeafTrait) '
				'	<-[:FOR_TRAIT*2]-(leaf_trait) '
				'	<-[:DATA_FOR]-(data) '
				' WHERE (trait.name) IN $traits '
				' WITH user_name, partner_name, data, trait, leaf_trait '
				' MATCH '
				'	(leaf_trait) '
				'	-[:FOR_ITEM]->(leaf:Leaf) '
				'	-[:FROM_TREE*2]->(tree:Tree) '
			)

			# if block_uid is defined
			if block_uid != "":
				tdp = td + (
					'-[:IS_IN {current: True}]->(: BlockTrees) '
					' -[:IS_IN]->(block: Block {uid: $block_uid}) '
					' -[:IS_IN]->(: FieldBlocks) '
					' -[:IS_IN]->(field: Field) '
				)
				optional_block = ''
			# if field_uid is defined
			elif field_uid != "":
				tdp = td + (
					' -[:IS_IN]->(: FieldTrees) '
					' -[:IS_IN]->(field: Field {uid: $field_uid}) '
				)
				optional_block = (
					' OPTIONAL MATCH (tree) '
					' -[:IS_IN {current: True}]->(: BlockTrees) '
					' -[:IS_IN]->(block: Block) '
				)
			# no field selected
			else:
				tdp = td + (
					' -[:IS_IN]->(: FieldTrees) '
					' -[:IS_IN]->(field:Field) '
				)
				optional_block = (
					' OPTIONAL MATCH (tree) '
					' -[:IS_IN {current:True}]->(: BlockTrees) '
					' -[:IS_IN]->(block:Block) '
				)
			optional_block += (
				' OPTIONAL MATCH '
				'	(leaf)-[:FROM_BRANCH {current: True}]->(branch:Branch) '
			)
			# and generate the return statement
			if data_format == 'table':
				response = (
					# need a with statement to allow order by with COLLECT
					' WITH '
					'	country.name as Country, '
					'	region.name as Region, '
					'	farm.name as Farm, '
					'	field.name as Field, '
					'	field.uid as `Field UID`, '
					'	block.name as Block, '
					'	block.uid as `Block UID`, '
					'	tree.uid as `Tree UID`, '
					'	tree.custom_id as `Tree Custom ID`, '
					'	tree.variety as Variety, '
					'	branch.uid as `Branch UID`, '
					'	leaf.uid as `Leaf UID`, '
					'	leaf.id as `Leaf ID`, '
					'	COLLECT([trait.name, data.value]) as Traits '
					' RETURN '
					'	{ ' 
					'		Country: Country, '
					'		Region: Region, '
					'		Farm: Farm, '
					'		Field: Field, '
					'		`Field UID`: `Field UID`, '
					'		Block : Block, '
					'		`Block UID`: `Block UID`, '
					'		`Tree UID`: `Tree UID`, '
					'		`Tree Custom ID`: `Tree Custom ID`, '
					'		Variety: Variety, '
					'		`Branch UID`: `Branch UID`, '
					'		UID: `Leaf UID`, '
					'		`Leaf ID`: `Leaf ID`, '
					'		Traits: Traits } '
					' ORDER BY '
					'	`Field UID`, `Leaf ID` '
				)
			else:  # if data_format == 'db':
				response = (
					' RETURN {'
					'	User : user_name, '
					'	Partner : partner_name,'
					'	Country : country.name, ' 
					'	Region : region.name, '
					'	Farm : farm.name, '
					'	Field : field.name, '
					'	`Field UID`: field.uid, '
					'	Block : block.name, '
					'	`Block UID` : block.uid, '
					'	`Tree UID` : tree.uid, '
					'	`Tree Custom ID`: tree.custom_id,	'
					'	Variety: tree.variety, '
					'	`Branch UID`: branch.uid, '
					'	UID: leaf.uid, '
					'	`Leaf ID`: leaf.id, '
					'	Trait : trait.name, '
					'	Value : data.value, '
					'	Location : data.location, '
					'	`Recorded at` : apoc.date.format(data.time), '
					'	`Recorded by`: data.person '
					' } '
					' ORDER BY '
					'	field.uid, '
					'	leaf.id, '
					'	trait.name, '
					'	apoc.date.format(data.time) '
					)
		elif level == 'branch':
			index_fieldnames = [
				'Country',
				'Region',
				'Farm',
				'Field',
				'Field UID',
				'Block',
				'Block UID',
				'Tree UID',
				'Tree Custom ID',
				'Variety',
				'UID',
			]
			td = (
				' MATCH '
				'	(trait: BranchTrait) '
				'	<-[:FOR_TRAIT*2]-(branch_trait) '
				'	<-[:DATA_FOR]-(data) '
				' WHERE (trait.name) IN $traits'
				' WITH user_name, partner_name, data, trait, branch_trait '
				' MATCH '
				'	(branch_trait) '
				'	-[:FOR_ITEM]->(branch:Branch) '
				'	-[:FROM_TREE*2]->(tree:Tree) '
			)
			# if block_uid is defined
			if block_uid != "":
				tdp = td + (
					'-[:IS_IN {current:True}]->(:BlockTrees) '
					' -[:IS_IN]->(block:Block {uid:$blockUID}) '
					' -[:IS_IN]->(:FieldBlocks) '
					' -[:IS_IN]->(field:Field) '
				)
				optional_block = ''
			# if field_uid is defined (but no block_uid)
			elif field_uid != "":
				tdp = td + (
					' -[:IS_IN]->(:FieldTrees) '
					' -[:IS_IN]->(field:Field {uid: $field_uid}) '
				)
				optional_block = (
					' OPTIONAL MATCH (tree) '
					' -[:IS_IN {current:True}]->(:BlockTrees) '
					' -[:IS_IN]->(block:Block) '
				)
			# if no field selected
			else:
				tdp = td + (
					' -[:IS_IN]->(:FieldTrees) '
					' -[:IS_IN]->(field:Field) '
				)
				optional_block = (
					' OPTIONAL MATCH (tree) '
					' -[:IS_IN {current:True}]->(:BlockTrees) '
					' -[:IS_IN]->(block:Block) '
				)
			# generate the return statement
			if data_format == 'table':
				response = (
					# need a with statement to allow order by with COLLECT
					' WITH '
					' 	country.name as Country, '
					' 	region.name as Region, '
					' 	farm.name as Farm, '
					' 	field.name as Field, '
					' 	field.uid as `Field UID`, '
					' 	block.name as Block, '
					' 	block.uid as `Block UID`, '
					' 	tree.uid as `Tree UID`, '
					' 	tree.custom_id as `Tree Custom ID`, '
					' 	tree.variety as Variety, '
					' 	branch.uid as `Branch UID`, '
					'	branch.id as `Branch ID`,'
					'	COLLECT([trait.name, data.value]) as Traits '
					' RETURN '
					'	{ ' 
					'		Country: Country, '
					'		Region: Region, '
					'		Farm: Farm, '
					'		Field: Field, '
					'		`Field UID`: `Field UID`, '
					'		Block: Block, '
					'		`Block UID`: `Block UID`, '
					'		`Tree UID`: `Tree UID`, '
					'		`Tree Custom ID`: `Tree Custom ID`, '
					'		Variety: Variety, '
					'		UID: `Branch UID`, '
					'		`Branch ID`: `Branch ID`,'
					'		Traits: Traits } '
					' ORDER BY '
					'	`Field UID`, `Branch ID` '
				)
			else:  # if data_format == 'db':
				response = (
					' RETURN {'
					'	User: user_name, '
					'	Partner: partner_name,'
					'	Country: country.name, ' 
					'	Region: region.name, '
					'	Farm: farm.name, '
					'	Field: field.name, '
					'	`Field UID`: field.uid, '
					'	Block: block.name, '
					'	`Block UID`: block.uid, '
					'	`Tree UID`: tree.uid, '
					'	`Tree Custom ID`: tree.custom_id,	'
					'	Variety: tree.variety, '
					'	UID: branch.uid, '
					'	`Branch ID`: branch.id, '
					'	Trait: trait.name, '
					'	Value: data.value, '
					'	Location: data.location, '
					'	`Recorded at`: apoc.date.format(data.time), '
					'	`Recorded_by`: data.person '
					' } '
					' ORDER BY '
					'	field.uid, '
					'	branch.id, '
					'	trait.name, '
					'	data.time '
					)
		elif level == 'tree':
			index_fieldnames = [
				'Country',
				'Region',
				'Farm',
				'Field',
				'Field UID',
				'Block',
				'Block UID',
				'UID',
				'Tree Custom ID',
				'Variety'
			]
			td = (
				' MATCH '
				'	(trait:TreeTrait) '
				'	<-[:FOR_TRAIT*2]-(tree_trait) '
				'	<-[:DATA_FOR]-(data) '
				' WHERE (trait.name) IN $traits '
				' WITH '
				'	user_name, '
				'	partner_name, '
				'	data, '
				'	trait, '
				'	tree_trait '
				' MATCH '
				'	(tree_trait) '
				'	-[:FOR_ITEM]->(tree:Tree) '
			)
			# if block_uid is defined
			if block_uid != "":
				tdp = td + (
					' -[:IS_IN {current: True}]->(: BlockTrees) '
					' -[:IS_IN]->(block: Block {uid: $block_uid}) '
					' -[:IS_IN]->(: FieldBlocks) '
					' -[:IS_IN]->(field: Field) '
				)
				optional_block = ''
			# if field_uid is defined
			elif field_uid != "":
				tdp = td + (
					' -[:IS_IN]->(FieldTrees) '
					' -[:IS_IN]->(field: Field {uid:$field_uid}) '
				)
				optional_block = (
					' OPTIONAL MATCH (tree) '
					' -[:IS_IN {current: True}]->(: BlockTrees) '
					' -[:IS_IN]->(block: Block) '
				)
			# if no field selected
			else:
				tdp = td + (
					' -[:IS_IN]->(:FieldTrees) '
					' -[:IS_IN]->(field:Field) '
				)
				optional_block = (
					' OPTIONAL MATCH (tree) '
					' -[:IS_IN {current:True}]->(:BlockTrees) '
					' -[:IS_IN]->(block:Block) '
				)
			# and generate the return statement
			if data_format == 'table':
				response = (
					# need WITH statement to allow order by with COLLECT
					' WITH '
					' 	country.name as Country, '
					' 	region.name as Region, '
					' 	farm.name as Farm, '
					' 	field.name as Field, '
					' 	field.uid as `Field UID`, '
					' 	block.name as Block, '
					' 	block.uid as `Block UID`, '
					' 	tree.uid as `Tree UID`, '
					' 	tree.custom_id as `Tree Custom ID`, '
					' 	tree.variety as Variety, '
					'	tree.id as `Tree ID`,'
					'	COLLECT([trait.name, data.value]) as Traits '
					' RETURN '
					'	{ ' 
					'		Country: Country, '
					'		Region: Region, '
					'		Farm: Farm, '
					'		Field: Field, '
					'		`Field UID`: `Field UID`, '
					'		Block: Block, '
					'		`Block UID`: `Block UID`, '
					'		UID: `Tree UID`, '
					'		`Tree Custom ID`: `Tree Custom ID`, '
					'		Variety: Variety, '
					'		`Tree ID`: `Tree ID`,'
					'		Traits: Traits '
					'	} '
					' ORDER BY '
					'	`Field UID`, `Tree ID` '
				)
			else:  # if data_format == 'db':
				response = (
					' RETURN {'
					'	User : user_name, '
					'	Partner : partner_name,'
					'	Country : country.name, ' 
					'	Region : region.name, '
					'	Farm : farm.name, '
					'	Field : field.name, '
					'	`Field UID`: field.uid, '
					'	Block : block.name, '
					'	`Block UID` : block.uid, '
					'	UID : tree.uid, '
					'	`Tree Custom ID`: tree.custom_id,	'
					'	Variety: tree.variety, '
					'	`Tree ID`: tree.id, '
					'	Trait : trait.name, '
					'	Value : data.value, '
					'	Location : data.location, '
					'	`Recorded at` : apoc.date.format(data.time), '
					'	`Recorded by`: data.person '
					' } '
					' ORDER BY field.uid, tree.id, trait.name, data.time '
					)
		elif level == 'block':
			index_fieldnames = [
				'Country',
				'Region',
				'Farm',
				'Field',
				'Field UID',
				'Block',
				'UID'
			]
			td = (
				' MATCH '
				'	(trait: BlockTrait) '
				'	<-[:FOR_TRAIT*2]-(block_trait) '
				'	<-[:DATA_FOR]-(data) '
				' WHERE (trait.name) IN $traits '
				' WITH user_name, partner_name, data, trait, block_trait '
				' MATCH '
				'	(block_trait) '
				'	-[:FOR_ITEM]->(block:Block '
			)
			if block_uid != "":
				tdp = td + (
					' {uid:$blockUID}) '
					' -[:IS_IN]->(:FieldBlocks) '
					' -[:IS_IN]->(field:Field) '
				)
			elif field_uid != "":
				tdp = td + (
					') '
					' -[:IS_IN]->(:FieldBlocks)'
					' -[:IS_IN]->(field:Field {uid:$field_uid}) '
				)
			else:
				tdp = td + (
					' ) '
					' -[:IS_IN]->(:FieldBlocks)'
					' -[:IS_IN]->(field:Field) '
				)
			optional_block = ''
			# and generate the return statement
			if data_format == 'table':
				response = (
					# need a with statement to allow order by with COLLECT
					' WITH '
					' 	country.name as Country, '
					' 	region.name as Region, '
					' 	farm.name as Farm, '
					' 	field.name as Field, '
					' 	field.uid as `Field UID`, '
					' 	block.name as Block, '
					' 	block.uid as `Block UID`, '
					'	block.id as `Block ID`, '
					'	COLLECT([trait.name, data.value]) as Traits '
					' RETURN '
					'	{ ' 
					'		Country : Country, '
					'		Region : Region, '
					'		Farm : Farm, '
					'		Field : Field, '
					'		`Field UID` : `Field UID`, '
					'		Block : Block, '
					'		UID: `Block UID`, '
					'		`Block ID`: `Block ID`,'
					'		Traits : Traits } '
					' ORDER BY '
					'	`Field UID`, `Block ID` '
				)
			else:  # if data_format == 'db':
				response = (
					' RETURN { '
					'	User: user_name, '
					'	Partner: partner_name,'
					'	Country: country.name, ' 
					'	Region: region.name, '
					'	Farm: farm.name, '
					'	Field: field.name, '
					'	`Field UID`: field.uid, '
					'	Block: block.name, '
					'	UID: block.uid, '
					'	`Block ID`: block.id, '
					'	Trait: trait.name, '
					'	Value: data.value, '
					'	Location: data.location, '
					'	`Recorded at`: apoc.date.format(data.time), '
					'	`Recorded by`: data.person '
					' } '
					' ORDER BY '
					'	field.uid, block.id, trait.name, data.time '
					)
		else:  # level == 'field':
			index_fieldnames = [
				'Country',
				'Region',
				'Farm',
				'Field',
				'UID'
			]
			tdp = (
				' MATCH '
				'	(trait:FieldTrait) '
				'	<-[:FOR_TRAIT]-(field_trait) '
				'	<-[:DATA_FOR]-(data) '
				' WHERE (trait.name) IN $traits'
				' WITH user_name, partner_name, data, trait, field_trait '
				' MATCH '
				'	(field_trait) '
				'	-[:FOR_ITEM]->(field: Field) '
			)
			optional_block = ''
			# and generate the return statement
			if data_format == 'table':
				response = (
					# need a WITH statement to allow order by with COLLECT
					' WITH '
					'	country.name as Country, '
					'	region.name as Region, '
					'	farm.name as Farm, '
					'	field.name as Field, '
					'	field.uid as `Field UID`, '
					'	COLLECT([trait.name, data.value]) as Traits '
					' RETURN '
					'	{ ' 
					'		Country: Country, '
					'		Region: Region, '
					'		Farm: Farm, '
					'		Field: Field, '
					'		UID: `Field UID`, '
					'		Traits: Traits '
					'	} '
					' ORDER BY '
					'	`Field UID` '
				)
			else:  # if data_format == 'db':
				response = (
					' RETURN { '
					'	User: user_name, '
					'	Partner: partner_name, '
					'	Country: country.name, ' 
					'	Region: region.name, '
					'	Farm: farm.name, '
					'	Field: field.name, '
					'	UID: field.uid, '
					'	Trait: trait.name, '
					'	Value: data.value, '
					'	Location: data.location, '
					'	`Recorded at`: apoc.date.format(data.time), '
					'	`Recorded by`: data.person '
					' } '
					' ORDER BY field.uid, trait.name, data.time '
					)
		# add conditional for data between time range
		if start_time != "" and end_time != "":
			time_condition = ' WHERE data.time > $start_time AND data.time < $end_time '
		elif start_time == "" and end_time != "":
			time_condition = ' WHERE data.time < $end_time '
		elif start_time != "" and end_time == "":
			time_condition = ' WHERE data.time > $start_time '
		else:
			time_condition = ''
		# finalise query, get data
		query = user_data_query + tdp + frc + time_condition + optional_block + response
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				self._get_csv,
				query,
				country,
				region,
				farm,
				field_uid,
				block_uid,
				traits,
				start_time,
				end_time
			)
		# check if any data found, if not return none
		if len(result) == 0:
			return None
		# prepare data and variables to make file
		if data_format == 'table':
			# expand the traits and values as key:value pairs (each measure as an item in a list if more than one)
			for record in result:
				for i in record["Traits"]:
					# if the trait hasn't appeared yet
					if not i[0] in record:
						# if the new value isn't a list just store it as a string
						if not isinstance(i[1], list):
							record.update({i[0]: str(i[1])})
						# if the new value is a list (multicat)
						# make all the elements of the list into strings
						# and make a string out of this list of strings
						else:
							record.update({i[0]: str([str(y) for y in i[1]])})
					# if the trait is already there
					else:
						# if the stored value for this trait is not a list
						if not isinstance(record[i[0]], list):
							#  - we make the existing value an item in a list
							record.update({i[0]: [record[i[0]]]})
						# Need to handle multicats that have lists of values as a single data point
						# these always return a list (even if 1 element) so at least it is consistent
						# in this case we can determine if it is a multicat by checking if the new value is a list
						if not isinstance(i[1], list):
							record[i[0]].append(str(i[1]))
						else:
							record[i[0]].append(str([str(y) for y in i[1]]))
			trait_fieldnames = traits
			# now create the csv file from result and fieldnames
			fieldnames = index_fieldnames + trait_fieldnames
		else:  # if data_format == 'db':
			fieldnames = index_fieldnames + ['Trait', 'Value', 'Location', 'Recorded at', 'Recorded by']
		# create the file path
		time = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
		filename = time + '_data.csv'
		file_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], self.username, filename)
		# create user download path if not found
		download_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], self.username)
		if not os.path.isdir(download_path):
			os.mkdir(download_path)
			gid = grp.getgrnam(app.config['CELERYGRPNAME']).gr_gid
			os.chown(download_path, -1, gid)
			os.chmod(download_path, 0775)
		# make the file
		with open(file_path, 'w') as csv_file:
			writer = csv.DictWriter(
				csv_file,
				fieldnames = fieldnames,
				quoting = csv.QUOTE_ALL,
				extrasaction = 'ignore')
			writer.writeheader()
			for row in result:
				for item in row:
					if isinstance(row[item], list):
						row[item] = [str(i).encode() for i in row[item]]
				writer.writerow(row)
			file_size = csv_file.tell()
		return {
			"filename": filename,
			"file_path": file_path,
			"file_size": file_size
		}

	def _get_csv(
			self,
			tx,
			query,
			country,
			region,
			farm,
			field_uid,
			block_uid,
			traits,
			start_time,
			end_time
	):
		# perform transaction and return the simplified result
		# remove all the transaction metadata and just keep the result map
		return [record[0] for record in tx.run(
			query,
			username = self.username,
			country = country,
			region =region,
			farm = farm,
			field_uid = field_uid,
			block_uid = block_uid,
			traits = traits,
			start_time = start_time,
			end_time = end_time
		)]

	def get_index_csv(
			self,
			form_data,
			existing_ids = None
	):
		if form_data['trait_level'] == 'field':
			parameters = {}
			query = 'MATCH (c:Country '
			if form_data['country'] != '':
				query += '{name_lower: toLower($country)}'
				parameters['country'] = form_data['country']
			query += ')<-[:IS_IN]-(r:Region '
			if form_data['region'] != '':
				query += '{name_lower: toLower($region)}'
				parameters['region'] = form_data['region']
			query += ')<-[:IS_IN]-(f:Farm '
			if form_data['farm'] != '':
				query += '{name_lower: toLower($farm)}'
				parameters['farm'] = form_data['farm']
			query += (
				' )<-[IS_IN]-(field:Field) '
				' RETURN { '
				'	UID: field.uid, '
				'	Field: field.name, '
				'	Farm: f.name, '
				'	Region: r.name, '
				'	Country: c.name '
				' } '
				' ORDER BY field.uid'
			)
			# make the file and return a dictionary with filename, file_path and file_size
			with get_driver().session() as neo4j_session:
				result = neo4j_session.read_transaction(
					neo4j_query,
					query,
					parameters)
				id_list = [record[0] for record in result]
			if len(id_list) == 0:
				return None
			fieldnames = [
				'Country',
				'Region',
				'Farm',
				'Field',
				'UID'
			]
			if 'farm' in parameters:
				filename = form_data['farm'] + '_'
			elif 'region' in parameters:
				filename = form_data['region'] + '_'
			elif 'country' in parameters:
				filename = form_data['country'] + '_'
			else:
				filename = ''
			csv_file_details = self.make_csv_file(
				fieldnames,
				id_list,
				filename + 'Field_UIDs.csv'
			)
		else:  # form_data['trait_level'] in ['block', 'tree', 'branch', 'leaf', 'sample']:
			field_uid = int(form_data['field'])
			if form_data['trait_level'] == 'sample':
				parameters = {
					'country': form_data['country'],
					'region': form_data['region'],
					'farm': form_data['farm'],
					'field_uid': int(field_uid),
					'trees_start': int(form_data['trees_start']) if form_data['trees_start'] else '',
					'trees_end': int(form_data['trees_end']) if form_data['trees_end'] else '',
					'replicates': int(form_data['replicates']) if form_data['replicates'] else "",
					'tissue': form_data['tissue'],
					'storage': form_data['storage'],
					'start_time': int(
						(
							datetime.strptime(form_data['date_from'], '%Y-%m-%d')
							- datetime(1970, 1, 1)
						).total_seconds() * 1000) if form_data['date_from'] != '' else '',
					'end_time': int(
						(
							datetime.strptime(form_data['date_to'], '%Y-%m-%d')
							- datetime(1970, 1, 1)
						).total_seconds() * 1000) if form_data['date_to'] != '' else '',
					'samples_start': int(form_data['samples_start']) if form_data['samples_start'] else "",
					'samples_end': int(form_data['samples_end']) if form_data['samples_end'] else ""
				}
				# build the file and return filename etc.
				fieldnames = [
					'Country',
					'Region',
					'Farm',
					'Field',
					'Field UID',
					'Block',
					'Block UID',
					'Tree UID',
					'Tree Custom ID',
					'Variety',
					'Branch UID',
					'Leaf UID',
					'Tissue',
					'Storage',
					'Date Sampled',
					'UID'
				]
				id_list = Samples().get_samples(parameters)
				if len(id_list) == 0:
					return None
				sample_replicates = int(form_data['sample_replicates']) if form_data['sample_replicates'] else 1
				if sample_replicates > 1:
					fieldnames.insert(15, 'Sample UID')
					id_list_reps = []
					for sample in id_list:
						sample['Sample UID'] = sample['UID']
						for i in range(sample_replicates):
							sample['UID'] = str(sample['Sample UID']) + "." + str(int(i + 1))
							id_list_reps.append(sample.copy())
					id_list = id_list_reps
				csv_file_details = self.make_csv_file(
					fieldnames,
					id_list,
					'Sample_UIDs.csv')
			elif form_data['trait_level'] == 'leaf':
				fieldnames = [
					'Country',
					'Region',
					'Farm',
					'Field',
					'Field UID',
					'Block',
					'Block UID',
					'Tree UID',
					'Tree Custom ID',
					'Variety',
					'Branch UID',
					'UID'
				]
				if form_data['old_new_ids'] == 'old':
					parameters = {
						'country': form_data['country'],
						'region': form_data['region'],
						'farm': form_data['farm'],
						'field_uid': field_uid,
						'trees_start': int(form_data['trees_start']) if form_data['trees_start'] else 1,
						'trees_end': int(form_data['trees_end']) if form_data['trees_end'] else 999999,
						'start_time': int(
							(
								datetime.strptime(form_data['date_from'], '%Y-%m-%d')
								- datetime(1970, 1, 1)
							).total_seconds() * 1000
						) if form_data['date_from'] != '' else '',
						'end_time': int(
							(
								datetime.strptime(form_data['date_to'], '%Y-%m-%d')
								- datetime(1970, 1, 1)
							).total_seconds() * 1000) if form_data['date_to'] != '' else '',
						'leaves_start': int(form_data['leaves_start']) if form_data['leaves_start'] != '' else 1,
						'leaves_end': int(form_data['leaves_end']) if form_data['leaves_end'] != ''else 999999
					}
					with get_driver().session() as neo4j_session:
						result = neo4j_session.read_transaction(neo4j_query, Cypher.leaves_get, parameters)
					id_list = [record[0] for record in result]
					if len(id_list) == 0:
						return None
					csv_file_details = self.make_csv_file(
						fieldnames,
						id_list,
						'Leaf_UIDs.csv'
					)
				else:
					if len(existing_ids) == 0:
						return None
					first_leaf_id = existing_ids[0]['Leaf ID']
					last_leaf_id = existing_ids[-1]['Leaf ID']
					filename = 'Field_' + str(field_uid) + '_L' + str(first_leaf_id) + '_to_L' + str(
						last_leaf_id) + '.csv'
					csv_file_details = self.make_csv_file(
						fieldnames,
						existing_ids,
						filename,
						with_time = False
					)
			elif form_data['trait_level'] == 'branch':
				fieldnames = [
					'Country',
					'Region',
					'Farm',
					'Field',
					'Field UID',
					'Block',
					'Block UID',
					'Tree UID',
					'Variety',
					'Tree Custom ID',
					'UID'
				]
				if form_data['old_new_ids'] == 'old':
					parameters = {
						'country': form_data['country'],
						'region': form_data['region'],
						'farm': form_data['farm'],
						'field_uid': field_uid,
						'trees_start': int(form_data['trees_start']) if form_data['trees_start'] else 1,
						'trees_end': int(form_data['trees_end']) if form_data['trees_end'] else 999999,
						'start_time': int(
							(
								datetime.strptime(form_data['date_from'], '%Y-%m-%d')
								- datetime(1970, 1, 1)
							).total_seconds() * 1000
						) if form_data['date_from'] != '' else '',
						'end_time': int(
							(
								datetime.strptime(form_data['date_to'], '%Y-%m-%d')
								- datetime(1970, 1, 1)
							).total_seconds() * 1000
						) if form_data['date_to'] != '' else '',
						'branches_start': int(form_data['branches_start']) if form_data['branches_start'] != '' else 1,
						'branches_end': int(form_data['branches_end']) if form_data['branches_end'] != ''else 999999
					}
					with get_driver().session() as neo4j_session:
						result = neo4j_session.read_transaction(neo4j_query, Cypher.branches_get, parameters)
					id_list = [record[0] for record in result]
					if len(id_list) == 0:
						return None
					csv_file_details = self.make_csv_file(
						fieldnames,
						id_list,
						'Branch_UIDs.csv'
					)
				else:
					if len(existing_ids) == 0:
						return None
					first_branch_id = existing_ids[0]['Branch ID']
					last_branch_id = existing_ids[-1]['Branch ID']
					filename = 'field_' + str(field_uid) + '_R' + str(first_branch_id) + '_to_R' + str(
						last_branch_id) + '.csv'
					csv_file_details = self.make_csv_file(
						fieldnames,
						existing_ids,
						filename,
						with_time = False
					)
			elif form_data['trait_level'] == 'tree':
				trees_start = int(form_data['trees_start'] if form_data['trees_start'] else 0)
				trees_end = int(form_data['trees_end'] if form_data['trees_end'] else 999999)
				# make the file and return filename, path, size
				fieldnames = [
					'Country',
					'Region',
					'Farm',
					'Field',
					'Field UID',
					'Block',
					'Block UID',
					'Variety',
					'Tree Custom ID',
					'UID'
				]
				id_list = Fields.get_trees(field_uid, trees_start, trees_end)
				if len(id_list) == 0:
					return None
				first_tree_id = id_list[0]['Tree ID']
				last_tree_id = id_list[-1]['Tree ID']
				csv_file_details = self.make_csv_file(
					fieldnames,
					id_list,
					'Tree_UIDs_' + str(first_tree_id) + '-' + str(last_tree_id) + '.csv'
				)
			else:  # form_data['trait_level'] == 'block':
				# make the file and return a dictionary with filename, file_path and file_size
				fieldnames = [
					'Country',
					'Region',
					'Farm',
					'Field',
					'Field UID',
					'Block',
					'UID'
				]
				id_list = Fields.get_blocks(field_uid)
				if len(id_list) == 0:
					return None
				csv_file_details = self.make_csv_file(fieldnames, id_list, 'Block_UIDs.csv')
		return csv_file_details

	# creates the traits.trt file for import to Field-Book
	# TODO may need to replace 'name' with 'trait' in header but doesn't seem to affect Field-Book
	def create_trt(self, form_data):
		node_label = str(form_data['trait_level']).title() + 'Trait'
		selection = [
			item for sublist in [
				form_data.getlist(i) for i in form_data if all(
					['csrf_token' not in i, form_data['trait_level'] + '-' in i]
				)
			]
			for item in sublist
		]
		if len(selection) == 0:
			return None
		selected_traits = Lists(node_label).get_selected(selection, 'name')
		for i, trait in enumerate(selected_traits):
			trait['realPosition'] = str(i + 1)
			trait['isVisible'] = 'True'
		# create user download path if not found
		download_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], self.username)
		if not os.path.isdir(download_path):
			os.mkdir(download_path)
			gid = grp.getgrnam(app.config['CELERYGRPNAME']).gr_gid
			os.chown(download_path, -1, gid)
			os.chmod(download_path, 0775)
		# set variables for file creation
		fieldnames = [
			'name',
			'format',
			'defaultValue',
			'minimum',
			'maximum',
			'details',
			'categories',
			'isVisible',
			'realPosition'
		]
		# make the file
		file_details = self.make_csv_file(fieldnames, selected_traits, form_data['trait_level'] + '.trt')
		return file_details

	def get_table_csv(self, form_data, existing_ids = None):
		# create two files, one is the index + trait names + time details table, the other is a trait description file
		# - specifying format and details of each trait value
		# starts with getting the index data (id_list) and fieldnames (to which the traits will be added)
		if form_data['trait_level'] == 'field':
			parameters = {}
			query = 'MATCH (c:Country '
			if form_data['country'] != '':
				query += '{name_lower: toLower($country)}'
				parameters['country'] = form_data['country']
			query += ')<-[:IS_IN]-(r:Region '
			if form_data['region'] != '':
				query += '{name_lower: toLower($region)}'
				parameters['region'] = form_data['region']
			query += ')<-[:IS_IN]-(f:Farm '
			if form_data['farm'] != '':
				query += '{name_lower: toLower($farm)}'
				parameters['farm'] = form_data['farm']
			query += (
				' )<-[IS_IN]-(field:Field) '
				' RETURN {'
				' UID : field.uid, '
				' Field : field.name, '
				' Farm : f.name, '
				' Region : r.name, '
				' Country : c.name }'
				' ORDER BY field.uid'
			)
			fieldnames = [
				'Country',
				'Region',
				'Farm',
				'Field',
				'UID'
			]
			with get_driver().session() as neo4j_session:
				result = neo4j_session.read_transaction(
					neo4j_query,
					query,
					parameters)
				id_list = [record[0] for record in result]
		else:  # if form_data['trait_level'] in ['block', 'tree', 'branch', 'leaf', 'sample']:
			field_uid = int(form_data['field'])
			if form_data['trait_level'] == 'sample':
				fieldnames = [
					'Country',
					'Region',
					'Farm',
					'Field',
					'Field UID',
					'Block',
					'Block UID',
					'Tree UID',
					'Tree Custom ID',
					'Variety',
					'Tissue',
					'Storage',
					'Date Sampled',
					'UID'
				]
				parameters = {
					'country': form_data['country'],
					'region': form_data['region'],
					'farm': form_data['farm'],
					'field_uid': field_uid,
					'trees_start': int(form_data['trees_start']) if form_data['trees_start'] else "",
					'trees_end': int(form_data['trees_end']) if form_data['trees_end'] else "",
					'replicates': int(form_data['replicates']) if form_data['replicates'] else "",
					'tissue': form_data['tissue'],
					'storage': form_data['storage'],
					'start_time': int(
						(
							datetime.strptime(form_data['date_from'], '%Y-%m-%d')
							- datetime(1970, 1, 1)
						).total_seconds() * 1000
					) if form_data['date_from'] else "",
					'end_time': int(
						(
							datetime.strptime(form_data['date_to'], '%Y-%m-%d')
							- datetime(1970, 1, 1)
						).total_seconds() * 1000
					) if form_data['date_to'] else "",
					'samples_start': int(form_data['samples_start']) if form_data['samples_start'] else "",
					'samples_end': int(form_data['samples_end']) if form_data['samples_end'] else ""
				}
				# build the file and return filename etc.
				id_list = Samples().get_samples(parameters)
			elif form_data['trait_level'] == 'leaf':
				fieldnames = [
					'Country',
					'Region',
					'Farm',
					'Field',
					'Field UID',
					'Block',
					'Block UID',
					'Tree Custom ID',
					'Tree UID',
					'Branch UID',
					'UID'
				]
				if form_data['old_new_ids'] == 'old':
					parameters = {
						'country': form_data['country'],
						'region': form_data['region'],
						'farm': form_data['farm'],
						'field_uid': field_uid,
						'trees_start': int(form_data['trees_start']) if form_data['trees_start'] else 1,
						'trees_end': int(form_data['trees_end']) if form_data['trees_end'] else 999999,
						'start_time': int(
							(
								datetime.strptime(form_data['date_from'], '%Y-%m-%d')
								- datetime(1970, 1, 1)
							).total_seconds() * 1000)
						if form_data['date_from'] != '' else '',
						'end_time': int(
							(
								datetime.strptime(form_data['date_to'], '%Y-%m-%d')
								- datetime(1970, 1, 1)
							).total_seconds() * 1000)
						if form_data['date_to'] != '' else '',
						'leaves_start': int(form_data['leaves_start']) if form_data['leaves_start'] != '' else 1,
						'leaves_end': int(form_data['leaves_end']) if form_data['leaves_end'] != ''else 999999
					}
					with get_driver().session() as neo4j_session:
						result = neo4j_session.read_transaction(neo4j_query, Cypher.leaves_get, parameters)
					id_list = [record[0] for record in result]
				else:
					id_list = existing_ids
			elif form_data['trait_level'] == 'tree':
				trees_start = int(form_data['trees_start'] if form_data['trees_start'] else 0)
				trees_end = int(form_data['trees_end'] if form_data['trees_end'] else 999999)
				# make the file and return filename, path, size
				fieldnames = [
					'Country',
					'Region',
					'Farm',
					'Field',
					'Field UID',
					'Block',
					'Block UID',
					'Variety',
					'Tree Custom ID',
					'UID'
				]
				id_list = Fields.get_trees(field_uid, trees_start, trees_end)
			elif form_data['trait_level'] == 'branch':
				fieldnames = [
					'Country',
					'Region',
					'Farm',
					'Field',
					'Field UID',
					'Block',
					'Block UID',
					'Tree Custom ID',
					'Tree UID',
					'Variety',
					'UID'
				]
				if form_data['old_new_ids'] == 'old':
					parameters = {
						'country': form_data['country'],
						'region': form_data['region'],
						'farm': form_data['farm'],
						'field_uid': field_uid,
						'trees_start': int(form_data['trees_start']) if form_data['trees_start'] else 1,
						'trees_end': int(form_data['trees_end']) if form_data['trees_end'] else 999999,
						'start_time': int(
							(
								datetime.strptime(form_data['date_from'], '%Y-%m-%d')
								- datetime(1970, 1, 1)
							).total_seconds() * 1000) if form_data['date_from'] != '' else '',
						'end_time': int(
							(
								datetime.strptime(form_data['date_to'], '%Y-%m-%d')
								- datetime(1970, 1, 1)
							).total_seconds() * 1000) if form_data['date_to'] != '' else '',
						'branches_start': int(form_data['branches_start']) if form_data['branches_start'] != '' else 1,
						'branches_end': int(form_data['branches_end']) if form_data['branches_end'] != ''else 999999
					}
					with get_driver().session() as neo4j_session:
						result = neo4j_session.read_transaction(neo4j_query, Cypher.branches_get, parameters)
					id_list = [record[0] for record in result]
				else:
					id_list = existing_ids
			else:  # form_data['trait_level'] == 'block':
				# make the file and return a dictionary with filename, file_path and file_size
				fieldnames = [
					'Country',
					'Region',
					'Farm',
					'Field',
					'Field UID',
					'Block',
					'UID'
				]
				id_list = Fields.get_blocks(field_uid)
		# check we have found matching ID's, if not return None
		if len(id_list) == 0:
			return None
		# if requesting replicates for samples then create these in the id_list
		sample_replicates = int(form_data['sample_replicates']) if form_data['sample_replicates'] else 1
		if sample_replicates > 1:
			fieldnames.insert(13, 'Sample UID')
			id_list_reps = []
			for sample in id_list:
				sample['Sample UID'] = sample['UID']
				for i in range(sample_replicates):
					sample['UID'] = str(sample['Sample UID']) + "." + str(int(i + 1))
					id_list_reps.append(sample.copy())
			id_list = id_list_reps
		# then we get the traits list from the form
		node_label = str(form_data['trait_level']).title() + 'Trait'
		selection = [
			item for sublist in [
				form_data.getlist(i) for i in form_data if all(
					['csrf_token' not in i, form_data['trait_level'] + '-' in i]
				)
			]
			for item in sublist
		]
		# if there are no selected traits exit here returning None
		if len(selection) == 0:
			return None
		# check that they are in the database
		selected_traits = Lists(node_label).get_selected(selection, 'name')
		# make the table file
		fieldnames += ['Date', 'Time']
		fieldnames += [trait['name'] for trait in selected_traits]
		table_file = self.make_csv_file(fieldnames, id_list, 'table.csv')
		# and a file that describes the trait details
		trait_fieldnames = [
			'name',
			'format',
			'minimum',
			'maximum',
			'details',
			'category_list'
		]
		details_file = self.make_csv_file(trait_fieldnames, selected_traits, 'details.csv')
		return {
			'table': table_file,
			'details': details_file
		}
