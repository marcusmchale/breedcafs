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

#User class related (all uploads are tied to a user) yet more specifically regarding uploads
class Download:
	def __init__(self, username):
		self.username = username
	def make_csv_file(self, fieldnames, id_list, filename, with_time = True):
		#create user download path if not found
		download_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], self.username)
		if not os.path.isdir(download_path):
			os.mkdir(download_path)
			gid = grp.getgrnam(app.config['CELERYGRPNAME']).gr_gid
			os.chown(download_path, -1, gid)
			os.chmod(download_path, 0775)
		#prepare variables to write the file
		time = datetime.now().strftime('%Y%m%d-%H%M%S')
		if with_time:
			filename = time + '_' + filename
		file_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username'], filename)
		# handle case where user is making more than one file a minute (per second filenames)
		if os.path.isfile(file_path):
			time = datetime.now().strftime('%Y%m%d-%H%M%S')
			filename = time + '_' + filename
			file_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username'], filename)
		# make the file
		with open(file_path, 'w') as file:
			writer = csv.DictWriter(file,
				fieldnames = fieldnames,
				quoting = csv.QUOTE_ALL,
				extrasaction = 'ignore')
			writer.writeheader()
			for row in id_list:
				for item in row:
					if isinstance(row[item], list):
						row[item] = [i.encode() for i in row[item]]
				#for key, value in row:
				writer.writerow(row)
			file_size = file.tell()
		# return file details
		return {
			"filename":filename,
			"file_path":file_path,
			"file_size":file_size,
			"url": url_for(
				'download_file',
				username = self.username,
				filename = filename,
				_external = True
			)
		}
	def get_csv(self,
			country, 
			region, 
			farm, 
			plotID, 
			blockUID, 
			level, 
			traits, 
			data_format, 
			start_time, 
			end_time):
		self.country = country
		self.region = region
		self.farm = farm
		self.plotID = plotID
		self.blockUID = blockUID
		self.traits = traits
		self.start_time = start_time
		self.end_time = end_time
		#build query strings - always be careful not to allow injection when doing this!!
		#all of the input needs to be validated (in this case it is already done by WTForms checking against selectfield options)
		#but as I am not entirely sure about the security of this layer I am adding another check of the values that are used to concatenate the string
		node_label = str(level).title() + 'Trait'
		all_level_traits = Lists(node_label).create_list('name')
		traits = [str(i) for i in traits if i in all_level_traits]
		#First I limit the data nodes to those submitted by members of an affiliated institute
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
		#this section first as not dependent on level but part of query build
		if country == "" :
			frc = (
				' -[:IS_IN]->(farm: Farm) '
				' -[:IS_IN]->(region: Region) '
				' -[:IS_IN]->(country: Country) '
			)
		elif country != "" and region == "":
			frc = (
				' -[:IS_IN]->(farm: Farm) '
				' -[:IS_IN]->(region: Region) '
				' -[:IS_IN]->(country: Country {name_lower: toLower($country)}) '
			)
		elif region != "" and farm == "" :
			frc = (' <-[:IS_IN]->(farm:Farm) '
				' -[:IS_IN]->(region:Region {name_lower: toLower($region)}) '
				' -[:IS_IN]->(country:Country {name_lower: toLower($country)}) ')
		elif farm != "" :
			frc = (' -[:IS_IN]->(farm:Farm {name_lower: toLower($farm)}) '
				' -[:IS_IN]->(region:Region {name_lower: toLower($region)}) '
				' -[:IS_IN]->(country:Country {name_lower: toLower($country)}) ')
		#then level dependent stuff
		if level == 'sample':
			index_fieldnames = [
				'Country',
				'Region',
				'Farm',
				'Plot',
				'PlotUID',
				'Block',
				'BlockUID',
				'TreeUID',
				'TreeCustomID',
				'Variety',
				'BranchUID',
				'LeafUID',
				'UID',
				'Tissue',
				'Storage',
				'SampleDate'
			]
			td = (
				' MATCH '
				'	(trait:SampleTrait) '
				'	<-[FOR_TRAIT*2]-(sample_trait) '
				'	<-[DATA_FOR]-(data)'
				' WHERE (trait.name) IN ' + str(traits) +
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
			#if no plot selected
			if plotID == "" :
				tdp = td + ( ' -[:IS_IN]->(PlotTrees) '
					' -[:IS_IN]->(plot:Plot) ' )
				#query = tdp + frc
				optional_block = ( ' OPTIONAL MATCH (tree) '
					' -[:IS_IN]->(:BlockTrees) '
					' -[:IS_IN]->(block:Block) ' )
			#if plotID is defined (but no blockUID)
			elif blockUID == "" and plotID != "" :
				tdp = td + ( ' -[:IS_IN]->(PlotTrees) '
					' -[:IS_IN]->(plot:Plot {uid:$plotID}) ' )
				#query = tdp + frc
				optional_block = ( ' OPTIONAL MATCH (tree) '
					' -[:IS_IN]->(:BlockTrees) '
					' -[:IS_IN]->(block:Block) ')
			#else blockID is defined
			else: # blockUID != "":
				tdp = td + ( '-[:IS_IN]->(:BlockTrees) '
					' -[:IS_IN]->(block:Block {uid:$blockUID}) '
					' -[:IS_IN]->(:PlotBlocks) '
					' -[:IS_IN]->(plot:Plot) ')
				#query = tdp + frc
				optional_block = ''
			optional_block += (
				' OPTIONAL MATCH '
				'	(sample)-[:FROM_BRANCH {current: True}]->(branch:Branch) '
				' OPTIONAL MATCH '
				'	(sample)-[:FROM_LEAF {current:True}]->(leaf:Leaf) '
			)
			#and generate the return statement
			if data_format == 'table':
				response = ( 
					#need a with statement to allow order by with COLLECT
					' WITH '
					' 	country.name as Country, '
					' 	region.name as Region, '
					' 	farm.name as Farm, '
					' 	plot.name as Plot, '
					' 	plot.uid as PlotUID, '
					' 	block.name as Block, '
					' 	block.uid as BlockUID, '
					' 	tree.uid as TreeUID, '
					' 	tree.custom_id as TreeCustomID, '
					' 	tree.variety as Variety, '
					' 	branch.uid as BranchUID, '
					' 	leaf.uid as LeafUID, '
					' 	sample.uid as SampleUID, '
					'	tissue.name as Tissue, '
					'	storage.name as Storage, '
					'	sample.date as SampleDate, '
					' 	sample.id as SampleID, '
					'	COLLECT([trait.name, data.value]) as Traits '
					' RETURN '
					'	{ ' 
					'		Country : Country, '
					'		Region : Region, '
					'		Farm : Farm, '
					'		Plot : Plot, '
					'		PlotUID : PlotUID, '
					'		Block : Block, '
					'		BlockUID: BlockUID, '
					'		TreeUID : TreeUID, '
					'		TreeCustomID: TreeCustomID, '
					'		Variety: Variety, '
					'		BranchUID: BranchUID, '
					'		LeafUID: LeafUID, '
					'		UID : SampleUID ,'
					'		Tissue: Tissue, '
					'		Storage: Storage, '
					'		SampleDate: SampleDate, '
					'		SampleID : SampleID, '
					'		Traits : Traits } '
					' ORDER BY '
						' PlotUID, SampleID '
				)
			elif data_format == 'db':
				response = (
					' RETURN {'
						' User : user_name, '
						' Partner : partner_name,'
						' Country : country.name, ' 
						' Region : region.name, '
						' Farm : farm.name, '
						' Plot : plot.name, '
						' PlotUID: plot.uid, '
						' Block : block.name, '
						' BlockUID : block.uid, '
						' TreeUID : tree.uid, '
						' TreeCustomID: tree.custom_id,	'
						' Variety: tree.variety, '
						' SampleUID : sample.uid, '
						' BranchUID: branch.uid, '
						' LeafUID: leaf.uid, '
						' UID : sample.uid, '
						' Tissue: tissue.name, '
						' Storage: storage.name, '
						' SampleDate: sample.date, '
						' SampleID : sample.id, '
						' Trait : trait.name, '
						' Value : data.value, '
						' Location : data.location, '
						' Recorded_at : apoc.date.format(data.time), '
						' Recorded_by: data.person '
					' } '
					' ORDER BY plot.uid, sample.id, trait.name, apoc.date.format(data.time) '
					)
		elif level == 'leaf':
			index_fieldnames = [
				'Country',
				'Region',
				'Farm',
				'Plot',
				'PlotUID',
				'Block',
				'BlockUID',
				'TreeUID',
				'TreeCustomID',
				'Variety',
				'BranchUID',
				'UID'
			]
			td = (
				' MATCH '
				'	(trait:LeafTrait) '
				'	<-[:FOR_TRAIT*2]-(leaf_trait) '
				'	<-[:DATA_FOR]-(data) '
				' WHERE (trait.name) IN ' + str(traits) +
				' WITH user_name, partner_name, data, trait, leaf_trait '
				' MATCH '
				'	(leaf_trait) '
				'	-[:FOR_ITEM]->(leaf:Leaf) '
				'	-[:FROM_TREE*2]->(tree:Tree) '
			)
			#if no plot selected
			if plotID == "":
				tdp = td + ( ' -[:IS_IN]->(PlotTrees) '
					' -[:IS_IN]->(plot:Plot) ' )
				#query = tdp + frc
				optional_block = ( ' OPTIONAL MATCH (tree) '
					' -[:IS_IN {current:True}]->(:BlockTrees) '
					' -[:IS_IN]->(block:Block) ' )
			#if plotID is defined (but no blockUID)
			elif blockUID == "" and plotID != "":
				tdp = td + ( ' -[:IS_IN]->(PlotTrees) '
					' -[:IS_IN]->(plot:Plot {uid:$plotID}) ' )
				#query = tdp + frc
				optional_block = ( ' OPTIONAL MATCH (tree) '
					' -[:IS_IN {current:True}]->(:BlockTrees) '
					' -[:IS_IN]->(block:Block) ')
			#if blockID is defined
			else: # blockUID != "":
				tdp = td + ( '-[:IS_IN {current:True}]->(:BlockTrees) '
					' -[:IS_IN]->(block:Block {uid:$blockUID}) '
					' -[:IS_IN]->(:PlotBlocks) '
					' -[:IS_IN]->(plot:Plot) ')
				#query = tdp + frc
				optional_block = ''
			optional_block += (
				' OPTIONAL MATCH '
				'	(leaf)-[:FROM_BRANCH {current: True}]->(branch:Branch) '
			)
			#and generate the return statement
			if data_format == 'table':
				response = (
					#need a with statement to allow order by with COLLECT
					' WITH '
					' 	country.name as Country, '
					' 	region.name as Region, '
					' 	farm.name as Farm, '
					' 	plot.name as Plot, '
					' 	plot.uid as PlotUID, '
					' 	block.name as Block, '
					' 	block.uid as BlockUID, '
					' 	tree.uid as TreeUID, '
					' 	tree.custom_id as TreeCustomID, '
					' 	tree.variety as Variety, '
					' 	branch.uid as BranchUID, '
					' 	leaf.uid as LeafUID, '
					' 	leaf.id as LeafID, '
					'	COLLECT([trait.name, data.value]) as Traits '
					' RETURN '
					'	{ ' 
					'		Country : Country, '
					'		Region : Region, '
					'		Farm : Farm, '
					'		Plot : Plot, '
					'		PlotUID : PlotUID, '
					'		Block : Block, '
					'		BlockUID: BlockUID, '
					'		TreeUID : TreeUID, '
					'		TreeCustomID: TreeCustomID, '
					'		Variety: Variety, '
					'		BranchUID: BranchUID, '
					'		UID: LeafUID, '
					'		LeafID: LeafID, '
					'		Traits : Traits } '
					' ORDER BY '
						' PlotUID, LeafID '
				)
			elif data_format == 'db':
				response = (
					' RETURN {'
						' User : user_name, '
						' Partner : partner_name,'
						' Country : country.name, ' 
						' Region : region.name, '
						' Farm : farm.name, '
						' Plot : plot.name, '
						' PlotUID: plot.uid, '
						' Block : block.name, '
						' BlockUID : block.uid, '
						' TreeUID : tree.uid, '
						' TreeCustomID: tree.custom_id,	'
						' Variety: tree.variety, '
						' BranchUID: branch.uid, '
						' UID: leaf.uid, '
						' LeafID: leaf.id, '
						' Trait : trait.name, '
						' Value : data.value, '
						' Location : data.location, '
						' Recorded_at : apoc.date.format(data.time), '
						' Recorded_by: data.person '
					' } '
					' ORDER BY plot.uid, leaf.id, trait.name, apoc.date.format(data.time) '
					)
		elif level == 'branch':
			index_fieldnames = [
				'Country',
				'Region',
				'Farm',
				'Plot',
				'PlotUID',
				'Block',
				'BlockUID',
				'TreeUID',
				'TreeCustomID',
				'Variety',
				'UID',
			]
			td = (
				' MATCH '
				'	(trait:BranchTrait) '
				'	<-[:FOR_TRAIT*2]-(branch_trait) '
				'	<-[:DATA_FOR]-(data) '
				' WHERE (trait.name) IN ' + str(traits) +
				' WITH user_name, partner_name, data, trait, branch_trait '
				' MATCH '
				'	(branch_trait) '
				'	-[:FOR_ITEM]->(branch:Branch) '
				'	-[:FROM_TREE*2]->(tree:Tree) '
			)
			#if no plot selected
			if plotID == "":
				tdp = td + ( ' -[:IS_IN]->(PlotTrees) '
					' -[:IS_IN]->(plot:Plot) ' )
				#query = tdp + frc
				optional_block = ( ' OPTIONAL MATCH (tree) '
					' -[:IS_IN {current:True}]->(:BlockTrees) '
					' -[:IS_IN]->(block:Block) ' )
			#if plotID is defined (but no blockUID)
			elif blockUID == "" and plotID != "":
				tdp = td + ( ' -[:IS_IN]->(PlotTrees) '
					' -[:IS_IN]->(plot:Plot {uid:$plotID}) ' )
				#query = tdp + frc
				optional_block = ( ' OPTIONAL MATCH (tree) '
					' -[:IS_IN {current:True}]->(:BlockTrees) '
					' -[:IS_IN]->(block:Block) ')
			#if blockID is defined
			else: # blockUID != "":
				tdp = td + ( '-[:IS_IN {current:True}]->(:BlockTrees) '
					' -[:IS_IN]->(block:Block {uid:$blockUID}) '
					' -[:IS_IN]->(:PlotBlocks) '
					' -[:IS_IN]->(plot:Plot) ')
				#query = tdp + frc
				optional_block = ''
			#and generate the return statement
			if data_format == 'table':
				response = (
					#need a with statement to allow order by with COLLECT
					' WITH '
					' 	country.name as Country, '
					' 	region.name as Region, '
					' 	farm.name as Farm, '
					' 	plot.name as Plot, '
					' 	plot.uid as PlotUID, '
					' 	block.name as Block, '
					' 	block.uid as BlockUID, '
					' 	tree.uid as TreeUID, '
					' 	tree.custom_id as TreeCustomID, '
					' 	tree.variety as Variety, '
					' 	branch.uid as BranchUID, '
					'	branch.id as BranchID,'
					'	COLLECT([trait.name, data.value]) as Traits '
					' RETURN '
					'	{ ' 
					'		Country : Country, '
					'		Region : Region, '
					'		Farm : Farm, '
					'		Plot : Plot, '
					'		PlotUID : PlotUID, '
					'		Block : Block, '
					'		BlockUID: BlockUID, '
					'		TreeUID : TreeUID, '
					'		TreeCustomID: TreeCustomID, '
					'		Variety: Variety, '
					'		UID: BranchUID, '
					'		BranchID: BranchID,'
					'		Traits : Traits } '
					' ORDER BY '
						' PlotUID, BranchID '
				)
			elif data_format == 'db':
				response = (
					' RETURN {'
						' User : user_name, '
						' Partner : partner_name,'
						' Country : country.name, ' 
						' Region : region.name, '
						' Farm : farm.name, '
						' Plot : plot.name, '
						' PlotUID: plot.uid, '
						' Block : block.name, '
						' BlockUID : block.uid, '
						' TreeUID : tree.uid, '
						' TreeCustomID: tree.custom_id,	'
						' Variety: tree.variety, '
						' UID: branch.uid, '
						' BranchID: branch.id, '
						' Trait : trait.name, '
						' Value : data.value, '
						' Location : data.location, '
						' Recorded_at : apoc.date.format(data.time), '
						' Recorded_by: data.person '
					' } '
					' ORDER BY plot.uid, branch.id, trait.name, apoc.date.format(data.time) '
					)
		elif level == 'tree':
			index_fieldnames = [
				'Country',
				'Region',
				'Farm',
				'Plot',
				'PlotUID',
				'Block',
				'BlockUID',
				'UID',
				'TreeCustomID',
				'Variety'
			]
			td = (
				' MATCH '
				'	(trait:TreeTrait) '
				'	<-[:FOR_TRAIT*2]-(tree_trait) '
				'	<-[:DATA_FOR]-(data) '
				' WHERE (trait.name) IN ' + str(traits) +
				' WITH user_name, partner_name, data, trait, tree_trait '
				' MATCH '
				'	(tree_trait) '
				'	-[:FOR_ITEM]->(tree:Tree) '
			)
			#if no plot selected
			if plotID == "":
				tdp = td + ( ' -[:IS_IN]->(PlotTrees) '
					' -[:IS_IN]->(plot:Plot) ' )
				#query = tdp + frc
				optional_block = ( ' OPTIONAL MATCH (tree) '
					' -[:IS_IN {current:True}]->(:BlockTrees) '
					' -[:IS_IN]->(block:Block) ' )
			#if plotID is defined (but no blockUID)
			elif blockUID == "" and plotID != "":
				tdp = td + ( ' -[:IS_IN]->(PlotTrees) '
					' -[:IS_IN]->(plot:Plot {uid:$plotID}) ' )
				#query = tdp + frc
				optional_block = ( ' OPTIONAL MATCH (tree) '
					' -[:IS_IN {current:True}]->(:BlockTrees) '
					' -[:IS_IN]->(block:Block) ')
			#if blockID is defined
			else: # blockUID != "":
				tdp = td + ( '-[:IS_IN {current:True}]->(:BlockTrees) '
					' -[:IS_IN]->(block:Block {uid:$blockUID}) '
					' -[:IS_IN]->(:PlotBlocks) '
					' -[:IS_IN]->(plot:Plot) ')
				#query = tdp + frc
				optional_block = ''
			#and generate the return statement
			if data_format == 'table':
				response = (
					#need a with statement to allow order by with COLLECT
					' WITH '
					' 	country.name as Country, '
					' 	region.name as Region, '
					' 	farm.name as Farm, '
					' 	plot.name as Plot, '
					' 	plot.uid as PlotUID, '
					' 	block.name as Block, '
					' 	block.uid as BlockUID, '
					' 	tree.uid as TreeUID, '
					' 	tree.custom_id as TreeCustomID, '
					' 	tree.variety as Variety, '
					'	tree.id as TreeID,'
					'	COLLECT([trait.name, data.value]) as Traits '
					' RETURN '
					'	{ ' 
					'		Country : Country, '
					'		Region : Region, '
					'		Farm : Farm, '
					'		Plot : Plot, '
					'		PlotUID : PlotUID, '
					'		Block : Block, '
					'		BlockUID: BlockUID, '
					'		UID : TreeUID, '
					'		TreeCustomID: TreeCustomID, '
					'		Variety: Variety, '
					'		TreeID: TreeID,'
					'		Traits : Traits } '
					' ORDER BY '
						' PlotUID, TreeID '
				)
			elif data_format == 'db':
				response = (
					' RETURN {'
						' User : user_name, '
						' Partner : partner_name,'
						' Country : country.name, ' 
						' Region : region.name, '
						' Farm : farm.name, '
						' Plot : plot.name, '
						' PlotUID: plot.uid, '
						' Block : block.name, '
						' BlockUID : block.uid, '
						' UID : tree.uid, '
						' TreeCustomID: tree.custom_id,	'
						' Variety: tree.variety, '
						' TreeID: tree.id, '
						' Trait : trait.name, '
						' Value : data.value, '
						' Location : data.location, '
						' Recorded_at : apoc.date.format(data.time), '
						' Recorded_by: data.person '
					' } '
					' ORDER BY plot.uid, tree.id, trait.name, apoc.date.format(data.time) '
					)
		elif level == 'block':
			index_fieldnames = [
				'Country',
				'Region',
				'Farm',
				'Plot',
				'PlotUID',
				'Block',
				'UID'
			]
			td = (
				' MATCH '
				'	(trait:BlockTrait) '
				'	<-[:FOR_TRAIT*2]-(block_trait) '
				'	<-[:DATA_FOR]-(data) '
				' WHERE (trait.name) IN ' + str(traits) +
				' WITH user_name, partner_name, data, trait, block_trait '
				' MATCH '
				'	(block_trait) '
				'	-[:FOR_ITEM]->(block:Block '
			)
			if blockUID != "" :
				tdp = td + ( ' {uid:$blockUID}) '
					' -[:IS_IN]->(:PlotBlocks) '
					' -[:IS_IN]->(plot:Plot) ' )
			elif blockUID == "" and plotID != "":
				tdp = td + ( ') '
					' -[:IS_IN]->(:PlotBlocks)'
					' -[:IS_IN]->(plot:Plot {uid:$plotID}) ' )
			elif plotID == "" :
				tdp = td + ( ') '
					' -[:IS_IN]->(:PlotBlocks)'
					' -[:IS_IN]->(plot:Plot) ' )
			optional_block = ''
			#and generate the return statement
			if data_format == 'table':
				response = (
					# need a with statement to allow order by with COLLECT
					' WITH '
					' 	country.name as Country, '
					' 	region.name as Region, '
					' 	farm.name as Farm, '
					' 	plot.name as Plot, '
					' 	plot.uid as PlotUID, '
					' 	block.name as Block, '
					' 	block.uid as BlockUID, '
					'	block.id as BlockID, '
					'	COLLECT([trait.name, data.value]) as Traits '
					' RETURN '
					'	{ ' 
					'		Country : Country, '
					'		Region : Region, '
					'		Farm : Farm, '
					'		Plot : Plot, '
					'		PlotUID : PlotUID, '
					'		Block : Block, '
					'		UID: BlockUID, '
					'		BlockID: BlockID,'
					'		Traits : Traits } '
					' ORDER BY '
					'	PlotUID, BlockID '
				)
			elif data_format == 'db':
				response = (
					' RETURN {'
						' User : user_name, '
						' Partner : partner_name,'
						' Country : country.name, ' 
						' Region : region.name, '
						' Farm : farm.name, '
						' Plot : plot.name, '
						' PlotUID: plot.uid, '
						' Block : block.name, '
						' UID : block.uid, '
						' BlockID: block.id, '
						' Trait : trait.name, '
						' Value : data.value, '
						' Location : data.location, '
						' Recorded_at : apoc.date.format(data.time), '
						' Recorded_by: data.person '
					' } '
					' ORDER BY plot.uid, block.id, trait.name, apoc.date.format(data.time) '
					)
		elif level == 'plot':
			index_fieldnames = [
				'Country',
				'Region',
				'Farm',
				'Plot',
				'UID'
			]
			tdp = (
				' MATCH '
				'	(trait:PlotTrait) '
				'	<-[:FOR_TRAIT]-(plot_trait) '
				'	<-[:DATA_FOR]-(data) '
				' WHERE (trait.name) IN ' + str(traits) +
				' WITH user_name, partner_name, data, trait, plot_trait '
				' MATCH '
				'	(plot_trait) '
				'	-[:FOR_ITEM]->(plot:Plot) '
			)
			optional_block = ''
			#and generate the return statement
			if data_format == 'table':
				response = (
					#need a with statement to allow order by with COLLECT
					' WITH '
					' 	country.name as Country, '
					' 	region.name as Region, '
					' 	farm.name as Farm, '
					' 	plot.name as Plot, '
					' 	plot.uid as PlotUID, '
					'	COLLECT([trait.name, data.value]) as Traits '
					' RETURN '
					'	{ ' 
					'		Country : Country, '
					'		Region : Region, '
					'		Farm : Farm, '
					'		Plot : Plot, '
					'		UID : PlotUID, '
					'		Traits : Traits } '
					' ORDER BY '
						' PlotUID '
				)
			elif data_format == 'db':
				response = (
					' RETURN {'
						' User : user_name, '
						' Partner : partner_name,'
						' Country : country.name, ' 
						' Region : region.name, '
						' Farm : farm.name, '
						' Plot : plot.name, '
						' UID: plot.uid, '
						' Trait : trait.name, '
						' Value : data.value, '
						' Location : data.location, '
						' Recorded_at : apoc.date.format(data.time), '
						' Recorded_by: data.person '
					' } '
					' ORDER BY plot.uid, trait.name, apoc.date.format(data.time) '
					)
		#add conditional for data between time range
		if start_time != "" and end_time != "":
			time_condition = ' WHERE data.time > $start_time AND data.time < $end_time '
		elif start_time == "" and end_time != "":
			time_condition = ' WHERE data.time < $end_time '
		elif start_time != "" and end_time == "":
			time_condition = ' WHERE data.time > $start_time '
		else:
			time_condition = ''
		#finalise query, get data 
		query = user_data_query + tdp + frc + time_condition + optional_block + response
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(self._get_csv, query)
		#check if any data found, if not return none
		if len(result) == 0:
			return None
		#prepare data and variablesto make file
		if data_format == 'table':
			#expand the traits and values as key:value pairs (each measure as an item in a list if more than one)
			for record in result:
				for i in record["Traits"]:
					if i[0] in record:
						if isinstance(record[i[0]] , list):
							record[i[0]].append(str(i[1]))
						else:
							record.update({i[0]:[record[i[0]]]})
							record[i[0]].append(str(i[1]))
					else:
						record.update({i[0]:str(i[1])})
			#the below line only returned the last measurement of the trait, now the above returns as a list.
			#[[record.update({i[0]:i[1]}) for i in record['Traits']] for record in result]
			#the below line was here and it worked but I have no idea how, in the python shell if gives a syntax error..
			#[[record.update({i[0]:i[1] for i in record['Traits']})] for record in result] 
				#now pop out the Traits key and add the returned trait names to a set of fieldnames
				#trait_fieldnames = set()
				#[[trait_fieldnames.add(i[0]) for i in record.pop('Traits')] for record in result]
			#replaced the above with using the fieldnames of traits from the form, this will provide a predictable output from a user perspective )
			#will also allow me to do all this as a stream once the files get larger - thanks Tony for this advice
			trait_fieldnames = traits
			#now create the csv file from result and fieldnames
			fieldnames =  index_fieldnames + trait_fieldnames
		elif data_format == 'db':
			fieldnames = index_fieldnames + ['Trait', 'Value', 'Location', 'Recorded_at', 'Recorded_by']
		#create the file path
		time = datetime.now().strftime('%Y%m%d-%H%M%S')
		filename = time + '_data.csv'
		file_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], self.username, filename)
		#create user download path if not found
		download_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], self.username)
		if not os.path.isdir(download_path):
			os.mkdir(download_path)
			gid = grp.getgrnam(app.config['CELERYGRPNAME']).gr_gid
			os.chown(download_path, -1, gid)
			os.chmod(download_path, 0775)
		#make the file
		with open(file_path,'w') as file:
			writer = csv.DictWriter(file, 
				fieldnames=fieldnames, 
				quoting=csv.QUOTE_ALL,
				extrasaction='ignore')
			writer.writeheader()
			for item in result:
				writer.writerow(item)
			file_size = file.tell()
		return { "filename":filename,
			"file_path":file_path,
			"file_size":file_size }
	def _get_csv(self, tx, query):
		#perform transaction and return the simplified result (remove all the transaction metadata and just keep the result map)
		return [record[0] for record in tx.run(
			query,
			username = self.username,
			country = self.country,
			region = self.region,
			farm = self.farm,
			plotID = self.plotID,
			blockUID = self.blockUID,
			traits = self.traits,
			start_time = self.start_time,
			end_time = self.end_time
		)]
	def get_index_csv(
			self,
			form_data,
			existing_ids = None
	):
		if form_data['trait_level'] == 'plot':
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
				' )<-[IS_IN]-(p:Plot) '
				' RETURN { '
				'	UID: p.uid, '
				'	Plot: p.name, '
				'	Farm: f.name, '
				'	Region: r.name, '
				'	Country: c.name '
				' } '
				' ORDER BY p.uid'
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
				'Plot',
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
				filename + 'plotIDs.csv'
			)
		elif form_data['trait_level'] in ['block','tree','branch','leaf','sample']:
			plotID = int(form_data['plot'])
			if form_data['trait_level'] == 'block':
				# make the file and return a dictionary with filename, file_path and file_size
				fieldnames = [
					'Country',
					'Region',
					'Farm',
					'Plot',
					'PlotUID',
					'Block',
					'UID'
				]
				id_list = Fields.get_blocks(plotID)
				if len(id_list) == 0:
					return None
				csv_file_details = self.make_csv_file(fieldnames, id_list, 'blockIDs.csv')
			elif form_data['trait_level'] == 'tree':
				trees_start = int(form_data['trees_start'] if form_data['trees_start'] else 0)
				trees_end = int(form_data['trees_end'] if form_data['trees_end'] else 999999)
				# make the file and return filename, path, size
				fieldnames = [
					'Country',
					'Region',
					'Farm',
					'Plot',
					'PlotUID',
					'Block',
					'BlockUID',
					'Variety',
					'TreeCustomID',
					'UID'
				]
				id_list = Fields.get_trees(plotID, trees_start, trees_end)
				if len(id_list) == 0:
					return None
				first_tree_id = id_list[0]['TreeID']
				last_tree_id = id_list[-1]['TreeID']
				csv_file_details = self.make_csv_file(
					fieldnames,
					id_list,
					'treeIDs_' + str(first_tree_id) + '-' + str(last_tree_id) + '.csv'
				)
			elif form_data['trait_level'] == 'branch':
				fieldnames = [
					'Country',
					'Region',
					'Farm',
					'Plot',
					'PlotUID',
					'Block',
					'BlockUID',
					'TreeUID',
					'Variety',
					'TreeCustomID',
					'UID'
				]
				if form_data['old_new_ids'] == 'old':
					parameters = {
						'country': form_data['country'],
						'region': form_data['region'],
						'farm': form_data['farm'],
						'plotID': plotID,
						'trees_start': int(form_data['trees_start']) if form_data['trees_start'] else 1,
						'trees_end': int(form_data['trees_end']) if form_data['trees_end'] else 999999,
						'start_time': int((datetime.strptime(form_data['date_from'], '%Y-%m-%d') -	datetime(1970, 1, 1)).total_seconds() * 1000) if form_data['date_from'] != '' else '',
						'end_time': int((datetime.strptime(form_data['date_to'], '%Y-%m-%d')- datetime(1970, 1, 1)).total_seconds() * 1000) if form_data['date_to'] != '' else '',
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
						'branchIDs.csv'
					)
				else:
					if len(existing_ids) == 0:
						return None
					first_branch_id = existing_ids[0]['BranchID']
					last_branch_id = existing_ids[-1]['BranchID']
					filename = 'plot_' + str(plotID) + '_R' + str(first_branch_id) + '_to_R' + str(
						last_branch_id) + '.csv'
					csv_file_details = self.make_csv_file(
						fieldnames,
						existing_ids,
						filename,
						with_time = False
					)
			elif form_data['trait_level'] == 'leaf':
				fieldnames = [
					'Country',
					'Region',
					'Farm',
					'Plot',
					'PlotUID',
					'Block',
					'BlockUID',
					'TreeUID',
					'TreeCustomID',
					'Variety',
					'BranchUID',
					'UID'
				]
				if form_data['old_new_ids'] == 'old':
					parameters = {
						'country': form_data['country'],
						'region': form_data['region'],
						'farm': form_data['farm'],
						'plotID': plotID,
						'trees_start': int(form_data['trees_start']) if form_data['trees_start'] else 1,
						'trees_end': int(form_data['trees_end']) if form_data['trees_end'] else 999999,
						'start_time': int((datetime.strptime(form_data['date_from'], '%Y-%m-%d') -	datetime(1970, 1, 1)).total_seconds() * 1000) if form_data['date_from'] != '' else '',
						'end_time': int((datetime.strptime(form_data['date_to'], '%Y-%m-%d')- datetime(1970, 1, 1)).total_seconds() * 1000) if form_data['date_to'] != '' else '',
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
						'leafIDs.csv'
					)
				else:
					if len(existing_ids) == 0:
						return None
					first_leaf_id = existing_ids[0]['LeafID']
					last_leaf_id = existing_ids[-1]['LeafID']
					filename = 'plot_' + str(plotID) + '_L' + str(first_leaf_id) + '_to_L' + str(
						last_leaf_id) + '.csv'
					csv_file_details = self.make_csv_file(
						fieldnames,
						existing_ids,
						filename,
						with_time = False
					)
			elif form_data['trait_level'] == 'sample':
				parameters = {
					'country': form_data['country'],
					'region': form_data['region'],
					'farm': form_data['farm'],
					'plotID': int(plotID),
					'trees_start': int(form_data['trees_start']) if form_data['trees_start'] else '',
					'trees_end': int(form_data['trees_end']) if form_data['trees_end'] else '',
					'replicates': int(form_data['replicates']) if form_data['replicates'] else "",
					'tissue': form_data['tissue'],
					'storage': form_data['storage'],
					'start_time': int((datetime.strptime(form_data['date_from'], '%Y-%m-%d') -	datetime(1970, 1, 1)).total_seconds() * 1000) if form_data['date_from'] != '' else '',
					'end_time': int((datetime.strptime(form_data['date_to'], '%Y-%m-%d')- datetime(1970, 1, 1)).total_seconds() * 1000) if form_data['date_to'] != '' else '',
					'samples_start': int(form_data['samples_start']) if form_data['samples_start'] else "",
					'samples_end': int(form_data['samples_end']) if form_data['samples_end'] else ""
				}
				# build the file and return filename etc.
				fieldnames = [
					'Country',
					'Region',
					'Farm',
					'Plot',
					'PlotUID',
					'Block',
					'BlockUID',
					'TreeUID',
					'TreeCustomID',
					'Variety',
					'BranchUID',
					'LeafUID',
					'Tissue',
					'Storage',
					'SampleDate',
					'UID'
				]
				id_list = Samples().get_samples(parameters)
				if len(id_list) == 0:
					return None
				sample_replicates = int(form_data['sample_replicates']) if form_data['sample_replicates'] else 1
				if sample_replicates > 1:
					fieldnames.insert(15, 'SampleUID')
					id_list_reps = []
					for sample in id_list:
						sample['SampleUID'] = sample['UID']
						for i in range(sample_replicates):
							sample['UID'] = str(sample['SampleUID']) + "." + str(int(i + 1))
							id_list_reps.append(sample.copy())
					id_list = id_list_reps
				csv_file_details = self.make_csv_file(
					fieldnames,
					id_list,
					'sampleIDs.csv')
		return csv_file_details
		#return the file details
	#creates the traits.trt file for import to Field-Book
	#may need to replace 'name' with 'trait' in header but doesn't seem to affect Field-Book
	def create_trt(self, form_data):
		node_label = str(form_data['trait_level']).title() + 'Trait'
		selection = [
			item for sublist in [
				form_data.getlist(i) for i in form_data if all(['csrf_token' not in i, form_data['trait_level'] + '-' in i])
			]
			for item in sublist
		]
		if len(selection) == 0:
			return None
		selected_traits = Lists(node_label).get_selected(selection, 'name')
		for i, trait in enumerate(selected_traits):
			trait['realPosition'] = str(i + 1)
			trait['isVisible'] = 'True'
		#create user download path if not found
		download_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], self.username)
		if not os.path.isdir(download_path):
			os.mkdir(download_path)
			gid = grp.getgrnam(app.config['CELERYGRPNAME']).gr_gid
			os.chown(download_path, -1, gid)
			os.chmod(download_path, 0775)
		#set variables for file creation
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
		#make the file
		file_details = self.make_csv_file(fieldnames, selected_traits, form_data['trait_level'] + '.trt')
		return file_details
	def get_table_csv(self, form_data, existing_ids = None):
		#create two files, one is the index + trait names + time details table, the other is a trait description file - specifying format and details of each trait value
		# starts with getting the index data (id_list) and fieldnames (to which the traits will be added)
		if form_data['trait_level'] == 'plot':
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
				' )<-[IS_IN]-(p:Plot) '
				' RETURN {'
				' UID : p.uid, '
				' Plot : p.name, '
				' Farm : f.name, '
				' Region : r.name, '
				' Country : c.name }'
				' ORDER BY p.uid'
			)
			fieldnames = [
				'Country',
				'Region',
				'Farm',
				'Plot',
				'UID'
			]
			with get_driver().session() as neo4j_session:
				result = neo4j_session.read_transaction(
					neo4j_query,
					query,
					parameters)
				id_list = [record[0] for record in result]
		elif form_data['trait_level'] in ['block', 'tree', 'branch', 'leaf', 'sample']:
			plotID = int(form_data['plot'])
			if form_data['trait_level'] == 'block':
				# make the file and return a dictionary with filename, file_path and file_size
				fieldnames = [
					'Country',
					'Region',
					'Farm',
					'Plot',
					'PlotUID',
					'Block',
					'UID'
				]
				id_list = Fields.get_blocks(plotID)
			if form_data['trait_level'] == 'tree':
				trees_start = int(form_data['trees_start'] if form_data['trees_start'] else 0)
				trees_end = int(form_data['trees_end'] if form_data['trees_end'] else 999999)
				# make the file and return filename, path, size
				fieldnames = [
					'Country',
					'Region',
					'Farm',
					'Plot',
					'PlotUID',
					'Block',
					'BlockUID',
					'Variety',
					'TreeCustomID',
					'UID'
				]
				id_list = Fields.get_trees(plotID, trees_start, trees_end)
			if form_data['trait_level'] == 'branch':
				fieldnames = [
					'Country',
					'Region',
					'Farm',
					'Plot',
					'PlotUID',
					'Block',
					'BlockUID',
					'TreeCustomID',
					'TreeUID',
					'Variety',
					'UID'
				]
				if form_data['old_new_ids'] == 'old':
					parameters = {
						'country': form_data['country'],
						'region': form_data['region'],
						'farm': form_data['farm'],
						'plotID': plotID,
						'trees_start': int(form_data['trees_start']) if form_data['trees_start'] else 1,
						'trees_end': int(form_data['trees_end']) if form_data['trees_end'] else 999999,
						'start_time': int((datetime.strptime(form_data['date_from'], '%Y-%m-%d') -	datetime(1970, 1, 1)).total_seconds() * 1000) if form_data['date_from'] != '' else '',
						'end_time': int((datetime.strptime(form_data['date_to'], '%Y-%m-%d')- datetime(1970, 1, 1)).total_seconds() * 1000) if form_data['date_to'] != '' else '',
						'branches_start': int(form_data['branches_start']) if form_data['branches_start'] != '' else 1,
						'branches_end': int(form_data['branches_end']) if form_data['branches_end'] != ''else 999999
					}
					with get_driver().session() as neo4j_session:
						result = neo4j_session.read_transaction(neo4j_query, Cypher.branches_get, parameters)
					id_list = [record[0] for record in result]
				else:
					id_list = existing_ids
			if form_data['trait_level'] == 'leaf':
				fieldnames = [
					'Country',
					'Region',
					'Farm',
					'Plot',
					'PlotUID',
					'Block',
					'BlockUID',
					'TreeCustomID',
					'TreeUID',
					'BranchUID',
					'UID'
				]
				if form_data['old_new_ids'] == 'old':
					parameters = {
						'country': form_data['country'],
						'region': form_data['region'],
						'farm': form_data['farm'],
						'plotID': plotID,
						'trees_start': int(form_data['trees_start']) if form_data['trees_start'] else 1,
						'trees_end': int(form_data['trees_end']) if form_data['trees_end'] else 999999,
						'start_time': int((datetime.strptime(form_data['date_from'], '%Y-%m-%d') -	datetime(1970, 1, 1)).total_seconds() * 1000) if form_data['date_from'] != '' else '',
						'end_time': int((datetime.strptime(form_data['date_to'], '%Y-%m-%d')- datetime(1970, 1, 1)).total_seconds() * 1000) if form_data['date_to'] != '' else '',
						'leaves_start': int(form_data['leaves_start']) if form_data['leaves_start'] != '' else 1,
						'leaves_end': int(form_data['leaves_end']) if form_data['leaves_end'] != ''else 999999
					}
					with get_driver().session() as neo4j_session:
						result = neo4j_session.read_transaction(neo4j_query, Cypher.leaves_get, parameters)
					id_list = [record[0] for record in result]
				else:
					id_list = existing_ids
			if form_data['trait_level'] == 'sample':
				fieldnames = [
					'Country',
					'Region',
					'Farm',
					'Plot',
					'PlotUID',
					'Block',
					'BlockUID',
					'TreeUID',
					'TreeCustomID',
					'Variety',
					'Tissue',
					'Storage',
					'SampleDate',
					'UID'
				]
				parameters = {
					'country': form_data['country'],
					'region': form_data['region'],
					'farm': form_data['farm'],
					'plotID': plotID,
					'trees_start': int(form_data['trees_start']) if form_data['trees_start'] else "",
					'trees_end': int(form_data['trees_end']) if form_data['trees_end'] else "",
					'replicates': int(form_data['replicates']) if form_data['replicates'] else "",
					'tissue': form_data['tissue'],
					'storage': form_data['storage'],
					'start_time': int(
						(datetime.strptime(form_data['date_from'], '%Y-%m-%d') -
						datetime(1970, 1, 1)).total_seconds() * 1000
					) if form_data['date_from'] else "",
					'end_time': int(
						(datetime.strptime(form_data['date_to'], '%Y-%m-%d')
						- datetime(1970, 1, 1)).total_seconds() * 1000
					) if form_data['date_to'] else "",
					'samples_start': int(form_data['samples_start']) if form_data[
					'samples_start'] else "",
					'samples_end': int(form_data['samples_end']) if form_data['samples_end'] else ""
				}
				# build the file and return filename etc.
				id_list = Samples().get_samples(parameters)
		#check we have found matching ID's, if not return None
		if len(id_list) == 0:
			return None
		#if requesting replicates for samples then create these in the id_list
		sample_replicates = int(form_data['sample_replicates']) if form_data['sample_replicates'] else 1
		if sample_replicates > 1:
			fieldnames.insert(13, 'SampleUID')
			id_list_reps = []
			for sample in id_list:
				sample['SampleUID'] = sample['UID']
				for i in range(sample_replicates):
					sample['UID'] = str(sample['SampleUID']) + "." + str(int(i + 1))
					id_list_reps.append(sample.copy())
			id_list = id_list_reps
		#then we get the traits list from the form
		node_label = str(form_data['trait_level']).title() + 'Trait'
		selection = [
			item for sublist in [
				form_data.getlist(i) for i in form_data if all(['csrf_token' not in i, form_data['trait_level'] + '-' in i])
			]
			for item in sublist
		]
		#if there are no selected traits exit here returning None
		if len(selection) == 0:
			return None
		#check that they are in the database
		selected_traits = Lists(node_label).get_selected(selection, 'name')
		#make the table file
		fieldnames += ['Date','Time']
		fieldnames += [trait['name'] for trait in selected_traits]
		table_file = self.make_csv_file(fieldnames, id_list, 'table.csv')
		#and a file that describes the trait details
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
			'table':table_file,
			'details':details_file
		}
