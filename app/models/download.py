import os
from app import app
from app.cypher import Cypher
from user import User
from lists import Lists
from config import uri, driver, ALLOWED_EXTENSIONS
from flask import session
import cStringIO
import unicodecsv as csv
from datetime import datetime

#User class related (all uploads are tied to a user) yet more specifically regarding uploads
class Download(User):
	def __init__(self, username):
		self.username=username
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
		traits = [i.encode('utf-8') for i in traits]
		self.traits=traits
		self.start_time = start_time
		self.end_time = end_time
		#build query strings - always be careful not to allow injection when doing this!!
		#all of the input needs to be validated (in this case it is already done by WTForms checking against selectfield options)
		#but as I am not entirely sure about the security of this layer I am adding another check of the values that are used to concatenate the string
		TREETRAITS = Lists('TreeTrait').create_list('name')
		BLOCKTRAITS = Lists('BlockTrait').create_list('name')
		#this section first as not dependent on level but part of query build
		if country == "" :
			frc = (' -[:IS_IN]->(farm:Farm) '
				' -[:IS_IN]->(region:Region) '
				' -[:IS_IN]->(country:Country) ')
		elif country != "" and region == "":
			frc = (' -[:IS_IN]->(farm:Farm) '
				' -[:IS_IN]->(region:Region) '
				' -[:IS_IN]->(country:Country {name:$country}) ')
		elif region != "" and farm == "" :
			frc = (' <-[:IS_IN]->(farm:Farm) '
				' -[:IS_IN]->(region:Region {name:$region}) '
				' -[:IS_IN]->(country:Country {name:$country}) ')
		elif farm != "" :
			frc = (' -[:IS_IN]->(farm:Farm {name:$farm}) '
				' -[:IS_IN]->(region:Region {name:$region}) '
				' -[:IS_IN]->(country:Country {name:$country}) ')
		#then level dependent stuff
		if level == 'tree' and set(traits).issubset(set(TREETRAITS)):
			index_fieldnames = [
				'Country',
				'Region',
				'Farm', 
				'Plot', 
				'Block',
				'PlotID',
				'TreeID',
				'UID']
			td = ( ' MATCH (trait:TreeTrait) '
				' WHERE (trait.name) IN ' + str(traits) +
				' WITH trait '
				' MATCH (trait) '
					' <-[:FOR_TRAIT]-(:PlotTrait) '
					' <-[:FOR_TRAIT]-(tt:TreeTreeTrait), '
					' (tt)<-[:DATA_FOR]-(data:Data), '
					' (tt)-[:FROM_TREE]->(tree:Tree)'
					 )
			#if no plot selected
			if plotID == "" :
				tdp = td + ( ' -[:IS_IN]->(PlotTrees) '
					' -[:IS_IN]->(plot:Plot) ' )
				query = tdp + frc 
				optional_block = ( ' OPTIONAL MATCH (tree) '
					' -[:IS_IN]->(:BlockTrees) '
					' -[:IS_IN]->(block:Block) ')
			#if plotID is defined (but no blockUID)
			elif blockUID == "" and plotID != "" :
				tdp = td + ( ' -[:IS_IN]->(PlotTrees) '
					' -[:IS_IN]->(plot:Plot {uid:$plotID}) ' )
				query = tdp + frc 
				optional_block = ( ' OPTIONAL MATCH (tree) '
					' -[:IS_IN]->(:BlockTrees) '
					' -[:IS_IN]->(block:Block) ')
			#if blockID is defined
			elif blockUID != "":
				tdp = td + ( '-[:IS_IN]->(:BlockTrees) '
					' -[:IS_IN]->(block:Block {uid:$blockUID}) '
					' <-[:IS_IN]-(:PlotBlocks) '
					' <-[:IS_IN]-(plot:Plot) ')
				query = tdp + frc
				optional_block = ''
			#and generate the return statement
			if data_format == 'table':
				response = ( 
					#need a with statement to allow order by with COLLECT
					' WITH '
						' tree.uid as UID, '
						' plot.uid as PlotID, '
						' tree.id as TreeID, '
						' block.name as Block, '
						' plot.name as Plot, '
						' farm.name as Farm, '
						' region.name as Region, '
						' country.name as Country, '
						' COLLECT([trait.name, data.value]) as Traits '
					' RETURN '
						' { UID : UID ,'
						' PlotID : PlotID, '
						' TreeID : TreeID, '
						' Block : Block, '
						' Plot : Plot, '
						' Farm : Farm, '
						' Region : Region, '
						' Country : Country, '
						' Traits : Traits } '
					' ORDER BY '
						' PlotID, TreeID ' )
			elif data_format == 'db':
				response = (
					' RETURN {'
						' Country : country.name, ' 
						' Region : region.name, '
						' Farm : farm.name, '
						' Plot : plot.name, '
						' Block : block.name, '
						' PlotID : plot.uid, '
						' TreeID : tree.id, '
						' UID : tree.uid, '
						' Trait : trait.name, '
						' Value : data.value, '
						' Location : data.location, '
						' Recorded_at : data.timeFB, '
						' Recorded_by: data.person '
					'}'
					)
			#defining these as headers for tree data
		elif level == 'block' and set(traits).issubset(set(BLOCKTRAITS)):
			optional_block = ''
			index_fieldnames = [
				'Country',
				'Region',
				'Farm', 
				'Plot', 
				'PlotID',
				'Block',
				'BlockID',
				'UID']
			td = ( ' MATCH (trait:BlockTrait) '
				' WHERE (trait.name) IN ' + str(traits) +
				' WITH trait '
				' MATCH (trait) '
					' <-[:FOR_TRAIT]-(:PlotTrait) '
					' <-[:FOR_TRAIT]-(bt:BlockBlockTrait), '
					' (bt)<-[:DATA_FOR]-(data:Data), ' 
					' (bt)-[:FROM_BLOCK]->(block:Block ' )
			if blockUID != "" :
				tdp = td + ( ' {uid:$blockUID}) '
					' -[:IS_IN]->(:PlotBlocks) '
					' -[:IS_IN]->(plot:Plot) ' )
			elif blockUID =="" and plotID != "" :
				tdp = td + ( ') '
					' -[:IS_IN]->(:PlotBlocks)'
					' -[:IS_IN]->(plot:Plot {uid:$plotID}) ' )
			elif plotID == "" :
				tdp = td + ( ') '
					' -[:IS_IN]->(:PlotBlocks)'
					' -[:IS_IN]->(plot:Plot) ' )
			query = tdp + frc
			if data_format == 'table':
				response = ( 
					#need a with statement to allow order by with COLLECT
					' WITH '
						' block.uid as UID, '
						' plot.uid as PlotID, '
						' block.id as BlockID, '
						' block.name as Block, '
						' plot.name as Plot, '
						' farm.name as Farm, '
						' region.name as Region, '
						' country.name as Country, '
						' COLLECT([trait.name, data.value]) as Traits '
					' RETURN {'
						' UID : UID ,'
						' PlotID : PlotID, '
						' BlockID : BlockID, '
						' Block : Block, '
						' Plot : Plot, '
						' Farm : Farm, '
						' Region : Region, '
						' Country : Country, '
						' Traits : Traits '
					'}'
					' ORDER BY '
						' PlotID, BlockID ' )
			elif data_format == 'db':
				response = (
					' RETURN { '
						' Country : country.name, ' 
						' Region : region.name, '
						' Farm : farm.name, '
						' Plot : plot.name, '
						' PlotID : plot.uid, '
						' Block : block.name, '
						' BlockID : block.id, '
						' UID : block.uid, '
						' Trait : trait.name, '
						' Value : data.value, '
						' Location : data.location, '
						' Recorded_at : data.timeFB, '
						' Recorded_by : data.person'
					'}'
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
		self.query = query + time_condition + optional_block + response
		with driver.session() as neo4j_session:
			result = neo4j_session.read_transaction(self._get_csv)
		#prepare data and variablesto make file
		if data_format == 'table':
			#expand the traits and values as key:value pairs
			[[record.update({i[0]:i[1] for i in record['Traits']})] for record in result]
				#now pop out the Traits key and add the returned trait names to a set of fieldnames
				#trait_fieldnames = set()
				#[[trait_fieldnames.add(i[0]) for i in record.pop('Traits')] for record in result]
			#replaced the above with using the fieldnames of traits from the form, this will provide a predictable output from a user perspective )
			#will also allow me to do all this as a stream once the files get larger - thanks Tony for this advice
			if set(traits).issubset(set(BLOCKTRAITS)) or set(traits).issubset(set(TREETRAITS)):
				trait_fieldnames = traits
			#now create the csv file from result and fieldnames
			fieldnames =  index_fieldnames + list(trait_fieldnames)
		elif data_format == 'db':
			fieldnames = index_fieldnames + ['Trait', 'Value', 'Location', 'Recorded_at', 'Recorded_by']
		#create the file path
		time = datetime.now().strftime('%Y%m%d-%H%M%S')
		filename = time + '_data.csv'
		file_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], self.username, filename)
		#create user download path if not found
		if not os.path.isdir(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username'])):
			os.mkdir(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username']))
		#make the file
		with open(file_path,'w') as file:
			writer = csv.DictWriter(file, 
				fieldnames=fieldnames, 
				quoting=csv.QUOTE_ALL,
				extrasaction='ignore')
			writer.writeheader()
			for i, item in enumerate(result):
				writer.writerow(item)
			file_size = file.tell()
		return { "filename":filename,
			"file_path":file_path,
			"file_size":file_size }
	def _get_csv(self, tx):
		#perform transaction and return the simplified result (remove all the transaction metadata and just keep the result map)
		return [record[0] for record in tx.run(self.query,
			country = self.country,
			region = self.region,
			farm = self.farm,
			plotID = self.plotID,
			blockUID = self.blockUID,
			traits = self.traits,
			start_time = self.start_time,
			end_time = self.end_time)]