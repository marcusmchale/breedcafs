from app import app, os
from app.cypher import Cypher
from user import User
from lists import Lists
from config import ALLOWED_EXTENSIONS
from neo4j_driver import get_driver
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
		if level == 'sample':
			node_label = 'SampleTrait'
		if level == 'tree':
			node_label = 'TreeTrait'
		if level == 'block':
			node_label = 'BlockTrait'			
		if level == 'plot':
			node_label = 'PlotTrait'
		TRAITS = Lists(node_label).create_list('name')
		#First I limit the data nodes to those submitted by members of an affiliated institute
		user_data_query = ( 
			' MATCH (:User {username: $username}) '
			' -[:AFFILIATED {confirmed :true}] '
			' ->(partner:Partner) '
			' WITH partner '			
			' MATCH (partner)<-[:AFFILIATED {data_shared :true}] '
			' -(user:User) '
			' -[:SUBMITTED*5]->(data:Data) '
			' WITH user.name as user_name, partner.name as partner_name, data' )
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
		if level == 'sample' and set(traits).issubset(set(TRAITS)):
			index_fieldnames = [
				'User',
				'Partner',
				'Country',
				'Region',
				'Farm', 
				'Plot', 
				'Block',
				'PlotID',
				'TreeID',
				'SampleID',
				'UID']
			td = ( ' MATCH (trait:SampleTrait) '
				' WHERE (trait.name) IN ' + str(traits) +
				' WITH user_name, partner_name, data, trait '
				' MATCH (trait) '
					' <-[:FOR_TRAIT]-(:PlotSampleTrait) '
					' <-[:DATA_FOR]-(tst:TreeSampleTrait) '
					' <-[:DATA_FOR]-(sst:SampleSampleTrait) '
					' <-[:DATA_FOR]-(data), '
					' (sst)-[:FROM_SAMPLE]->(sample:Sample), '
					' (tst)-[:FROM_TREE]->(tree:Tree) '
				)
			#if no plot selected
			if plotID == "" :
				tdp = td + ( ' -[:IS_IN]->(PlotTrees) '
					' -[:IS_IN]->(plot:Plot) ' )
				query = tdp + frc 
				optional_block = ( ' OPTIONAL MATCH (tree) '
					' -[:IS_IN]->(:BlockTrees) '
					' -[:IS_IN]->(block:Block) ' )
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
					' -[:IS_IN]->(:PlotBlocks) '
					' -[:IS_IN]->(plot:Plot) ')
				query = tdp + frc
				optional_block = ''
			#and generate the return statement
			if data_format == 'table':
				response = ( 
					#need a with statement to allow order by with COLLECT
					' WITH '
						' sample.uid as UID, '
						' sample.id as SampleID, '
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
						' SampleID : SampleID, '
						' Block : Block, '
						' Plot : Plot, '
						' Farm : Farm, '
						' Region : Region, '
						' Country : Country, '
						' Traits : Traits } '
					' ORDER BY '
						' PlotID, SampleID ' )
			elif data_format == 'db':
				response = (
					' RETURN {'
						' User : user_name, '
						' Partner : partner_name,'
						' Country : country.name, ' 
						' Region : region.name, '
						' Farm : farm.name, '
						' Plot : plot.name, '
						' Block : block.name, '
						' PlotID : plot.uid, '
						' TreeID : tree.id, '
						' SampleID : sample.id, '
						' UID : sample.uid, '
						' Trait : trait.name, '
						' Value : data.value, '
						' Location : data.location, '
						' Recorded_at : data.timeFB, '
						' Recorded_by: data.person '
					' } '
					' ORDER BY plot.uid, sample.id, trait.name, data.timeFB '
					)
		if level == 'tree' and set(traits).issubset(set(TRAITS)):
			index_fieldnames = [
				'User',
				'Partner',
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
				' WITH user_name, partner_name, data, trait '
				' MATCH (trait) '
					' <-[:FOR_TRAIT]-(:PlotTreeTrait) '
					' <-[:DATA_FOR]-(tt:TreeTreeTrait) '
					' <-[:DATA_FOR]-(data), '
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
					' -[:IS_IN]->(:PlotBlocks) '
					' -[:IS_IN]->(plot:Plot) ')
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
						' User : user_name, '
						' Partner : partner_name,'
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
					' } '
					' ORDER BY plot.uid, tree.id, trait.name, data.timeFB '
					)
		elif level == 'block' and set(traits).issubset(set(TRAITS)):
			optional_block = ''
			index_fieldnames = [
				'User',
				'Partner',
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
				' WITH user_name, partner_name, data, trait '
				' MATCH (trait) '
					' <-[:FOR_TRAIT]-(:PlotBlockTrait) '
					' <-[:DATA_FOR]-(bt:BlockBlockTrait) '
					' <-[:DATA_FOR]-(data), ' 
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
						' User : user_name, '
						' Partner : partner_name,'
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
					' } '
					' ORDER BY plot.uid, block.id, trait.name, data.timeFB '
					)
		elif level == 'plot' and set(traits).issubset(set(TRAITS)):
			optional_block = ''
			index_fieldnames = [
				'User',
				'Partner',
				'Country',
				'Region',
				'Farm', 
				'Plot', 
				'UID']
			tdp = ( ' MATCH (trait:PlotTrait) '
				' WHERE (trait.name) IN ' + str(traits) +
				' WITH user_name, partner_name, data, trait '
				' MATCH (trait) '
					' <-[:FOR_TRAIT]-(ppt:PlotPlotTrait) '
					' <-[:DATA_FOR]-(data), ' 
					' (ppt)-[:FROM_PLOT]->(plot:Plot) ' )
			query = tdp + frc
			if data_format == 'table':
				response = ( 
					#need a with statement to allow order by with COLLECT
					' WITH '
						' plot.uid as UID, '
						' plot.name as Plot, '
						' farm.name as Farm, '
						' region.name as Region, '
						' country.name as Country, '
						' COLLECT([trait.name, data.value]) as Traits '
					' RETURN {'
						' UID : UID ,'
						' Plot : Plot, '
						' Farm : Farm, '
						' Region : Region, '
						' Country : Country, '
						' Traits : Traits '
					'}'
					' ORDER BY '
						' PlotID ' )
			elif data_format == 'db':
				response = (
					' RETURN { '
						' User : user_name, '
						' Partner : partner_name,'
						' Country : country.name, ' 
						' Region : region.name, '
						' Farm : farm.name, '
						' Plot : plot.name, '
						' UID : plot.uid, '
						' Trait : trait.name, '
						' Value : data.value, '
						' Location : data.location, '
						' Recorded_at : data.timeFB, '
						' Recorded_by : data.person'
					' } '
					' ORDER BY plot.uid, trait.name, data.timeFB '
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
		query = user_data_query + query + time_condition + optional_block + response
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
			if set(traits).issubset(set(TRAITS)):
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
	def _get_csv(self, tx, query):
		#perform transaction and return the simplified result (remove all the transaction metadata and just keep the result map)
		return [record[0] for record in tx.run(query,
			username = self.username,
			country = self.country,
			region = self.region,
			farm = self.farm,
			plotID = self.plotID,
			blockUID = self.blockUID,
			traits = self.traits,
			start_time = self.start_time,
			end_time = self.end_time)]