from app import app, os
from app.cypher import Cypher
from user import User
from lists import Lists
from samples import Samples
from config import ALLOWED_EXTENSIONS
from neo4j_driver import (
	get_driver,
	neo4j_query
)
from flask import (
	session,
	url_for
)
import cStringIO
import unicodecsv as csv
from datetime import datetime
from app.models import (
	Fields)

#User class related (all uploads are tied to a user) yet more specifically regarding uploads
class Download(User):
	def __init__(self, username):
		self.username = username
	def make_csv_file(self, fieldnames, id_list, filename):
		#create user download path if not found
		if not os.path.isdir(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], self.username)):
			os.mkdir(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], self.username))
		#prepare variables to write the file
		time = datetime.now().strftime('%Y%m%d-%H%M%S')
		filename = time + '_' + filename
		file_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username'], filename)
		#make the file
		with open(file_path, 'w') as file:
			writer = csv.DictWriter(file,
				fieldnames = fieldnames,
				quoting = csv.QUOTE_ALL,
				extrasaction = 'ignore')
			writer.writeheader()
			for row in id_list:
				to_title_case = set(fieldnames).intersection([
					'Variety',
					'Tissue',
					'Storage',
					'Block',
					'Plot',
					'Farm',
					'Region',
					'Country'])
				for item in to_title_case:
					row[item] = str(row[item]).title() if row[item] else None
				writer.writerow(row)
			file_size = file.tell()
		#return file details
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
		traits = [i.encode('utf-8') for i in traits]
		self.traits=traits
		self.start_time = start_time
		self.end_time = end_time
		#build query strings - always be careful not to allow injection when doing this!!
		#all of the input needs to be validated (in this case it is already done by WTForms checking against selectfield options)
		#but as I am not entirely sure about the security of this layer I am adding another check of the values that are used to concatenate the string
		node_label = str(level).title() + 'Trait'
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
						' Recorded_at : apoc.date.format(data.time), '
						' Recorded_by: data.person '
					' } '
					' ORDER BY plot.uid, sample.id, trait.name, apoc.date.format(data.time) '
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
						' Recorded_at : apoc.date.format(data.time), '
						' Recorded_by: data.person '
					' } '
					' ORDER BY plot.uid, tree.id, trait.name, apoc.date.format(data.time) '
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
						' Recorded_at : apoc.date.format(data.time), '
						' Recorded_by : data.person'
					' } '
					' ORDER BY plot.uid, block.id, trait.name, apoc.date.format(data.time) '
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
						' Recorded_at : apoc.date.format(data.time), '
						' Recorded_by : data.person'
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
				to_title_case = set(fieldnames).intersection(
					['Variety', 'Tissue', 'Storage', 'Block', 'Plot', 'Farm', 'Region', 'Country'])
				for item in to_title_case:
					row[item] = str(row[item]).title() if row[item] else None
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
	def get_index_csv(
			self,
			form_data,
			existing_ids = None
	):
		if form_data['trait_level'] == 'plot':
			parameters = {}
			query = 'MATCH (c:Country '
			if form_data['country'] != '':
				query += '{name: $country}'
				parameters['country'] = form_data['country']
			query += ')<-[:IS_IN]-(r:Region '
			if form_data['region'] != '':
				query += '{name: $region}'
				parameters['region'] = form_data['region']
			query += ')<-[:IS_IN]-(f:Farm '
			if form_data['farm'] != '':
				query += '{name: $farm}'
				parameters['farm'] = form_data['farm']
			query += (
				' )<-[IS_IN]-(p:Plot) '
				' RETURN {'
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
				'UID',
				'Country',
				'Region',
				'Farm',
				'Plot'
			]
			if 'farm' in parameters:
				filename = parameters['farm'] + '_'
			elif 'region' in parameters:
				filename = parameters['region'] + '_'
			elif 'country' in parameters:
				filename = parameters['country'] + '_'
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
				fieldnames = ['UID', 'PlotID', 'BlockID', 'Block', 'Plot', 'Farm', 'Region', 'Country']
				id_list = Fields.get_blocks(plotID)
				if len(id_list) == 0:
					return None
				csv_file_details = self.make_csv_file(fieldnames, id_list, 'blockIDs.csv')
			elif form_data['trait_level'] == 'tree':
				trees_start = int(form_data['trees_start'] if form_data['trees_start'] else 0)
				trees_end = int(form_data['trees_end'] if form_data['trees_end'] else 999999)
				# make the file and return filename, path, size
				fieldnames = [
					'UID',
					'Country',
					'Region',
					'Farm',
					'Plot',
					'PlotID',
					'Block',
					'BlockID',
					'TreeName',
					'TreeID',
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
				fieldnames = ['UID', 'Country', 'Region', 'Farm', 'Plot', 'PlotID', 'Block', 'BlockID', 'TreeName', 'TreeID', 'BranchID']
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
					csv_file_details = self.make_csv_file(
						fieldnames,
						existing_ids,
						'branchIDs.csv'
					)
			elif form_data['trait_level'] == 'leaf':
				fieldnames = ['UID', 'Country', 'Region', 'Farm', 'Plot', 'PlotID', 'Block', 'BlockID', 'TreeName', 'TreeID', 'BranchID', 'LeafID']
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
					csv_file_details = self.make_csv_file(
						fieldnames,
						existing_ids,
						'leafIDs.csv'
					)
			elif form_data['trait_level'] == 'sample':
				parameters = {
					'country': form_data['country'],
					'region': form_data['region'],
					'farm': form_data['farm'],
					'plotID': int(plotID),
					'trees_start': int(form_data['trees_start']) if form_data['trees_start'] else '',
					'trees_end': int(form_data['trees_end']) if form_data['trees_end'] else '',
					'tissue': form_data['tissue'],
					'storage': form_data['storage'],
					'start_time': int((datetime.strptime(form_data['date_from'], '%Y-%m-%d') -	datetime(1970, 1, 1)).total_seconds() * 1000) if form_data['date_from'] != '' else '',
					'end_time': int((datetime.strptime(form_data['date_to'], '%Y-%m-%d')- datetime(1970, 1, 1)).total_seconds() * 1000) if form_data['date_to'] != '' else '',
					'samples_start': int(form_data['samples_start']) if form_data['samples_start'] else "",
					'samples_end': int(form_data['samples_end']) if form_data['samples_end'] else ""
				}
				# build the file and return filename etc.
				fieldnames = [
					'UID',
					'Country',
					'Region',
					'Farm',
					'Plot',
					'PlotID',
					'Block',
					'BlockID',
					'TreeName',
					'TreeID',
					'BranchID',
					'LeafID',
					'SampleID',
					'Tissue',
					'Storage',
					'Date'
				]
				id_list = Samples().get_samples(parameters)
				if len(id_list) == 0:
					return None
				sample_replicates = int(form_data['sample_replicates']) if form_data['sample_replicates'] else 1
				if sample_replicates != 1:
					fieldnames.insert(11, 'Replicate')
					id_list_reps = []
					for sample in id_list:
						for i in range(sample_replicates):
							sample['UID'] = str(sample['PlotID']) + "_S" + str(sample['SampleID']) + "." + str(int(i + 1))
							sample['Replicate'] = i + 1
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
		if not os.path.isdir(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], self.username)):
			os.mkdir(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], self.username))
		#set variables for file creation
		fieldnames = ['name',
			'format',
			'defaultValue',
			'minimum',
			'maximum',
			'details',
			'categories',
			'isVisible',
			'realPosition']
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
				query += '{name: $country}'
				parameters['country'] = form_data['country']
			query += ')<-[:IS_IN]-(r:Region '
			if form_data['region'] != '':
				query += '{name: $region}'
				parameters['region'] = form_data['region']
			query += ')<-[:IS_IN]-(f:Farm '
			if form_data['farm'] != '':
				query += '{name: $farm}'
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
				'UID',
				'Country',
				'Region',
				'Farm',
				'Plot'
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
					'UID',
					'Country',
					'Region',
					'Farm',
					'Plot',
					'PlotID',
					'Block',
					'BlockID'
				]
				id_list = Fields.get_blocks(plotID)
			if form_data['trait_level'] == 'tree':
				trees_start = int(form_data['trees_start'] if form_data['trees_start'] else 0)
				trees_end = int(form_data['trees_end'] if form_data['trees_end'] else 999999)
				# make the file and return filename, path, size
				fieldnames = [
					'UID',
					'Country',
					'Region',
					'Farm',
					'Plot',
					'PlotID',
					'Block',
					'BlockID',
					'TreeName',
					'TreeID'
				]
				id_list = Fields.get_trees(plotID, trees_start, trees_end)
			if form_data['trait_level'] == 'branch':
				fieldnames = [
					'UID',
					'Country',
					'Region',
					'Farm',
					'Plot',
					'PlotID',
					'Block',
					'BlockID',
					'TreeName',
					'TreeID',
					'BranchID'
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
					'UID',
					'Country',
					'Region',
					'Farm',
					'Plot',
					'PlotID',
					'Block',
					'BlockID',
					'TreeName',
					'TreeID',
					'BranchID',
					'LeafID'
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
				fieldnames = [
					'UID',
					'Country',
					'Region',
					'Farm',
					'Plot',
					'PlotID',
					'Block',
					'BlockID',
					'TreeName',
					'TreeID',
					'SampleID',
					'Date',
					'Tissue',
					'Storage',
					]
				id_list = Samples().get_samples(parameters)
		#check we have found matching ID's, if not return None
		if len(id_list) == 0:
			return None
		#if requesting replicates for samples then create these in the id_list
		sample_replicates = int(form_data['sample_replicates']) if form_data['sample_replicates'] else 1
		if sample_replicates != 1:
			fieldnames.insert(11, 'Replicate')
			id_list_reps = []
			for sample in id_list:
				for i in range(sample_replicates):
					sample['UID'] = str(sample['PlotID']) + "_S" + str(sample['SampleID']) + "." + str(int(i + 1))
					sample['Replicate'] = i + 1
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
		fieldnames += [trait['name'] for trait in selected_traits]
		table_file = self.make_csv_file(fieldnames, id_list, 'table.csv')
		#and a file that describes the trait details
		trait_fieldnames = [
			'name',
			'format',
			'minimum',
			'maximum',
			'details',
			'categories'
		]
		details_file = self.make_csv_file(trait_fieldnames, selected_traits, 'details.csv')
		return {
			'table':table_file,
			'details':details_file
		}
