import os, grp
import unicodecsv as csv 
from app import app
from app.cypher import Cypher
from neo4j_driver import get_driver, neo4j_query
from flask import session
from datetime import datetime

class Samples:
	def __init__ (self):
		pass
	def add_tissue(self, tissue):
		parameters = {
			'tissue': tissue,
			'username': session['username']
		}
		with get_driver().session() as neo4j_session:
			result = neo4j_session.write_transaction(neo4j_query, Cypher.tissue_add, parameters)
			return [record[0] for record in result]
	def add_storage(self, storage):
		parameters = {
			'storage': storage,
			'username': session['username']
		}
		with get_driver().session() as neo4j_session:
			result = neo4j_session.write_transaction(neo4j_query, Cypher.storage_add, parameters)
			return [record[0] for record in result]
	def add_samples(self, plotID, start, end, replicates, tissue, storage, date, get_file):
		with get_driver().session() as neo4j_session:
			lock_parameters = {
				'plotID': plotID,
				'level': 'sample'
			}
			neo4j_session.write_transaction(neo4j_query, Cypher.id_lock, lock_parameters)
			add_parameters = {
				'plotID': plotID,
				'start': start,
				'end': end,
				'replicates': replicates,
				'tissue': tissue,
				'storage': storage,
				'date': date,
				'time': (datetime.strptime(date, '%Y-%m-%d') - datetime(1970, 1, 1)).total_seconds() * 1000,
				'username': session['username']
			}
			result = neo4j_session.write_transaction(neo4j_query, Cypher.samples_add, add_parameters)
			id_list = [record[0] for record in result]
		if len(id_list) == 0:
			return { "error" : "No sample codes generated. Please check the selected trees are registered"}
		if get_file == True:
			#create user download path if not found
			download_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username'])
			if not os.path.isdir(download_path):
				os.mkdir(download_path)
				gid = grp.getgrnam(app.config('celery_group_name')).gr_gid
				os.chown(download_path, -1, gid)
				os.chmod(download_path, 0775)
			#prepare variables to write the file
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
				'SampleUID'
			]
			first_sample_id = id_list[0]['SampleID']
			last_sample_id = id_list[-1]['SampleID']
			filename = 'plot_' + str(plotID) + '_S' + str(first_sample_id) + '_to_S' + str(last_sample_id) + '.csv'
			file_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username'], filename)
			#make the file
			with open(file_path, 'w') as file:
				writer = csv.DictWriter(file,
					fieldnames=fieldnames,
					quoting=csv.QUOTE_ALL,
					extrasaction='ignore')
				writer.writeheader()
				for row in id_list:
					writer.writerow(row)
				file_size = file.tell()
			#return file details
			return {
				"filename": filename,
				"file_path": file_path,
				"file_size": file_size,
				"first_sample_id": first_sample_id,
				"last_sample_id": last_sample_id
			}
		else:
			return id_list
	def get_samples(self, parameters):
		#build the query
		#match samples by location in the graph
		q = ' MATCH (country:Country '
		if parameters['country']:
			q = q + ' {name_lower:toLower($country)} '
		q = q + ')<-[:IS_IN]-(region:Region '
		if parameters['region']:
			q = q + ' {name_lower:toLower($region)} '
		q = q + ')<-[:IS_IN]-(farm:Farm '
		if parameters['farm']:
			q = q + '{name_lower:toLower($farm)} '
		q = q + ')<-[:IS_IN]-(plot:Plot '
		if parameters['plotID']:
			q = q + ' {uid:$plotID}'
		q = (q + ')<-[:FROM_PLOT]-(:PlotSamples) '
			+ ' <-[:FROM_PLOT]-(pts:PlotTissueStorage) '
			+ ' <-[:COLLECTED_AS]-(sample),')
		#collect the Tissue/Storage container
		q = (q + ' (pts)-[:COLLECTED_AS]->(TiSt:TissueStorage) '
			+ ' -[:OF_TISSUE]->(tissue:Tissue ')
		# and if tissue specified filter by tissue
		if parameters['tissue']:
			q = q + ' {name_lower:toLower($tissue)}'
		# and same for storage
		q = q + '), (TiSt)-[:STORED_IN]-(storage:Storage '
		if parameters['storage']:
			q = q + ' {name_lower:toLower($storage)} '
		# find the tree from each samples
		q = (q + '), (sample)-[:FROM_TREE]->(:TreeSamples) '
			+ ' -[:FROM_TREE]->(tree:Tree) ')
		#now parse out ranges of values provided, first from trees if provided a range, then from samples 
		#not sure if there is an order to processing of where statements..but would be better to do trees first anyway i guess
		if any(
			[parameters[key] != '' for key in [
				'trees_start',
				'trees_end',
				'start_time',
				'end_time',
				'samples_start',
				'samples_end',
				'replicates']
			]
		):
			q = q + ' WHERE '
			filters_list = []
			filters_list.append('tree.id >= $trees_start') if parameters['trees_start'] != '' else None
			filters_list.append('tree.id <= $trees_end') if parameters['trees_end'] != '' else None
			filters_list.append('sample.time >= $start_time') if parameters['start_time'] != '' else None
			filters_list.append('sample.time <= $end_time') if parameters['end_time'] != '' else None
			filters_list.append('sample.id >= $samples_start') if parameters['samples_start'] != '' else None
			filters_list.append('sample.id <= $samples_end') if parameters['samples_end'] != '' else None
			filters_list.append('sample.replicates >= $replicates') if parameters['replicates'] != '' else None
			for i, filter in enumerate(filters_list):
				if i != 0:
					q = q + ' AND '
				q = q + filter
		# get block name
		q = (q + ' OPTIONAL MATCH (tree) '
			' -[:IS_IN {current: True}]->(:BlockTrees) '
			' -[:IS_IN]->(block:Block) ')
		# get branch ID
		q = (q + ' OPTIONAL MATCH (sample) '
			 	' -[:FROM_BRANCH {current : True}]->(branch:Branch) ')
		 # get leaf ID
		q = (q + ' OPTIONAL MATCH (sample) ' 
			 ' -[:FROM_LEAF {current: True}]->(leaf:Leaf) ')
		# build the return statement
		q = q + (
			' RETURN { '
				' Country : country.name, '
				' Region : region.name, '
				' Farm : farm.name, '
				' Plot : plot.name, '
				' PlotUID : plot.uid, '
				' Block : block.name, '
				' BlockUID : block.uid, '
				' TreeUID : tree.uid, '
				' TreeCustomID : tree.custom_id, '
				' Variety: tree.variety, '
				' BranchUID : branch.uid, '
				' LeafUID : leaf.uid, '
				' Tissue : tissue.name, '
				' Storage : storage.name, '
				' SampleDate : sample.date, '
				' UID: sample.uid, '
				' SampleID : sample.id '
			' } '
		)
		#and order by sample id
		query = q + ' ORDER BY sample.id'
		#return index data
		with get_driver().session() as neo4j_session:
			id_list = [record[0] for record in neo4j_session.read_transaction(neo4j_query, query, parameters)]
		#check if any data found, if not return none
		return id_list