import os
import unicodecsv as csv 
import cStringIO
from app import app
from app.cypher import Cypher
from neo4j_driver import get_driver
from flask import session
from datetime import datetime

class Samples:
	def __init__ (self):
		pass
	def add_tissue(self, tissue):
		self.tissue = tissue
		with get_driver().session() as neo4j_session:
			neo4j_session.write_transaction(self._add_tissue)
	def _add_tissue(self, tx):
		tx.run(Cypher.tissue_add,
			tissue = self.tissue,
			username = session['username'])
# should really reduce these calls down to a common function, lots of repitition here!!
	def add_storage(self, storage):
		self.storage = storage
		with get_driver().session() as neo4j_session:
			neo4j_session.write_transaction(self._add_storage)
	def _add_storage(self, tx):
		tx.run(Cypher.storage_add,
			storage = self.storage,
			username = session['username'])
	def add_samples(self, plotID, start, end, replicates, tissue, storage, date):
		self.plotID = plotID
		self.start = start
		self.end = end
		self.replicates = replicates
		self.tissue = tissue
		self.storage = storage
		self.date = date
		#register samples and return index data
		with get_driver().session() as neo4j_session:
			neo4j_session.write_transaction(self._add_samples)
		#create user download path if not found
		if not os.path.isdir(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username'])):
			os.mkdir(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username']))
		#prepare variables to write the file
		fieldnames= ['UID', 'PlotID', 'TreeID', 'TreeName', 'SampleID', 'Date', 'Tissue', 'Storage', 'Block', 'Plot', 'Farm', 'Region', 'Country']
		time = datetime.now().strftime('%Y%m%d-%H%M%S')
		first_sample_id = self.id_list[0]['SampleID']
		last_sample_id = self.id_list[-1]['SampleID']
		filename = time + '_plot_' + str(plotID) + '_S' + str(first_sample_id) + '_to_S' + str(last_sample_id) + '.csv'
		file_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username'], filename)
		#make the file
		with open(file_path, 'w') as file:
			writer = csv.DictWriter(file,
				fieldnames=fieldnames,
				quoting=csv.QUOTE_ALL,
				extrasaction='ignore')
			writer.writeheader()
			for row in self.id_list:
				writer.writerow(row)
			file_size = file.tell()
		#return file details
		return { "filename":filename, 
			"file_path":file_path, 
			"file_size":file_size, 
			"first_sample_id":first_sample_id, 
			"last_sample_id":last_sample_id }
	def _add_samples(self, tx):
		tx.run(Cypher.sample_id_lock,
			plotID = self.plotID)
		result=tx.run(Cypher.samples_add,
			plotID = self.plotID,
			start = self.start,
			end = self.end,
			replicates = self.replicates,
			tissue = self.tissue,
			storage = self.storage,
			date = self.date,
			#convert the date to epoch time (ms)
			time = (datetime.strptime(self.date, '%Y-%m-%d')-datetime(1970,1,1)).total_seconds()*1000,
			username = session['username'])
		self.id_list = [record[0] for record in result]
		#setting default values for args
	def get_samples(self, 
			country, 
			region, 
			farm,
			plotID, 
			trees_start, 
			trees_end,
			replicates, 
			tissue, 
			storage, 
			start_time,
			end_time,
			samples_start,
			samples_end):
		self.country = country
		self.region = region
		self.farm = farm
		self.plotID = plotID
		self.trees_start = trees_start
		self.trees_end = trees_end
		self.replicates = replicates
		self.tissue = tissue
		self.storage = storage
		self.start_time = start_time
		self.end_time = end_time
		self.samples_start = samples_start
		self.samples_end = samples_end
		#build the query
		#match samples by location in the graph
		q = ' MATCH (country:Country '
		if country:
			q = q + ' {name:$country} '
		q = q + ')<-[:IS_IN]-(region:Region '
		if region:
			q = q + ' {name:$region} '
		q = q + ')<-[:IS_IN]-(farm:Farm '
		if farm:
			q = q + '{name:$farm} '
		q = q + ')<-[:IS_IN]-(plot:Plot '
		if plotID:
			q = q + ' {uid:$plotID}'
		q = (q + ')<-[:FROM_PLOT]-(:PlotSamples) '
			+ ' <-[:FROM_PLOT]-(pts:PlotTissueStorage) '
			+ ' -[:COLLECTED_AS]->(TiSt:TissueStorage) '
			+ ' -[:OF_TISSUE]->(tissue:Tissue ')
		# and tissue 
		if tissue:
			q = q + ' {name:$tissue}'
		# and storage
		q = q + '), (TiSt)-[:STORED_IN]-(storage:Storage '
		if storage:
			q = q + ' {name:$storage} '
		#and find the trees from these samples
		q = (q + '), (sample)-[:FROM_TREE]->(:TreeSamples) '
			+ ' -[:FROM_TREE]->(tree:Tree) ')
		#now parse out ranges of values provided, first from trees if provided a range, then from samples 
		#not sure if there is an order to processing of where statments..but would be better to do trees first anyway i guess
		if any ([trees_start, trees_end, start_time, end_time, samples_start, samples_end, replicates]):
			q = q + ' WHERE '
			if trees_start:
				q = q + ' tree.id >= $trees_start '
				if any ([trees_end, start_time, end_time, samples_start, samples_end, replicates]):
					q = q + ' AND '
			if trees_end:
				q = q + ' tree.id <= $trees_end '
				if any ([start_time, end_time, samples_start, samples_end, replicates]):
					q = q + ' AND '
			if start_time:
				q = q + ' sample.time >= $start_time ' 
				if any ([end_time, samples_start, samples_end, replicates]):
					q = q + ' AND '
			if end_time:
				q = q + ' sample.time <= $end_time '
				if any ([samples_start, samples_end, replicates]):
					q = q + ' AND '
			if samples_start:
				q = q + ' sample.id >= $samples_start '
				if any ([samples_end, replicates]):
					q = q + ' AND '
			if samples_end:
				q = q + ' sample.id <= $samples_end '
				if replicates:
					q = q + ' AND '
			if replicates:
				q = q + ' sample.replicates >= $replicates'
		#get tree name
		q = (q + ' OPTIONAL MATCH (tree)'
				' <-[:FROM_TREE]-(treename:TreeTreeTrait)'
				' -[:FOR_TRAIT]->(:PlotTrait) '
				' -[:FOR_TRAIT]->(:TreeTrait {name:"name"}) '
			' OPTIONAL MATCH (treename) '
				' <-[:DATA_FOR]-(d:Data) ' )
		# get block name
		q = (q + ' OPTIONAL MATCH (tree) '
			' -[:IS_IN]->(:BlockTrees) '
			' -[:IS_IN]->(block:Block) ')
		# build the return statement
		q = q + (' RETURN {UID: sample.uid, '
			' PlotID : plot.uid, '
			' TreeID : tree.id, '
			' TreeName : d.value, '
			' SampleID : sample.id, '
			' Date : sample.date, '
			' Tissue : tissue.name, '
			' Storage : storage.name, '
			' Block : block.name, '
			' Plot : plot.name, '
			' Farm : farm.name, '
			' Region : region.name, '
			' Country : country.name} ')
		#and order by sample id
		q = q + ' ORDER BY sample.id'
		#register samples and return index data
		self.query = q
		with get_driver().session() as neo4j_session:
			neo4j_session.read_transaction(self._get_samples)
		if len(self.id_list) == 0:
			return None
		#create user download path if not found
		if not os.path.isdir(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username'])):
			os.mkdir(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username']))
		#prepare variables to write the file
		fieldnames= ['UID', 'PlotID', 'TreeID', 'TreeName', 'SampleID', 'Date', 'Tissue', 'Storage', 'Plot', 'Farm', 'Region', 'Country']
		time = datetime.now().strftime('%Y%m%d-%H%M%S')
		filename = time + '_custom_samples.csv'
		file_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username'], filename)
		#make the file
		with open(file_path, 'w') as file:
			writer = csv.DictWriter(file,
				fieldnames=fieldnames,
				quoting=csv.QUOTE_ALL,
				extrasaction='ignore')
			writer.writeheader()
			for row in self.id_list:
				writer.writerow(row)
			file_size = file.tell()
		#return file details
		return { "filename":filename, 
			"file_path":file_path, 
			"file_size":file_size }
	def _get_samples(self, tx):
		result = tx.run(self.query,
			country = self.country,
			region = self.region,
			farm = self.farm,
			plotID = self.plotID,
			trees_start = self.trees_start,
			trees_end = self.trees_end,
			samples_start = self.samples_start,
			samples_end = self.samples_end,
			replicates = self.replicates,
			start_time = self.start_time,
			end_time = self.end_time,
			tissue = self.tissue,
			storage = self.storage)
		self.id_list = [record[0] for record in result]

