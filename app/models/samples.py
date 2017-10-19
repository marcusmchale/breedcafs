import os
import unicodecsv as csv 
import cStringIO
from app import app
from app.cypher import Cypher
from config import uri, driver
from flask import session
from datetime import datetime

class Samples:
	def __init__ (self):
		pass
	def add_tissue(self, tissue):
		self.tissue = tissue
		with driver.session() as session:
			session.write_transaction(self._add_tissue)
	def _add_tissue(self, tx):
		tx.run(Cypher.tissue_add,
			tissue = self.tissue,
			username = session['username'])
# should really reduce these calls down to a common function, lots of repitition here!!
	def add_storage(self, storage):
		self.storage = storage
		with driver.session() as session:
			session.write_transaction(self._add_storage)
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
		with driver.session() as neo4j_session:
			neo4j_session.write_transaction(self._add_samples)
		#create user download path if not found
		if not os.path.isdir(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username'])):
			os.mkdir(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username']))
		#prepare variables to write the file
		fieldnames= ['UID', 'PlotID', 'TreeID', 'TreeName', 'SampleID', 'Date', 'Tissue', 'Storage', 'Plot', 'Farm', 'Region', 'Country']
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
