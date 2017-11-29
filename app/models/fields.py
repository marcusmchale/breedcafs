import os
import unicodecsv as csv 
import cStringIO
from app import app
from app.cypher import Cypher
from neo4j_driver import get_driver
from flask import session
from datetime import datetime
from neo4j_driver import get_driver

class Fields:
	def __init__(self, country):
		self.country=country
	def find_country(self):
		with get_driver().session() as neo4j_session:
			return neo4j_session.read_transaction(self._find_country)
	def _find_country (self, tx):
		for record in tx.run(Cypher.country_find, 
			country=self.country):
			return (record['country'])
	def add_country(self):
		with get_driver().session() as neo4j_session:
			neo4j_session.write_transaction(self._add_country)
	def _add_country (self, tx):
		tx.run(Cypher.country_add, 
			country = self.country, 
			username=session['username'])
	def find_region(self, region):
		self.region=region
		with get_driver().session() as neo4j_session:
			return neo4j_session.read_transaction(self._find_region)
	def _find_region (self, tx):
		for record in tx.run(Cypher.region_find, 
			country=self.country, 
			region=self.region):
			return (record['region'])
	def add_region(self, region):
		self.region=region
		with get_driver().session() as neo4j_session:
			neo4j_session.write_transaction(self._add_region)
	def _add_region (self, tx):
		tx.run(Cypher.region_add, 
			country = self.country, 
			region=self.region, 
			username=session['username'])
	def find_farm(self, region, farm):
		self.region=region
		self.farm=farm
		with get_driver().session() as neo4j_session:
			return neo4j_session.read_transaction(self._find_farm)
	def _find_farm (self, tx):
		for record in tx.run(Cypher.farm_find, 
			country=self.country, 
			region=self.region, 
			farm=self.farm):
			return (record['farm'])
	def add_farm(self, region, farm):
		self.region=region
		self.farm=farm
		with get_driver().session() as neo4j_session:
			neo4j_session.write_transaction(self._add_farm)
	def _add_farm (self, tx):
		tx.run(Cypher.farm_add, 
			country = self.country, 
			region=self.region, 
			farm=self.farm, 
			username=session['username'])
	def find_plot(self, region, farm, plot):
		self.region=region
		self.farm=farm
		self.plot=plot
		with get_driver().session() as neo4j_session:
			return neo4j_session.read_transaction(self._find_plot)
	def _find_plot (self, tx):
		for record in tx.run(Cypher.plot_find, 
			country=self.country, 
			region=self.region, 
			farm=self.farm, 
			plot=self.plot):
			return (record['plot'])
	def add_plot(self, region, farm, plot):
		self.region=region
		self.farm=farm
		self.plot=plot
		with get_driver().session() as neo4j_session:
			neo4j_session.write_transaction(self._add_plot)
	def _add_plot (self, tx):
		tx.run(Cypher.plot_id_lock)
		tx.run(Cypher.plot_add, 
			country = self.country, 
			region = self.region, 
			farm = self.farm, 
			plot = self.plot, 
			username = session['username'])
	@staticmethod #has plotID so doesn't need country
	def find_block(plotID, block):
		with get_driver().session() as neo4j_session:
			return neo4j_session.read_transaction(Fields._find_block, plotID, block)
	@staticmethod #has plotID so doesn't need country
	def _find_block(tx, plotID, block):
		for record in tx.run(Cypher.block_find, 
			plotID = plotID,
			block = block):
			return (record['block'])
	@staticmethod #has plotID so doesn't need country
	def add_block(plotID, block):
		with get_driver().session() as neo4j_session:
			neo4j_session.write_transaction(Fields._add_block, plotID, block)
	@staticmethod #has plotID so doesn't need country
	def _add_block (tx, plotID, block):
		tx.run(Cypher.block_id_lock,
			plotID = plotID)
		tx.run(Cypher.block_add, 
			plotID = plotID,
			block = block,
			username = session['username'])
	@staticmethod #has plotID so doesn't need country
	def add_trees(plotID, count, blockUID=None):
		#register trees and return index data
		with get_driver().session() as neo4j_session:
			id_list = neo4j_session.write_transaction(Fields._add_trees, plotID, count, blockUID)
		#create user download path if not found
		if not os.path.isdir(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username'])):
			os.mkdir(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username']))
		#prepare variables to write the file
		fieldnames = ['UID','PlotID','TreeID', 'Block', 'Plot', 'Farm', 'Region', 'Country']
		time = datetime.now().strftime('%Y%m%d-%H%M%S')
		first_tree_id = id_list[0]['TreeID']
		last_tree_id = id_list[-1]['TreeID']
		filename = time + '_plot_' + str(plotID) + '_T' + str(first_tree_id) + '_to_T' + str(last_tree_id) + '.csv'
		file_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username'], filename)
		#make the file
		with open (file_path, 'w') as file:
			writer = csv.DictWriter(file,
				fieldnames=fieldnames,
				quoting=csv.QUOTE_ALL,
				extrasaction='ignore')
			writer.writeheader()
			for row in id_list:
				writer.writerow(row)
			file_size = file.tell()
		return { "filename":filename, 
			"file_path":file_path, 
			"file_size":file_size, 
			"first_tree_id":first_tree_id, 
			"last_tree_id":last_tree_id }
	@staticmethod #has plotID so doesn't need country
	def _add_trees(tx, plotID, count, blockUID):
		tx.run(Cypher.tree_id_lock,
			plotID = plotID)
		if blockUID == None:
			result = tx.run(Cypher.trees_add, 
				plotID = plotID,
				count = count,
				username = session['username'])
		else:
			result=tx.run(Cypher.block_trees_add, 
				plotID = plotID,
				count = count,
				blockUID = blockUID,
				username = session['username'])
		return [record[0] for record in result]
	@staticmethod #has plotID so doesn't need country
	def get_trees(plotID, start, end):
		with get_driver().session() as neo4j_session:
			id_list = neo4j_session.read_transaction(Fields._get_trees, plotID, start, end)
		#check if any data found, if not return none
		if len(id_list) == 0:
			return None
		#create user download path if not found
		if not os.path.isdir(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username'])):
			os.mkdir(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username']))
		#prepare variables to write file
		fieldnames = ['UID','PlotID','TreeID', 'TreeName', 'Block', 'Plot', 'Farm', 'Region', 'Country']
		time = datetime.now().strftime('%Y%m%d-%H%M%S')
		first_tree_id = id_list[0]['TreeID']
		last_tree_id = id_list[-1]['TreeID']
		filename = time + '_plot_' + str(plotID) + '_T' + str(first_tree_id) + '_to_T' + str(last_tree_id) + '.csv'
		file_path =  os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username'], filename)
		with open (file_path, 'w') as file:
			writer = csv.DictWriter(file,
				fieldnames=fieldnames,
				quoting=csv.QUOTE_ALL,
				extrasaction='ignore')
			writer.writeheader()
			for row in id_list:
				writer.writerow(row)
			file_size = file.tell()
		return { "filename":filename,
			"file_path":file_path,
			"file_size":file_size,
			"first_tree_id":first_tree_id,
			"last_tree_id":last_tree_id }
	@staticmethod #has plotID so doesn't need country
	def _get_trees(tx, plotID, start, end):
		result = tx.run(Cypher.trees_get, 
			plotID = plotID,
			start = start,
			end = end)
		return [record[0] for record in result]
	def get_farms(self, region):
		self.region=region
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(self._get_farms)
			return [(record[0]['name']) for record in result]
	def _get_farms(self, tx):
		return tx.run(Cypher.get_farms, 
			country=self.country, 
			region=self.region)
	def get_plotIDs(self, region, farm):
		self.region=region
		self.farm=farm
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(self._get_plots)
			return [(str(record[0]['uid'])) for record in result]
	def get_plots_tup(self, region, farm):
		self.region=region
		self.farm=farm
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(self._get_plots)
			return [(str(record[0]['uid']), record[0]['name']) for record in result]
	def _get_plots(self, tx):
		return tx.run(Cypher.get_plots, 
			country=self.country, 
			region=self.region, 
			farm=self.farm)
	#this may not have country specified so is class method
	@staticmethod
	def make_plots_csv(username, query, parameters):
		#get the block index data (country, region etc.)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(Fields._get_plots_csv, query, parameters)
			id_list = [record[0] for record in result]
		#check if any data found, if not return none
		if len(id_list) == 0:
			return None
		#create user download path if not found
		if not os.path.isdir(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], username)):
			os.mkdir(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], username))
		#prepare the file
		fieldnames = ['UID', 'Plot', 'Farm', 'Region', 'Country']
		time = datetime.now().strftime('%Y%m%d-%H%M%S')
		if 'farm' in parameters:
			filename = time + '_' + parameters['farm'] + '_plots.csv'
		elif 'region' in parameters:
			filename = time + '_' + parameters['region'] + '_plots.csv'
		elif 'country' in parameters:
			filename = time + '_' + parameters['country'] + '_plots.csv'
		else:
			filename = time + '_plots.csv'
		file_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], username, filename)
		with open (file_path, 'w') as file:
			writer = csv.DictWriter(file,
				fieldnames=fieldnames,
				quoting=csv.QUOTE_ALL,
				extrasaction='ignore')
			writer.writeheader()
			for row in id_list:
				writer.writerow(row)
			file_size = file.tell()
		return {"filename":filename, "file_path":file_path, "file_size":file_size}
	@staticmethod
	def _get_plots_csv(tx, query, parameters):
		return tx.run(query, parameters)
	@staticmethod #has plotID so doesn't need country
	def get_blockUIDs(plotID):
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(Fields._get_blocks, plotID)
			return [(record[0]['uid']) for record in result]
	@staticmethod #has plotID so doesn't need country
	def get_blocks_tup(plotID):
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(Fields._get_blocks, plotID)
			return [(record[0]['uid'], record[0]['name']) for record in result]
	@staticmethod #has plotID so doesn't need country
	def _get_blocks(tx, plotID):
		return tx.run(Cypher.get_blocks,
			plotID = plotID)
	@staticmethod #has plotID so doesn't need country
	def make_blocks_csv(username, plotID):
		#get the block index data (country, region etc.)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(Fields._get_blocks_csv, plotID)
			id_list = [record[0] for record in result]
		#check if any data found, if not return none
		if len(id_list) == 0:
			return None
		#create user download path if not found
		if not os.path.isdir(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], username)):
			os.mkdir(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], username))
		#prepare the file
		fieldnames = ['UID', 'PlotID', 'BlockID', 'Block', 'Plot', 'Farm', 'Region', 'Country']
		time = datetime.now().strftime('%Y%m%d-%H%M%S')
		filename = time + '_plot_' + str(plotID) + '_blocks.csv'
		file_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], username, filename)
		with open (file_path, 'w') as file:
			writer = csv.DictWriter(file,
				fieldnames=fieldnames,
				quoting=csv.QUOTE_ALL,
				extrasaction='ignore')
			writer.writeheader()
			for row in id_list:
				writer.writerow(row)
			file_size = file.tell()
		return {"filename":filename, "file_path":file_path, "file_size":file_size}
	@staticmethod #has plotID so doesn't need country
	def _get_blocks_csv(tx, plotID):
		return tx.run(Cypher.get_blocks_csv,
			plotID = plotID)
		