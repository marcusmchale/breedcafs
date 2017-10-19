import os
import unicodecsv as csv 
import cStringIO
from app import app
from app.cypher import Cypher
from config import uri, driver
from flask import session
from datetime import datetime

class Fields:
	def __init__(self, country):
		self.country=country
	def find_country(self):
		with driver.session() as session:
			return session.read_transaction(self._find_country)
	def _find_country (self, tx):
		for record in tx.run(Cypher.country_find, 
			country=self.country):
			return (record['country'])
	def add_country(self):
		with driver.session() as session:
			session.write_transaction(self._add_country)
	def _add_country (self, tx):
		tx.run(Cypher.country_add, 
			country = self.country, 
			username=session['username'])
	def find_region(self, region):
		self.region=region
		with driver.session() as session:
			return session.read_transaction(self._find_region)
	def _find_region (self, tx):
		for record in tx.run(Cypher.region_find, 
			country=self.country, 
			region=self.region):
			return (record['region'])
	def add_region(self, region):
		self.region=region
		with driver.session() as session:
			session.write_transaction(self._add_region)
	def _add_region (self, tx):
		tx.run(Cypher.region_add, 
			country = self.country, 
			region=self.region, 
			username=session['username'])
	def find_farm(self, region, farm):
		self.region=region
		self.farm=farm
		with driver.session() as session:
			return session.read_transaction(self._find_farm)
	def _find_farm (self, tx):
		for record in tx.run(Cypher.farm_find, 
			country=self.country, 
			region=self.region, 
			farm=self.farm):
			return (record['farm'])
	def add_farm(self, region, farm):
		self.region=region
		self.farm=farm
		with driver.session() as session:
			session.write_transaction(self._add_farm)
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
		with driver.session() as session:
			return session.read_transaction(self._find_plot)
	def _find_plot (self, tx):
		for record in tx.run(Cypher.plot_find, 
			country=self.country, 
			region=self.region, 
			farm=self.farm, 
			plot=self.plot):
			return (record['name'])
	def add_plot(self, region, farm, plot):
		self.region=region
		self.farm=farm
		self.plot=plot
		with driver.session() as session:
			session.write_transaction(self._add_plot)
	def _add_plot (self, tx):
		tx.run(Cypher.plot_id_lock)
		tx.run(Cypher.plot_add, 
			country = self.country, 
			region = self.region, 
			farm = self.farm, 
			plot = self.plot, 
			username = session['username'])
	@classmethod #has plotID so doesn't need country
	def find_block(cls, plotID, block):
		cls.plotID = plotID
		cls.block = block
		with driver.session() as session:
			return session.read_transaction(cls._find_block)
	@classmethod #has plotID so doesn't need country
	def _find_block(cls, tx):
		for record in tx.run(Cypher.block_find, 
			plotID = cls.plotID,
			block = cls.block):
			return (record['name'])
	@classmethod #has plotID so doesn't need country
	def add_block(cls, plotID, block):
		cls.plotID=plotID
		cls.block=block
		with driver.session() as session:
			session.write_transaction(cls._add_block)
	@classmethod #has plotID so doesn't need country
	def _add_block (cls, tx):
		tx.run(Cypher.block_id_lock,
			plotID = cls.plotID)
		tx.run(Cypher.block_add, 
			plotID = cls.plotID,
			block = cls.block,
			username = session['username'])
	@classmethod #has plotID so doesn't need country
	def add_trees(cls, plotID, count, blockUID=None):
		cls.plotID = plotID
		cls.count = count
		cls.blockUID = blockUID
		#register trees and return index data
		with driver.session() as neo4j_session:
			neo4j_session.write_transaction(cls._add_trees)
		#create user download path if not found
		if not os.path.isdir(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username'])):
			os.mkdir(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username']))
		#prepare variables to write the file
		fieldnames = ['UID','PlotID','TreeID', 'Block', 'Plot', 'Farm', 'Region', 'Country']
		time = datetime.now().strftime('%Y%m%d-%H%M%S')
		first_tree_id = cls.id_list[0]['TreeID']
		last_tree_id = cls.id_list[-1]['TreeID']
		filename = time + '_plot_' + str(plotID) + '_T' + str(first_tree_id) + '_to_T' + str(last_tree_id) + '.csv'
		file_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username'], filename)
		#make the file
		with open (file_path, 'w') as file:
			writer = csv.DictWriter(file,
				fieldnames=fieldnames,
				quoting=csv.QUOTE_ALL,
				extrasaction='ignore')
			writer.writeheader()
			for row in cls.id_list:
				writer.writerow(row)
			file_size = file.tell()
		return { "filename":filename, 
			"file_path":file_path, 
			"file_size":file_size, 
			"first_tree_id":first_tree_id, 
			"last_tree_id":last_tree_id }
	@classmethod #has plotID so doesn't need country
	def _add_trees(cls, tx):
		tx.run(Cypher.tree_id_lock,
			plotID = cls.plotID)
		if cls.blockUID == None:
			result=tx.run(Cypher.trees_add, 
				plotID = cls.plotID,
				count = cls.count,
				username = session['username'])
			cls.id_list = [record[0] for record in result]
		else:
			result=tx.run(Cypher.block_trees_add, 
				plotID = cls.plotID,
				count = cls.count,
				blockUID = cls.blockUID,
				username = session['username'])
			cls.id_list = [record[0] for record in result]
	@classmethod #has plotID so doesn't need country
	def get_trees(cls, plotID, start, end):
		cls.plotID = plotID
		cls.start = start
		cls.end = end
		with driver.session() as neo4j_session:
			neo4j_session.read_transaction(cls._get_trees)
		#create user download path if not found
		if not os.path.isdir(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username'])):
			os.mkdir(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username']))
		#prepare variables to write file
		fieldnames = ['UID','PlotID','TreeID', 'TreeName', 'Block', 'Plot', 'Farm', 'Region', 'Country']
		time = datetime.now().strftime('%Y%m%d-%H%M%S')
		filename = time + '_plot_' + str(plotID) + '_T' + str(start) + '_to_T' + str(end) + '.csv'
		file_path =  os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], session['username'], filename)
		with open (file_path, 'w') as file:
			writer = csv.DictWriter(file,
				fieldnames=fieldnames,
				quoting=csv.QUOTE_ALL,
				extrasaction='ignore')
			writer.writeheader()
			for row in cls.id_list:
				writer.writerow(row)
			file_size = file.tell()
		return { "filename":filename,
			"file_path":file_path,
			"file_size":file_size }
	@classmethod #has plotID so doesn't need country
	def _get_trees(cls, tx):
		result=tx.run(Cypher.trees_get, 
			plotID = cls.plotID,
			start = cls.start,
			end = cls.end)
		cls.id_list = [record[0] for record in result]
	def get_farms(self, region):
		self.region=region
		with driver.session() as session:
			result = session.read_transaction(self._get_farms)
			return [(record[0]['name']) for record in result]
	def _get_farms(self, tx):
		return tx.run(Cypher.get_farms, 
			country=self.country, 
			region=self.region)
	def get_plotIDs(self, region, farm):
		self.region=region
		self.farm=farm
		with driver.session() as session:
			result = session.read_transaction(self._get_plots)
			return [(str(record[0]['uid'])) for record in result]
	def get_plots_tup(self, region, farm):
		self.region=region
		self.farm=farm
		with driver.session() as session:
			result = session.read_transaction(self._get_plots)
			return [(str(record[0]['uid']), record[0]['name']) for record in result]
	def _get_plots(self, tx):
		return tx.run(Cypher.get_plots, 
			country=self.country, 
			region=self.region, 
			farm=self.farm)
	@classmethod #has plotID so doesn't need country
	def get_blockUIDs(cls, plotID):
		cls.plotID = plotID
		with driver.session() as session:
			result = session.read_transaction(cls._get_blocks)
			return [(record[0]['uid']) for record in result]
	@classmethod #has plotID so doesn't need country
	def get_blocks_tup(cls, plotID):
		cls.plotID = plotID
		with driver.session() as session:
			result = session.read_transaction(cls._get_blocks)
			return [(record[0]['uid'], record[0]['name']) for record in result]
	@classmethod #has plotID so doesn't need country
	def _get_blocks(cls, tx):
		return tx.run(Cypher.get_blocks,
			plotID = cls.plotID)
	@classmethod #has plotID so doesn't need country
	def make_blocks_csv(cls, username, plotID):
		cls.plotID = plotID
		#get the block index data (country, region etc.)
		with driver.session() as session:
			result = session.read_transaction(cls._get_blocks_csv)
			cls.id_list = [record[0] for record in result]
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
			for row in cls.id_list:
				writer.writerow(row)
			file_size = file.tell()
		return {"filename":filename, "file_path":file_path, "file_size":file_size}
	@classmethod #has plotID so doesn't need country
	def _get_blocks_csv(cls, tx):
		return tx.run(Cypher.get_blocks_csv,
			plotID = cls.plotID)
		