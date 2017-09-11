import csv, cStringIO
from app import app
from app.cypher import Cypher
from config import uri, driver
from flask import session

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
			region=self.region, 
			farm=self.farm, 
			plot=self.plot, 
			username=session['username'])
	def add_trees(self, region, farm, plot, count):
		self.region = region
		self.farm = farm
		self.plot = plot
		self.count = count
		with driver.session() as session:
			session.write_transaction(self._add_trees)
		fieldnames = ['UID','PlotID','TreeCount']
		fields_csv = cStringIO.StringIO()
		writer = csv.DictWriter(fields_csv,
			fieldnames=fieldnames,
			quoting=csv.QUOTE_ALL,
			extrasaction='ignore')
		writer.writeheader()
		for tree in self.id_list:
			writer.writerow(tree)
		fields_csv.seek(0)
		return fields_csv
	def _add_trees(self, tx):
		tx.run(Cypher.tree_id_lock,
			country = self.country, 
			region = self.region, 
			farm = self.farm, 
			plot = self.plot)
		result=tx.run(Cypher.trees_add, 
			country = self.country, 
			region = self.region, 
			farm = self.farm, 
			plot = self.plot,
			count = self.count,
			username= session['username'])
		self.id_list = [{'UID':str(record[0][0]), 'PlotID':record[0][1],'TreeCount':record[0][2]} for record in result]
	def get_trees(self, region, farm, plot, start, end):
		self.region = region
		self.farm = farm
		self.plot = plot
		self.start = start
		self.end = end
		with driver.session() as session:
			session.read_transaction(self._get_trees)
		fieldnames = ['UID','PlotID','TreeCount']
		fields_csv = cStringIO.StringIO()
		writer = csv.DictWriter(fields_csv,
			fieldnames=fieldnames,
			quoting=csv.QUOTE_ALL,
			extrasaction='ignore')
		writer.writeheader()
		for tree in self.id_list:
			writer.writerow(tree)
		fields_csv.seek(0)
		return fields_csv
	def _get_trees(self, tx):
		result=tx.run(Cypher.trees_get, 
			country = self.country, 
			region = self.region, 
			farm = self.farm, 
			plot = self.plot,
			start = self.start,
			end = self.end)
		self.id_list = [{'UID':str(record[0][0]), 'PlotID':record[0][1],'TreeCount':record[0][2]} for record in result]
	def get_farms(self, region):
		self.region=region
		with driver.session() as session:
			return session.read_transaction(self._get_farms)
	def _get_farms(self, tx):
		result = tx.run(Cypher.get_farms, 
			country=self.country, 
			region=self.region)
		dict_result= [record[0] for record in result]
		return [(node['name'], node['name']) for node in dict_result]
	def get_plots(self, region, farm):
		self.region=region
		self.farm=farm
		with driver.session() as session:
			return session.read_transaction(self._get_plots)
	def _get_plots(self, tx):
		result = tx.run(Cypher.get_plots, 
			country=self.country, 
			region=self.region, 
			farm=self.farm)
		dict_result= [record[0] for record in result]
		return [(node['name'], node['name']) for node in dict_result]