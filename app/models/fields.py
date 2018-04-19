import os
import unicodecsv as csv 
import cStringIO
from app import app
from app.cypher import Cypher
from neo4j_driver import get_driver, neo4j_query
from flask import session
from datetime import datetime
from neo4j_driver import get_driver

class Fields:
	def __init__(self, country):
		self.country=country
	#find location procedures
	def find_country(self):
		with get_driver().session() as neo4j_session:
			parameters = {
				'country':self.country
			}
			result = neo4j_session.read_transaction(neo4j_query, Cypher.country_find, parameters)
			return [record[0] for record in result]
	def find_region(self, region):
		with get_driver().session() as neo4j_session:
			parameters = {
				'country': self.country,
				'region': region,
				'username': session['username']
			}
			result = neo4j_session.read_transaction(neo4j_query, Cypher.region_find, parameters)
			return [record[0] for record in result]
	def find_farm(self, region, farm):
		with get_driver().session() as neo4j_session:
			parameters = {
				'country':self.country,
				'region': region,
				'farm': farm,
				'username': session['username']
			}
			result = neo4j_session.read_transaction(neo4j_query, Cypher.farm_find, parameters)
			return [record[0] for record in result]
	def find_plot(self, region, farm, plot):
		with get_driver().session() as neo4j_session:
			parameters = {
				'country': self.country,
				'region': region,
				'farm': farm,
				'plot': plot,
				'username': session['username']
			}
			result = neo4j_session.read_transaction(neo4j_query, Cypher.plot_find, parameters)
			return [record[0] for record in result]
	#find item procedures - these have plotID so don't need country
	@staticmethod
	def find_block(plotID, block):
		with get_driver().session() as neo4j_session:
			parameters = {
				'plotID' : plotID,
				'block' : block,
			}
			result = neo4j_session.read_transaction(neo4j_query, Cypher.block_find, parameters)
			return [record[0] for record in result]
	#add location procedures
	def add_country(self):
		with get_driver().session() as neo4j_session:
			parameters = {
				'country': self.country,
				'username':session['username']
			}
			result = neo4j_session.write_transaction(neo4j_query, Cypher.country_add, parameters)
			return [record[0] for record in result]
	def add_region(self, region):
		with get_driver().session() as neo4j_session:
			parameters = {
				'country': self.country,
				'region': region,
				'username': session['username']
			}
			result = neo4j_session.write_transaction(neo4j_query, Cypher.region_add, parameters)
			return [record[0] for record in result]
	def add_farm(self, region, farm):
		with get_driver().session() as neo4j_session:
			parameters = {
				'country': self.country,
				'region': region,
				'farm': farm,
				'username': session['username']
			}
			result = neo4j_session.write_transaction(neo4j_query, Cypher.farm_add, parameters)
			return [record[0] for record in result]
	def add_plot(self, region, farm, plot):
		with get_driver().session() as neo4j_session:
			parameters = {
				'country': self.country,
				'region': region,
				'farm': farm,
				'plot':plot,
				'username': session['username']
			}
			result  = neo4j_session.write_transaction(neo4j_query, Cypher.plot_add, parameters)
			return [record[0] for record in result]
	#add item procedures - these have plotID so don't need country
	@staticmethod
	def add_block(plotID, block):
		with get_driver().session() as neo4j_session:
			lock_parameters = {
				'plotID' : plotID,
				'level' : "block"
			}
			neo4j_session.write_transaction(neo4j_query, Cypher.id_lock, lock_parameters)
			add_parameters = {
				'plotID' : plotID,
				'block' : block,
				'username' : session['username']
			}
			result = neo4j_session.write_transaction(neo4j_query, Cypher.block_add, add_parameters)
			return [record[0] for record in result]
	@staticmethod
	def add_trees(plotID, count, blockUID):
		if blockUID:
			with get_driver().session() as neo4j_session:
				plot_lock_parameters = {
					'plotID': plotID,
					'level': 'tree'
				}
				neo4j_session.write_transaction(neo4j_query, Cypher.id_lock, plot_lock_parameters)
				block_lock_parameters = {
					'blockUID': blockUID,
					'level': 'tree'
				}
				neo4j_session.write_transaction(neo4j_query, Cypher.block_id_lock, block_lock_parameters)
				add_parameters = {
					'plotID': plotID,
					'blockUID': blockUID,
					'count': count,
					'username': session['username']
				}
				result = neo4j_session.write_transaction(neo4j_query, Cypher.trees_add_block, add_parameters)
		else:
			with get_driver().session() as neo4j_session:
				lock_parameters = {
					'plotID': plotID,
					'level': 'tree'
				}
				neo4j_session.write_transaction(neo4j_query, Cypher.id_lock, lock_parameters)
				add_parameters = {
					'plotID': plotID,
					'count': count,
					'username': session['username']
				}
				result = neo4j_session.write_transaction(neo4j_query, Cypher.trees_add, add_parameters)
		return [record[0] for record in result]
	@staticmethod
	def add_branches(plotID, start, end, replicates):
		with get_driver().session() as neo4j_session:
			lock_parameters = {
				'plotID' : plotID,
				'level' : 'branch'
			}
			neo4j_session.write_transaction(neo4j_query, Cypher.id_lock, lock_parameters)
			add_parameters = {
				'plotID' : plotID,
				'start' : start,
				'end' : end,
				'replicates' : replicates,
				'username': session['username']
			}
			result = neo4j_session.write_transaction(neo4j_query, Cypher.branches_add, add_parameters)
		return [record[0] for record in result]
	@staticmethod
	def add_leaves(plotID, start, end, replicates):
		with get_driver().session() as neo4j_session:
			lock_parameters = {
				'plotID' : plotID,
				'level' : 'leaf'
			}
			neo4j_session.write_transaction(neo4j_query, Cypher.id_lock, lock_parameters)
			add_parameters = {
				'plotID' : plotID,
				'start' : start,
				'end' : end,
				'replicates' : replicates,
				'username': session['username']
			}
			result = neo4j_session.write_transaction(neo4j_query, Cypher.leaves_add, add_parameters)
		return [record[0] for record in result]
	# get lists of locations
		# get regions can be found with a generic get_connected function
		# the below functions additionally check for all relevant parents and type
		#  e.g. (item)-[:IS_IN]->(parent)-[:IS_IN]->(grandparent)
	def get_farms(self, region):
		with get_driver().session() as neo4j_session:
			parameters = {
				'country': self.country,
				'region': region
			}
			result = neo4j_session.read_transaction(neo4j_query, Cypher.get_farms, parameters)
			return [(record[0]['name']) for record in result]
	def get_plots_tup(self, region, farm):
		with get_driver().session() as neo4j_session:
			parameters = {
				'country': self.country,
				'region': region,
				'farm': farm
			}
			result = neo4j_session.read_transaction(neo4j_query, Cypher.get_plots, parameters)
			return [(str(record[0]['uid']), record[0]['name']) for record in result]
	#get lists of items - these have plotID so don't need country
	@staticmethod  # has plotID so doesn't need country
	def get_blocks(plotID):
		with get_driver().session() as neo4j_session:
			parameters = {
				'plotID': plotID
			}
			result = neo4j_session.read_transaction(neo4j_query, Cypher.get_blocks_details, parameters)
			return [record[0] for record in result]
	@staticmethod  # has plotID so doesn't need country
	def get_blocks_tup(plotID):
		with get_driver().session() as neo4j_session:
			parameters = {
				'plotID': plotID
			}
			result = neo4j_session.read_transaction(neo4j_query, Cypher.get_blocks, parameters)
			return [(record[0]['uid'], record[0]['name']) for record in result]
	@staticmethod
	def get_trees(plotID, start = 0, end = 999999):
		with get_driver().session() as neo4j_session:
			parameters = {
				'plotID': plotID,
				'start': start,
				'end': end
			}
			return [record[0] for record in neo4j_session.read_transaction(neo4j_query, Cypher.trees_get, parameters)]
	@staticmethod
	def get_treecount(plotid):
		with get_driver().session() as neo4j_session:
			parameters = {
				'plotid': plotid
			}
			return [record[0] for record in neo4j_session.read_transaction(neo4j_query, Cypher.treecount, parameters)]
