# import os
# import unicodecsv as csv
# import cStringIO
# from app import app
from app.cypher import Cypher
from neo4j_driver import (
	# get_driver,
	neo4j_query
)
from flask import session
# from datetime import datetime
from neo4j_driver import get_driver


class Fields:
	def __init__(self, country):
		self.country = country

	# find location procedures
	def find_country(self):
		with get_driver().session() as neo4j_session:
			parameters = {
				'country': self.country
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
				'country': self.country,
				'region': region,
				'farm': farm,
				'username': session['username']
			}
			result = neo4j_session.read_transaction(neo4j_query, Cypher.farm_find, parameters)
			return [record[0] for record in result]

	def find_trial(self, region, farm, trial):
		with get_driver().session() as neo4j_session:
			parameters = {
				'country': self.country,
				'region': region,
				'farm': farm,
				'trial': trial,
				'username': session['username']
			}
			result = neo4j_session.read_transaction(neo4j_query, Cypher.trial_find, parameters)
			return [record[0] for record in result]

	# find item procedures - these have trial_uid so don't need country
	@staticmethod
	def find_block(trial_uid, block):
		with get_driver().session() as neo4j_session:
			parameters = {
				'trial_uid': trial_uid,
				'block': block,
			}
			result = neo4j_session.read_transaction(neo4j_query, Cypher.block_find, parameters)
			return [record[0] for record in result]

	# add location procedures
	def add_country(self):
		with get_driver().session() as neo4j_session:
			parameters = {
				'country': self.country,
				'username': session['username']
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

	def add_trial(self, region, farm, trial):
		with get_driver().session() as neo4j_session:
			parameters = {
				'country': self.country,
				'region': region,
				'farm': farm,
				'trial': trial,
				'username': session['username']
			}
			result = neo4j_session.write_transaction(neo4j_query, Cypher.trial_add, parameters)
			return [record[0] for record in result]

	# add item procedures - these have trial_uid so don't need country
	@staticmethod
	def add_block(trial_uid, block):
		with get_driver().session() as neo4j_session:
			add_parameters = {
				'trial_uid': trial_uid,
				'block': block,
				'username': session['username']
			}
			result = neo4j_session.write_transaction(neo4j_query, Cypher.block_add, add_parameters)
			return [record[0] for record in result]

	@staticmethod
	def add_trees(trial_uid, count, block_uid):
		if block_uid:
			with get_driver().session() as neo4j_session:
				add_parameters = {
					'trial_uid': trial_uid,
					'block_uid': block_uid,
					'count': count,
					'username': session['username']
				}
				result = neo4j_session.write_transaction(neo4j_query, Cypher.trees_add_block, add_parameters)
		else:
			with get_driver().session() as neo4j_session:
				add_parameters = {
					'trial_uid': trial_uid,
					'count': count,
					'username': session['username']
				}
				result = neo4j_session.write_transaction(neo4j_query, Cypher.trees_add, add_parameters)
		return [record[0] for record in result]

	@staticmethod
	def add_branches(trial_uid, start, end, replicates):
		with get_driver().session() as neo4j_session:
			add_parameters = {
				'trial_uid': trial_uid,
				'start': start,
				'end': end,
				'replicates': replicates,
				'username': session['username']
			}
			result = neo4j_session.write_transaction(neo4j_query, Cypher.branches_add, add_parameters)
		return [record[0] for record in result]

	@staticmethod
	def add_leaves(trial_uid, start, end, replicates):
		with get_driver().session() as neo4j_session:
			add_parameters = {
				'trial_uid': trial_uid,
				'start': start,
				'end': end,
				'replicates': replicates,
				'username': session['username']
			}
			result = neo4j_session.write_transaction(neo4j_query, Cypher.leaves_add, add_parameters)
		return [record[0] for record in result]

	# get lists of locations
	# get regions can be found with a generic get_connected function
	# the below functions additionally check for all relevant parents and type
	# e.g. (item)-[:IS_IN]->(parent)-[:IS_IN]->(grandparent)
	def get_farms(self, region):
		with get_driver().session() as neo4j_session:
			parameters = {
				'country': self.country,
				'region': region
			}
			result = neo4j_session.read_transaction(neo4j_query, Cypher.get_farms, parameters)
			return [(record[0]['name']) for record in result]

	def get_trials_tup(self, region, farm):
		with get_driver().session() as neo4j_session:
			parameters = {
				'country': self.country,
				'region': region,
				'farm': farm
			}
			result = neo4j_session.read_transaction(neo4j_query, Cypher.get_trials, parameters)
			return [(str(record[0]['uid']), record[0]['name'].title()) for record in result]

	# get lists of items - these have trial_uid so don't need country
	@staticmethod  # has trial_uid so doesn't need country
	def get_blocks(trial_uid):
		with get_driver().session() as neo4j_session:
			parameters = {
				'trial_uid': trial_uid
			}
			result = neo4j_session.read_transaction(neo4j_query, Cypher.get_blocks_details, parameters)
			return [record[0] for record in result]

	@staticmethod  # has trial_uid so doesn't need country
	def get_blocks_tup(trial_uid):
		with get_driver().session() as neo4j_session:
			parameters = {
				'trial_uid': trial_uid
			}
			result = neo4j_session.read_transaction(neo4j_query, Cypher.get_blocks, parameters)
			return [(record[0]['uid'], record[0]['name'].title()) for record in result]

	@staticmethod
	def get_trees(trial_uid, start = 0, end = 999999):
		with get_driver().session() as neo4j_session:
			parameters = {
				'trial_uid': trial_uid,
				'start': start,
				'end': end
			}
			return [record[0] for record in neo4j_session.read_transaction(neo4j_query, Cypher.trees_get, parameters)]

	@staticmethod
	def get_treecount(trial_uid):
		with get_driver().session() as neo4j_session:
			parameters = {
				'trial_uid': trial_uid
			}
			return [record[0] for record in neo4j_session.read_transaction(neo4j_query, Cypher.treecount, parameters)]
