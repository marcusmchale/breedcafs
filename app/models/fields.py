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

	def find_field(self, region, farm, field):
		with get_driver().session() as neo4j_session:
			parameters = {
				'country': self.country,
				'region': region,
				'farm': farm,
				'field': field,
				'username': session['username']
			}
			result = neo4j_session.read_transaction(neo4j_query, Cypher.field_find, parameters)
			return [record[0] for record in result]

	# find item procedures - these have field_uid so don't need country
	@staticmethod
	def find_block(field_uid, block):
		with get_driver().session() as neo4j_session:
			parameters = {
				'field_uid': field_uid,
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

	@staticmethod
	def add_field(country, region, farm, field):
		with get_driver().session() as neo4j_session:
			parameters = {
				'country': country,
				'region': region,
				'farm': farm,
				'field': field,
				'username': session['username']
			}
			import pdb;
			pdb.set_trace()
			result = neo4j_session.write_transaction(neo4j_query, Cypher.field_add, parameters)
			return [record[0] for record in result]

	# add item procedures - these have field_uid so don't need country
	@staticmethod
	def add_block(field_uid, block):
		with get_driver().session() as neo4j_session:
			add_parameters = {
				'field_uid': field_uid,
				'block': block,
				'username': session['username']
			}
			result = neo4j_session.write_transaction(neo4j_query, Cypher.block_add, add_parameters)
			return [record[0] for record in result]

	@staticmethod
	def add_trees(field_uid, count, block_uid):
		if block_uid:
			with get_driver().session() as neo4j_session:
				add_parameters = {
					'field_uid': field_uid,
					'block_uid': block_uid,
					'count': count,
					'username': session['username']
				}
				result = neo4j_session.write_transaction(neo4j_query, Cypher.trees_add_block, add_parameters)
		else:
			with get_driver().session() as neo4j_session:
				add_parameters = {
					'field_uid': field_uid,
					'count': count,
					'username': session['username']
				}
				result = neo4j_session.write_transaction(neo4j_query, Cypher.trees_add, add_parameters)
		return [record[0] for record in result]

	@staticmethod
	def add_branches(field_uid, start, end, replicates):
		with get_driver().session() as neo4j_session:
			add_parameters = {
				'field_uid': field_uid,
				'start': start,
				'end': end,
				'replicates': replicates,
				'username': session['username']
			}
			result = neo4j_session.write_transaction(neo4j_query, Cypher.branches_add, add_parameters)
		return [record[0] for record in result]

	@staticmethod
	def add_leaves(field_uid, start, end, replicates):
		with get_driver().session() as neo4j_session:
			add_parameters = {
				'field_uid': field_uid,
				'start': start,
				'end': end,
				'replicates': replicates,
				'username': session['username']
			}
			result = neo4j_session.write_transaction(neo4j_query, Cypher.leaves_add, add_parameters)
		return [record[0] for record in result]

	@staticmethod
	# get lists of locations
	# get regions can be found with a generic get_connected function
	# the below functions additionally check for all relevant parents and type
	# e.g. (item)-[:IS_IN]->(parent)-[:IS_IN]->(grandparent)
	def get_farms(country, region):
		with get_driver().session() as neo4j_session:
			parameters = {
				'country': country,
				'region': region
			}
			result = neo4j_session.read_transaction(neo4j_query, Cypher.get_farms, parameters)
			names_list = []
			for record in result:
				if 'name' in record[0]:
					names_list.append(record[0]['name'])
				else:
					pass
			return names_list

	@staticmethod
	def get_fields_tup(
			country = None,
			region = None,
			farm = None
	):
		with get_driver().session() as neo4j_session:
			if farm:
				parameters = {
					'country': country,
					'region': region,
					'farm': farm
				}
				result = neo4j_session.read_transaction(neo4j_query, Cypher.get_fields_farm, parameters)
			elif region:
				parameters = {
					'country': country,
					'region': region
				}
				result = neo4j_session.read_transaction(neo4j_query, Cypher.get_fields_region, parameters)
			elif country:
				parameters = {
					'country': country,
				}
				result = neo4j_session.read_transaction(neo4j_query, Cypher.get_fields_country, parameters)
			else:
				result = neo4j_session.read_transaction(neo4j_query, Cypher.get_fields)
			return [(str(record[0]['uid']), record[0]['name']) for record in result]

	# get lists of items - these have field_uid so don't need country
	@staticmethod  # has field_uid so doesn't need country
	def get_blocks(field_uid):
		with get_driver().session() as neo4j_session:
			parameters = {
				'field_uid': field_uid
			}
			result = neo4j_session.read_transaction(neo4j_query, Cypher.get_blocks_details, parameters)
			return [record[0] for record in result]

	@staticmethod  # has field_uid so doesn't need country
	def get_blocks_tup(field_uid):
		with get_driver().session() as neo4j_session:
			parameters = {
				'field_uid': field_uid
			}
			result = neo4j_session.read_transaction(neo4j_query, Cypher.get_blocks, parameters)
			return [(record[0]['uid'], record[0]['name'].title()) for record in result]

	@staticmethod
	def get_trees(field_uid, start = 0, end = 999999):
		with get_driver().session() as neo4j_session:
			parameters = {
				'field_uid': field_uid,
				'start': start,
				'end': end
			}
			return [record[0] for record in neo4j_session.read_transaction(neo4j_query, Cypher.trees_get, parameters)]

	@staticmethod
	def get_treecount(field_uid):
		with get_driver().session() as neo4j_session:
			parameters = {
				'field_uid': field_uid
			}
			return [record[0] for record in neo4j_session.read_transaction(neo4j_query, Cypher.treecount, parameters)]
