import os
import grp
import unicodecsv as csv 
from app import app
from app.cypher import Cypher
from neo4j_driver import get_driver, neo4j_query
from flask import session
from datetime import datetime


class Samples:
	def __init__(self):
		pass

	@staticmethod
	def add_tissue(tissue):
		parameters = {
			'tissue': tissue,
			'username': session['username']
		}
		with get_driver().session() as neo4j_session:
			result = neo4j_session.write_transaction(neo4j_query, Cypher.tissue_add, parameters)
			return [record[0] for record in result]

	@staticmethod
	def add_storage(storage):
		parameters = {
			'storage': storage,
			'username': session['username']
		}
		with get_driver().session() as neo4j_session:
			result = neo4j_session.write_transaction(neo4j_query, Cypher.storage_add, parameters)
			return [record[0] for record in result]

	@staticmethod
	def add_samples_per_tree(field_uid, start, end, per_tree_replicates):
		with get_driver().session() as neo4j_session:
			add_parameters = {
				'field_uid': field_uid,
				'start': start,
				'end': end,
				'replicates': per_tree_replicates,
				'username': session['username']
			}
			result = neo4j_session.write_transaction(neo4j_query, Cypher.samples_add_per_tree, add_parameters)
			id_list = [record[0] for record in result]
		return id_list

	@staticmethod
	def add_samples_pooled(field_uid, sample_count):
		with get_driver().session() as neo4j_session:
			add_parameters = {
				'field_uid': field_uid,
				'replicates': sample_count,
				'username': session['username']
			}
			result = neo4j_session.write_transaction(neo4j_query, Cypher.samples_add_pooled, add_parameters)
			id_list = [record[0] for record in result]
		return id_list


	@staticmethod
	def get_samples(parameters):
		# build the query
		# match samples by location in the graph
		q = ' MATCH (country:Country '
		if parameters['country']:
			q = q + ' {name_lower:toLower($country)} '
		q = q + ')<-[:IS_IN]-(region:Region '
		if parameters['region']:
			q = q + ' {name_lower:toLower($region)} '
		q = q + ')<-[:IS_IN]-(farm:Farm '
		if parameters['farm']:
			q = q + '{name_lower:toLower($farm)} '
		q = q + ')<-[:IS_IN]-(field:Field '
		if parameters['field_uid']:
			q = q + ' {uid:$field_uid}'
		q = (
				q + ')<-[:FROM_FIELD]-(:FieldSamples) '
				+ ' <-[:FROM_FIELD]-(tts:FieldTissueStorage) '
				+ ' <-[:COLLECTED_AS]-(sample),'
		)
		# collect the Tissue/Storage container
		q = (
				q + ' (tts)-[:COLLECTED_AS]->(TiSt:TissueStorage) '
				+ ' -[:OF_TISSUE]->(tissue:Tissue '
		)
		# and if tissue specified filter by tissue
		if parameters['tissue']:
			q = q + ' {name_lower:toLower($tissue)}'
		# and same for storage
		q = q + '), (TiSt)-[:STORED_IN]-(storage:Storage '
		if parameters['storage']:
			q = q + ' {name_lower:toLower($storage)} '
		# find the tree from each samples
		q = (
				q + '), (sample)-[:FROM_TREE]->(:TreeSamples) '
				+ ' -[:FROM_TREE]->(tree:Tree) '
		)
		# now parse out ranges of values provided, first from trees if provided a range, then from samples
		# not sure if there is an order to processing of where statements
		# ..but would be better to do trees first anyway i guess
		if any(
			[
				parameters[key] != '' for key in [
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
			for i, f in enumerate(filters_list):
				if i != 0:
					q = q + ' AND '
				q = q + f
		# get block name
		q = (
				q + ' OPTIONAL MATCH (tree) '
				' -[:IS_IN {current: True}]->(:BlockTrees) '
				' -[:IS_IN]->(block:Block) '
		)
		# get branch ID
		q = (
				q + ' OPTIONAL MATCH (sample) '
				' -[:FROM_BRANCH {current : True}]->(branch:Branch) '
		)
		# get leaf ID
		q = (
				q + ' OPTIONAL MATCH (sample) '
				' -[:FROM_LEAF {current: True}]->(leaf:Leaf) '
		)
		# build the return statement
		q = q + (
			' RETURN { '
			'	Country : country.name, '
			'	Region : region.name, '
			'	Farm : farm.name, '
			'	Field : field.name, '
			'	`Field UID`	: field.uid, '
			'	Block : block.name, '
			'	`Block UID` : block.uid, '
			'	`Tree UID` : tree.uid, '
			'	`Tree Custom ID` : tree.custom_id, '
			'	Variety: tree.variety, '
			'	`Branch UID` : branch.uid, '
			'	`Leaf UID` : leaf.uid, '
			'	Tissue : tissue.name, '
			'	Storage : storage.name, '
			'	`Date Sampled` : sample.date, '
			'	UID: sample.uid, '
			'	`Sample ID` : sample.id '
			' } '
		)
		# and order by sample id
		query = q + ' ORDER BY sample.id'
		# return index data
		with get_driver().session() as neo4j_session:
			id_list = [record[0] for record in neo4j_session.read_transaction(neo4j_query, query, parameters)]
		# check if any data found, if not return none
		return id_list
