from app import (
	# app,
	ServiceUnavailable,
	AuthError
)
from app.cypher import Cypher
from neo4j_driver import get_driver, neo4j_query
from datetime import datetime
from flask import jsonify


class Chart:
	def __init__(self):
		pass

	# get lists of submitted nodes and relationships in json format
	@staticmethod
	def get_submissions_range(username, startdate, enddate):
		try:
			epoch = datetime.utcfromtimestamp(0)
			parameters = {
				'username': username,
				'starttime': ((datetime.strptime(startdate, '%Y-%m-%d')-epoch).total_seconds())*1000,
				'endtime': ((datetime.strptime(enddate, '%Y-%m-%d')-epoch).total_seconds())*1000
			}
			with get_driver().session() as neo4j_session:
				result = neo4j_session.read_transaction(neo4j_query, Cypher.get_submissions_range, parameters)
			records = [record for record in result]
			# collect all nodes/rels from records into lists of dicts
			nodes = []
			rels = []
			for record in records:
				nodes.extend(
					(
						{'id': record['d_id'], 'label': record['d_label'], 'name': record['d_name']},
						{'id': record['n_id'], 'label': record['n_label'], 'name': record['n_name']}
					)
				)
				rels.append(
					{
						'id': record['r_id'],
						'source': record['r_start'],
						'type': record['r_type'],
						'target': record['r_end']
					}
				)
			# connect counters to block/trial
			for node in nodes:
				if str(node['id']).endswith('_count_node'):
					rels.append(
						{
							'source': node['id'],
							'type': 'FROM_COUNTER',
							'id': (node['id'] + '_' + node['id'].split('_')[0]),
							'target': int(node['id'].split('_')[0])
						}
					)
			# then uniquify
			nodes = {node['id']: node for node in nodes}.values()
			rels = {rel['id']: rel for rel in rels}.values()
			# and create the d3 input
			return jsonify({"nodes": nodes, "links": rels})
		except (ServiceUnavailable, AuthError):
			return jsonify({"status": "Database unavailable"})


	# get lists of submitted nodes (relationships and directly linked nodes) in json format
	@staticmethod
	def get_trials_treecount():
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(neo4j_query, Cypher.get_trials_treecount)
		records = [record for record in result]
		nested = {
			'name': 'nodes',
			'label': 'root_node',
			'children': [record[0] for record in records]
		}
		return jsonify(nested)
