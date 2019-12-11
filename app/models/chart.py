from app import (
	# app,
	ServiceUnavailable,
	SecurityError
)
from app.cypher import Cypher
from neo4j_driver import get_driver, neo4j_query
from datetime import datetime
from flask import jsonify
from parsers import Parsers


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
			# uniquify
			nodes = {node['id']: node for node in nodes}.values()
			rels = {rel['id']: rel for rel in rels}.values()
			# and create the d3 input
			return jsonify({"nodes": nodes, "links": rels})
		except (ServiceUnavailable, SecurityError):
			return jsonify({"status": "Database unavailable"})

	# get lists of submitted nodes (relationships and directly linked nodes) in json format
	@staticmethod
	def get_fields_treecount():
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(neo4j_query, Cypher.get_fields_treecount)
		records = [record for record in result]
		nested = {
			'name': 'nodes',
			'label': 'root_node',
			'children': [record[0] for record in records]
		}
		return jsonify(nested)

	@staticmethod
	def get_tree_count(uid):
		parameters = {
			'uid': uid
		}
		statement = (
			' MATCH '
			'	(c: Counter { '
			'		uid: ($uid + "_tree") '
			'	}) '
			' RETURN '
			'	c.count '
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(neo4j_query, statement, parameters)
			return [record[0] for record in result]

	@staticmethod
	def get_item_count(
			level,
			country=None,
			region=None,
			farm=None,
			field_uid=None,
			field_uid_list=None,
			block_uid=None,
			block_id_list=None,
			tree_id_list=None,
			sample_id_list=None
	):
		if field_uid_list:
			try:
				field_uid_list = Parsers.parse_range_list(field_uid_list)
				if len(field_uid_list) >= 1000:
					return "Please select a range of less than 1000 field IDs"
			except ValueError:
				return 'Invalid range of Field UIDs'
		if block_id_list:
			try:
				block_id_list = Parsers.parse_range_list(block_id_list)
				if len(block_id_list) >= 1000:
					return "Please select a range of less than 1000 block IDs"
			except ValueError:
				return 'Invalid range of Block IDs'
		if tree_id_list:
			try:
				tree_id_list = Parsers.parse_range_list(tree_id_list)
				if len(tree_id_list) >= 10000:
					return "Please select a range of less than 10000 tree IDs"
			except ValueError:
				return 'Invalid range of tree IDs'
		if sample_id_list:
			try:
				sample_id_list = Parsers.parse_range_list(sample_id_list)
				if len(sample_id_list) >= 10000:
					return "Please select a range of less than 10000 tree IDs"
			except ValueError:
				return 'Invalid range of sample IDs'
		parameters = {}
		statement = (
			' MATCH '
		)
		if country:
			parameters['country'] = country
			statement += (
				' (:Country '
				'	{ '
				'		name_lower: $country '
				'	}'
				' )<-[:IS_IN]-(:Region '
			)
			if region:
				parameters['region'] = region
				statement += (
					' { '
					'	name_lower: $region '
					' } '
				)
			statement += (
				' )<-[:IS_IN]-(:Farm '
			)
			if farm:
				parameters['farm'] = farm
				statement += (
					' { '
					'	name_lower: $farm '
					' } '
				)
			statement += (
				' )<-[:IS_IN]-(field:Field '
			)
			if field_uid:
				parameters['field_uid'] = field_uid
				statement += (
					' { '
					'	uid: toInteger($field_uid) '
					' } '
				)
			statement += (
				' ) '
			)
		else:
			statement += (
				' (field: Field) '
			)
		if level == 'field':
			if field_uid_list and not field_uid:
				parameters['field_uid_list'] = field_uid_list
				statement += (
					' WHERE field.uid IN $field_uid_list '
				)
			statement += (
				' RETURN count(field) '
			)
		else:
			if any([level == 'block', block_uid, block_id_list]):
				statement += (
					' <-[:IS_IN]-(:FieldBlocks) '
					' <-[:IS_IN]-(block:Block '
				)
				if block_uid:
					parameters['block_uid'] = block_uid
					statement += (
						' { '
						'	uid: $block_uid '
						' } '
					)
				statement += (
					' ) '
				)
				if level == 'block':
					if any([
						field_uid_list and not field_uid,
						block_id_list and not block_uid
					]):
						statement += ' WHERE '
						if field_uid_list and not field_uid:
							parameters['field_uid_list'] = field_uid_list
							statement += (
								' field.uid IN $field_uid_list '
							)
							if block_id_list and not block_uid:
								statement += ' AND '
						if block_id_list and not block_uid:
							parameters['block_id_list'] = block_id_list
							statement += (
								' block.id IN $block_id_list '
							)
					statement += (
						' RETURN count(block) '
					)
			if any([level == 'tree', tree_id_list]):
				statement += (
					' <-[:IS_IN]-'
				)
				if block_uid or block_id_list:
					statement += (
						' (:BlockTrees) '
						' <-[:IS_IN]-'
					)
				else:
					statement += (
						' (:FieldTrees) '
						' <-[:IS_IN]-'
					)
				statement += (
					' (tree:Tree) '
				)
				if level == 'tree':
					if any([
						field_uid_list and not field_uid,
						block_id_list and not block_uid,
						tree_id_list
					]):
						statement += ' WHERE '
						if field_uid_list and not field_uid:
							parameters['field_uid_list'] = field_uid_list
							statement += (
								' field.uid IN $field_uid_list '
							)
							if any([
								block_id_list and not block_uid,
								tree_id_list
							]):
								statement += ' AND '
						if block_id_list and not block_uid:
							parameters['block_id_list'] = block_id_list
							statement += (
								' block.id IN $block_id_list '
							)
							if tree_id_list:
								statement += 'AND '
						if tree_id_list:
							parameters['tree_id_list'] = tree_id_list
							statement += (
								' tree.id in $tree_id_list '
							)
					statement += (
						' RETURN count(tree) '
					)
			if level == 'sample':
				statement += (
					' <-[: FROM | IS_IN* ]-(sample: Sample) '
				)
				if any([
					field_uid_list and not field_uid,
					block_id_list and not block_uid,
					tree_id_list,
					sample_id_list
				]):
					statement += ' WHERE '
					if field_uid_list and not field_uid:
						parameters['field_uid_list'] = field_uid_list
						statement += (
							' field.uid IN $field_uid_list '
						)
						if any([
							block_id_list and not block_uid,
							tree_id_list,
							sample_id_list
						]):
							statement += ' AND '
					if block_id_list and not block_uid:
						parameters['block_id_list'] = block_id_list
						statement += (
							' block.id IN $block_id_list '
						)
						if any([
							tree_id_list,
							sample_id_list
						]):
							statement += 'AND '
					if tree_id_list:
						parameters['tree_id_list'] = tree_id_list
						statement += (
							' tree.id in $tree_id_list '
						)
						if sample_id_list:
							statement += ' AND '
					if sample_id_list:
						parameters['sample_id_list'] = sample_id_list
						statement += (
							' sample.id IN $sample_id_list '
						)
				statement += (
					' RETURN count(distinct(sample)) '
				)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				neo4j_query,
				statement,
				parameters
			)[0][0]
			return result






