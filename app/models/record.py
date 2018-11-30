from app import app, ServiceUnavailable, AuthError

from app.cypher import Cypher
from app.emails import send_email
from flask import render_template, url_for, jsonify

from neo4j_driver import get_driver, neo4j_query

import datetime


class Record:
	def __init__(self, username):
		self.username = username

	def submit_records(
			self,
			record_data
	):
		with get_driver().session() as neo4j_session:
			# Make sure we have the record submission node for this user
			if not [i for i in neo4j_session.read_transaction(
				neo4j_query,
				Cypher.match_record_submission_node,
				{'username': self.username}
			)]:
				print('record submission node not found, merging')
				neo4j_session.write_transaction(
					neo4j_query,
					Cypher.merge_record_submission_node,
					{'username': self.username}
				)
			record_data['username'] = self.username
			conflicts = self.find_conflicts(
				record_data
			)
			if conflicts:
				html_table = self.result_table(conflicts)
				return jsonify({
					'submitted': (
						' Record not submitted. <br><br> '
						' Conflicting values for some of the input records are found in this period: '
						+ html_table
					),
					'class': 'conflicts'
				})
			else:
				merged = self.merge_records(record_data)
				html_table = self.result_table(merged)
				return jsonify({'submitted': (
					' Records submitted or updated (highlighted): '
					+ html_table
					)
				})

	@staticmethod
	def find_conflicts(
			record_data,
	):
		with get_driver().session() as neo4j_session:
			parameters = {
				'level': record_data['level'],
				'start_time': record_data['start_time'],
				'end_time': record_data['end_time']
			}
			query = (
				' MATCH (:Country '
			)
			if record_data['country']:
				parameters['country'] = record_data['country']
				query += (
					' {name_lower: toLower($country)} ) '
				)
			query += (
				' )<-[:IS_IN]-(:Region '
			)
			if record_data['region']:
				parameters['region'] = record_data['region']
				query += (
					' {name_lower: toLower($region)} '
				)
			query += (
				' )<-[:IS_IN]-(:Farm '
			)
			if record_data['farm']:
				parameters['farm'] = record_data['farm']
				query += (
					' {name_lower: toLower($farm)} '
				)
			if record_data['level'] == 'field':
				query += (
					' )<-[:IS_IN]-(item:Field '
				)
			else: query += (
					' )<-[:IS_IN]-(:Field '
				)
			if record_data['field_uid']:
				parameters['field_uid'] = record_data['field_uid']
				query += (
					' {uid: $field_uid} '
				)
			query += (
				' ) '
			)
			if any([
				record_data['level'] == 'block',
				all([
					record_data['level'] == 'tree',
					record_data['block_uid']
				])
			]):
				if record_data['level'] == 'tree':
					query += (
						'<-[:IS_IN]-(:Block '
					)
				if record_data['level'] == 'block':
					query += (
						'<-[:IS_IN]-(item:Block '
					)
				if record_data['block_uid']:
					parameters['block_uid'] = record_data['block_uid']
					query += (
						' {uid: $block_uid} '
					)
				query += (
					')'
				)
			if record_data['level'] == 'tree':
				if record_data['block_uid']:
					parameters['block_uid'] = record_data['block_uid']
					query += (
						' <-[:IS_IN]-(:BlockTrees) '
					)
				else:
					query += (
						' <-[:IS_IN]-(:FieldTrees) '
					)
				query += (
					' <-[:IS_IN]-(item:Tree) '
				)
			# matched the item, now the condition and value for this in the selected period
			query += (
				' <-[:FOR_ITEM]-(ic:ItemCondition) '
				' -[:FOR_CONDITION*..2]->(condition:Condition'
				'	{name_lower: $condition} '
				' )-[:AT_LEVEL]->(:Level {name_lower: toLower($level)}), '
				' (ic)-[:RECORD_FOR]-(r: Record)  '
				' <-[s: SUBMITTED]-(: UserItemCondition) '
				' <-[: SUBMITTED]-(: UserCondition) '
				' <-[: SUBMITTED]-(: RecordSubmissions) '
				' <-[: SUBMITTED]-(: Submissions) '
				' <-[: SUBMITTED]-(u: User) '
				' WHERE '
				# no conflicts in cases where values match
				'	r.value <> $value '
			)
			# if providing start and/or end for tree.id
			if record_data['level'] == 'tree':
				if record_data['trees_start']:
					parameters['trees_start'] = record_data['trees_start']
					query += (
						' AND item.id >= $trees_start '
					)
				if record_data['trees_end']:
					parameters['trees_end'] = record_data['trees_end']
					query += (
						' AND item.id <= $trees_end '
					)
			# conflicts are only in overlapping time-frames
			if record_data['end_time']:
				query += (
					' AND ( '
					'	( '
					# case where both have start and end and overlap
					'		r.start < $end_time '
					'		AND '
					'		r.end > $start_time '
					'	) '
					'	OR '
					# case when - 
					#   existing entry has no end time 
					#   new record starts after existing one started
					# 
					'	( '
					'		r.end IS NULL '
					'		AND '
					'		r.start <= $start_time '
					'	) '
					'	OR '
					# case when - 
					#   existing entry has no end time
					#   new record does not end before start of existing
					'	( '
					'		r.end IS NULL '
					'		AND '
					'		r.start < $end_time '
					'	) '
					' )'
				)
			else:
				query += (
					' AND ( '
					'	( '
					# case when no end time but start time is between existing start and end  
					'		r.end > $start_time '
					'		AND '
					'		r.start <= $start_time '
					'	) '
					'	OR '
					# case when no end time on either but start time is equal
					'	( '
					'		r.end IS NULL '
					'		AND '
					'		r.start = $start_time '
					'	) '
					' ) '
				)
			query += (
				' RETURN { '
				'	UID: item.uid, '
				'	start: r.start, '
				'	end: r.end, '
				'	condition: condition.name, '
				'	value: r.value, '
				'	submitted_at: s.time, '
				'	user: u.name '
				' } '
			)
			conflicts = {}
			for condition, value in record_data['conditions_dict'].iteritems():
				parameters['condition'] = condition
				parameters['value'] = value
				result = neo4j_session.read_transaction(
					neo4j_query,
					query,
					parameters
				)
				result_list = [record[0] for record in result]
				if result_list:
					conflicts[condition] = result_list
		return conflicts

	def merge_records(self, record_data):
		with get_driver().session() as neo4j_session:
			parameters = {
				'username': self.username,
				'level': record_data['level'],
				'start_time': record_data['start_time'],
				'end_time': record_data['end_time']
			}
			query = (
				' MATCH (:Country '
			)
			if record_data['country']:
				parameters['country'] = record_data['country']
				query += (
					' {name_lower: toLower($country)} ) '
				)
			query += (
				' )<-[:IS_IN]-(:Region '
			)
			if record_data['region']:
				parameters['region'] = record_data['region']
				query += (
					' {name_lower: toLower($region)} '
				)
			query += (
				' )<-[:IS_IN]-(:Farm '
			)
			if record_data['farm']:
				parameters['farm'] = record_data['farm']
				query += (
					' {name_lower: toLower($farm)} '
				)
			if record_data['level'] == 'field':
				query += (
					' )<-[:IS_IN]-(item:Field '
				)
			else: query += (
					' )<-[:IS_IN]-(field:Field '
				)
			if record_data['field_uid']:
				parameters['field_uid'] = record_data['field_uid']
				query += (
					' {uid: $field_uid} '
				)
			query += (
				' ) '
			)
			if any([
				record_data['level'] == 'block',
				all([
					record_data['level'] == 'tree',
					record_data['block_uid']
				])
			]):
				if record_data['level'] == 'tree':
					query += (
						'<-[:IS_IN]-(:Block '
					)
				if record_data['level'] == 'block':
					query += (
						'<-[:IS_IN]-(item:Block '
					)
				if record_data['block_uid']:
					parameters['block_uid'] = record_data['block_uid']
					query += (
						' {uid: $block_uid} '
					)
				query += (
					')'
				)
			if record_data['level'] == 'tree':
				if record_data['block_uid']:
					parameters['block_uid'] = record_data['block_uid']
					query += (
						' <-[:IS_IN]-(:BlockTrees) '
					)
				else:
					query += (
						' <-[:IS_IN]-(:FieldTrees) '
					)
				query += (
					' <-[:IS_IN]-(item:Tree) '
				)
			# if providing start and/or end for tree.id
			if record_data['level'] == 'tree':
				if record_data['trees_start']:
					parameters['trees_start'] = record_data['trees_start']
					query += (
						' WHERE item.id >= $trees_start '
					)
				if record_data['trees_end']:
					parameters['trees_end'] = record_data['trees_end']
					if record_data['trees_start']:

						query += ' AND '
					else:
						query += ' WHERE '
					query += (
						' item.id <= $trees_end '
					)
			query += (
				' MATCH (condition:Condition'
				'	{name_lower: $condition} '
				' )-[:AT_LEVEL]->(:Level {name_lower: toLower($level)}) '
				' MATCH '
				'	(:User '
				'		{ username_lower: toLower($username) } '
				'	)-[:SUBMITTED]->(: Submissions) '
				' 	-[:SUBMITTED]->(us: RecordSubmissions) '
				' MERGE (condition) '
			)
			if record_data['level'] == 'field':
				query += (
					'	<-[:FOR_CONDITION]-(ic: ItemCondition) '
					'	-[:FOR_ITEM]->(item) '
				)
			else:  # record_data['level'] in ['tree', 'block']:
				query += (
					'	<-[:FOR_CONDITION]-(fic:FieldItemCondition) '
					'	-[:FROM_FIELD]->(field) '
					' MERGE '
					'	(fic)<-[:FOR_CONDITION]-(ic: ItemCondition) '
					'	-[:FOR_ITEM]->(item) '
				)
			query += (
				' MERGE (r:Record { '
				'	start: $start_time '
				' })-[:RECORD_FOR]-(ic) '
				' ON CREATE SET '
				'	r.end = $end_time, '
				'	r.value = $value, '
				'	r.found = False '
				' ON MATCH SET '
				'	r.found = True '
				# additional statements to occur when new record
				' FOREACH (n IN CASE '
				'		WHEN r.found = False '
				'			THEN [1] ELSE [] END | '
				# track user submissions through /User/Condition container
				'				MERGE '
				'					(us)-[:SUBMITTED]->(uc:UserCondition) '
				'					-[:CONTRIBUTED]->(condition) '
				# then /UserCondition/ItemCondition container
				'				MERGE '
				'					(uc)-[:SUBMITTED]->(uic:UserItemCondition) '
				'					-[:CONTRIBUTED]->(ic) '
				# then finally the record with a timestamp
				'				MERGE '
				'					(uic)-[s1:SUBMITTED]->(r) '
				'					ON CREATE SET '
				'						s1.time = timestamp() '
				' ) '
				' WITH r, item, condition '
				' MATCH '
				'	(r) '
				'	<-[s: SUBMITTED]-(: UserItemCondition) '
				'	<-[: SUBMITTED]-(: UserCondition) '
				'	<-[: SUBMITTED]-(: RecordSubmissions) '
				'	<-[: SUBMITTED]-(: Submissions) '
				'	<-[: SUBMITTED]-(u: User) '
				# we are merging on start time and not allowing change of value
				# but should allow same user to extend or set an end time for existing records
				# keep a list of previous values and record updates as specific relationship among submissions
				' FOREACH (n in CASE '
				'	WHEN r.found = True '
				'	AND u.username_lower = toLower($username) '
				'	AND ( '
				'		r.end IS NULL '
				'		OR '
				'		r.end < $end_time '
				'	) '
				'	THEN [1] ELSE [] END | '
				'		SET r.end_history = CASE '
				'			WHEN r.end_history IS NULL THEN [] + r.end '
				'			ELSE r.end_history + r.end '
				'			END '
				'		SET r.end = $end_time '
				# track user submissions through /User/Condition container
				'		MERGE '
				'			(us)-[:SUBMITTED]->(uc:UserCondition) '
				'			-[:CONTRIBUTED]->(condition) '
				# then /UserCondition/ItemCondition container
				'		MERGE '
				'			(uc)-[:SUBMITTED]->(uic:UserItemCondition) '
				'			-[:CONTRIBUTED]->(ic) '
				# then finally the record with a timestamp
				'		CREATE '
				'			(uic)-[:UPDATED {time: timestamp()}]->(r) '
				' ) '
				' RETURN { '
				'	UID: item.uid, '
				'	start: r.start,	'
				'	end: r.end, '
				'	condition: condition.name, '
				'	value: r.value, '
				'	found: r.found, '
				'	submitted_at: s.time, '
				'	user: u.name '
				' } '
			)
			merged = {}
			for condition, value in record_data['conditions_dict'].iteritems():
				parameters['condition'] = condition
				parameters['value'] = value
				result = neo4j_session.read_transaction(
					neo4j_query,
					query,
					parameters
				)
				result_list = [record[0] for record in result]
				if result_list:
					merged[condition] = result_list
		return merged

	@staticmethod
	def result_table(result):
		header_string = '<tr><th><p>'
		headers = ['UID', 'Condition', 'Start', 'End', 'Submitted by', 'Submitted at', 'Value']
		header_string += '</p></th><th><p>'.join(headers)
		header_string += '</p></th></tr>'
		for condition, record_list in result.iteritems():
			for record in record_list:
				if record['start']:
					start_time = datetime.datetime.utcfromtimestamp(int(record['start']) / 1000).strftime("%Y-%m-%d")
				else:
					start_time = ""
				if record['end']:
					end_time = datetime.datetime.utcfromtimestamp(int(record['end']) / 1000).strftime("%Y-%m-%d")
				else:
					end_time = ""
				if record['submitted_at']:
					submitted_at = datetime.datetime.utcfromtimestamp(int(record['submitted_at']) / 1000).strftime("%Y-%m-%d %H:%M:%S")
				else:
					submitted_at = ""
				row_string = '<tr><td>'
				row_string += '</td><td>'.join(
					[
						str(record['UID']),
						record['condition'],
						start_time,
						end_time,
						record['user'],
						submitted_at
					]
				)
				if 'found' in record:
					if record['found']:
						row_string += '</td><td bgcolor = #00FF00>'
					else:
						row_string += '</td><td>'
				else:
					row_string += '</td><td>'
				row_string += record['value'] + '</td></tr>'
				header_string += row_string
		return '<table>' + header_string + '<table>'
