from app import ServiceUnavailable, AuthError, TransactionError

from app.cypher import Cypher
from flask import jsonify

from neo4j_driver import get_driver, neo4j_query

import datetime


class Record:
	def __init__(self, username):
		self.username = username
		self.neo4j_session = get_driver().session()

	def ensure_data_submission_node(self):
		parameters = {
			'username': self.username
		}
		match_statement = (
			' MATCH (:User {username_lower: toLower($username)}) '
			'	-[: SUBMITTED]->(: Submissions) '
			'	-[: SUBMITTED]->(uds: DataSubmissions) '
			' RETURN uds '
		)
		merge_statement = (
			' MERGE (:User {username_lower: toLower($username)}) '
			'	-[: SUBMITTED]->(sub: Submissions) '
			' MERGE (sub) '
			'	-[: SUBMITTED]->(: DataSubmissions) '
		)
		if not self.neo4j_session.read_transaction(
			neo4j_query,
			match_statement,
			parameters
		):
			self.neo4j_session.write_transaction(
				neo4j_query,
				merge_statement,
				parameters
			)

	def submit_records(self, record_data):
			self.ensure_data_submission_node()
			record_data['username'] = self.username
			try:
				if record_data['data_type'] == 'condition':
					conflicts_query = self.build_condition_conflicts_query(record_data)
					conflicts = [
						record[0] for record in self.neo4j_session.read_transaction(
							neo4j_query,
							conflicts_query['statement'],
							conflicts_query['parameters']
						)
					]
					if conflicts:
						html_table = self.result_table(conflicts, record_data['data_type'])
						return jsonify({
							'submitted': (
									' Record not submitted. <br><br> '
									' Conflicting values for some of the input records are found in this period: '
									+ html_table
							),
							'class': 'conflicts'
						})
				if record_data['data_type'] == 'trait':
					merge_query = self.build_merge_trait_data_query(record_data)
				else:  # record_data['data_type'] == 'condition':
					merge_query = self.build_merge_condition_record_query(record_data)
				tx = self.neo4j_session.begin_transaction()
				merged = [
					record[0] for record in tx.run(merge_query['statement'], merge_query['parameters'])
				]

				if record_data['data_type'] == 'trait':
					conflicts = []
					for record in merged:
						if record['conflict']:
							conflicts.append(record)
					if conflicts:
						tx.rollback()
						html_table = self.result_table(conflicts, "trait")
						return jsonify({
							'submitted': (
								' Conflicts with existing values: '
								+ html_table
							),
							'class': 'conflicts'
						})
				tx.commit()
				if merged:
					html_table = self.result_table(merged, record_data['data_type'])
					return jsonify({'submitted': (
						' Records submitted or found (highlighted): '
						+ html_table
						)
					})
				else:
					return jsonify({
						'submitted': 'No records submitted, likely no items were found matching your filters',
						'class': 'conflicts'
					})
			except (TransactionError, AuthError, ServiceUnavailable):
				return jsonify({
					'submitted': (
						'An error occurred, please try again later'
					)
				})

	def build_condition_conflicts_query(
			self,
			record_data
	):
		parameters = {
			'level': record_data['level'],
			'country': record_data['country'],
			'region': record_data['region'],
			'farm': record_data['farm'],
			'field_uid': record_data['field_uid'],
			'block_uid': record_data['block_uid'],
			'tree_id_list': record_data['tree_id_list'],
			'start_time': record_data['start_time'],
			'end_time': record_data['end_time'],
			'features_list': record_data['selected_features'],
			'features_dict': record_data['features_dict']
		}
		statement = self.build_match_item_statement(record_data)
		statement += (
			' WITH item '
		)
		if record_data['level'] in ["block","tree"]:
			statement += (
				', field '
			)
		statement += (
			' UNWIND $features_list as feature_name'
			'	MATCH (item) '
			'	<-[:FOR_ITEM]-(ic:ItemCondition) '
			'	-[:FOR_CONDITION*..2]->(condition:Condition'
			'		{name_lower: toLower(feature_name)} '
			'	)-[:AT_LEVEL]->(:Level {name_lower: toLower($level)}), '
			'	(ic)<-[:RECORD_FOR]-(r: Record)  '
			'	<-[s: SUBMITTED]-(: UserItemCondition) '
			'	<-[: SUBMITTED]-(: UserCondition) '
			'	<-[: SUBMITTED]-(: DataSubmissions) '
			'	<-[: SUBMITTED]-(: Submissions) '
			'	<-[: SUBMITTED]-(u: User) '
			'	WHERE '
			# no conflicts in cases where values match
			'		r.value <> $features_dict[feature_name] '
		)
		# allowing open ended timeframes but :
		# defined period can not conflict with defined period
		# OR defined start or defined end
		if all([record_data['start_time'], record_data['end_time']]):
			statement += (
				' AND (( '
				# - any overlapping records
				'	r.start <= $end_time '
				'	AND r.end >= $start_time '
				'	) OR ( '
				# - OR a record that starts in the defined period and has a null end time
				'	r.end IS NULL '
				'	AND r.start <= $start_time '
				'	AND r.end >= $start_time '
				'	) OR ( '
				# - OR a record that ends in the defined period and has a null start time
				'	r.start IS NULL '
				'	AND r.start <= $end_time '
				'	AND r.end >= $end_time '
				' )) '
				# set lock on ItemCondition node and only unlock after merge or result
				# we do a rollback if get results so that this is only set when there are no conflicts 
				# (and the merge proceeds)
				' SET ic._LOCK_ = True '
				' WITH '
				'	r, '
				'	item.uid as UID, '
				'	condition.name as condition, '
				'	s.time as submitted_at, '
				'	u.name as user '
			)
			if record_data['level'] in ["tree", "block"]:
				statement += (
					' , field.uid as field_uid, '
					' item.id as item_id '
				)
		# defined start conflicts if:
		elif record_data['start_time']:
			statement += (
				# - defined start time is in existing defined period
				' AND (( '
				'	r.end >= $start_time '
				'	AND '
				'	r.start <= $start_time '
				'	) OR ( '
				# - OR defined start time is less than a record with defined end and no defined start
				# AND no "closing" statement (another record with start only defined) between these
				#  - this is checked last with optional match to such records and select nulls
				'	r.end >= $start_time '
				'	AND '
				'	r.start IS NULL '
				' )) '
				' OPTIONAL MATCH '
				'	(r)-[:RECORD_FOR]->(ic) '
				'	<-[:RECORD_FOR]-(rr:Record) '
				'	WHERE '
				'		rr.value = r.value '
				'		AND rr.start >= $start_time '
				'		AND rr.start <= r.end '
				'		AND rr.end IS NULL '
				# set lock on ItemCondition node and only unlock after merge or result
				' SET ic._LOCK_ = True '
				' WITH '
				'	r, '
				'	item.uid as UID, '
				'	condition.name as condition,'
				'	s.time as submitted_at, '
				'	u.name as user '
			)
			if record_data['level'] in ["tree","block"]:
				statement += (
					' , field.uid as field_uid, '
					' item.id as item_id '
				)
			statement += (
				' WHERE rr IS NULL '
			)
		# defined end conflicts if:
		elif record_data['end_time']:
			statement += (
				# - defined end time is in existing defined period
				' AND (( '
				'	r.end >= $end_time '
				'	AND '
				'	r.start <= $end_time '
				'	) OR ( '
				# - OR defined end time is greater than a record with defined start and no defined end
				# AND no "closing" statement (another record with end only defined) between these
				#  - this is checked last with optional match to such records and select nulls
				'	r.start <= $end_time '
				'	AND '
				'	r.end IS NULL '
				' )) '				
				' OPTIONAL MATCH '
				'	(r)-[:RECORD_FOR]->(ic) '
				'	<-[:RECORD_FOR]-(rr:Record) '
				'	WHERE '
				'		rr.value = r.value '
				'		AND rr.end <= $end_time '
				'		AND rr.end >= r.start '
				'		AND rr.start IS NULL '
				# set lock on ItemCondition node and only unlock after merge or result
				' SET ic._LOCK_ = True '
				' WITH '
				'	r, '
				'	item.uid as UID, '
				'	condition.name as condition,'
				'	s.time as submitted_at, '
				'	u.name as user '
			)
			if record_data['level'] in ["tree","block"]:
				statement += (
					' , field.uid as field_uid, '
					' item.id as item_id '
				)
			statement += (
				' WHERE rr IS NULL '
			)
		# No defined start or end will only conflict with another record that has no-defined start or end
		elif not any([record_data['start_time'], record_data['end_time']]):
			statement += (
				' AND '
				'	r.start IS NULL '
				' AND '
				'	r.end IS NULL '
				# set lock on ItemCondition node and only unlock after merge or result
				' SET ic._LOCK_ = True '
				' WITH '
				'	r, '
				'	item.uid as UID, '
				'	condition.name as condition, '
				'	s.time as submitted_at, '
				'	u.name as user '
			)
			if record_data['level'] in ["tree","block"]:
				statement += (
					' , field.uid as field_uid, '
					' item.id as item_id '
				)
		statement += (
			' RETURN { '
			'	UID: UID, '
			'	start: r.start, '
			'	end: r.end, '
			'	condition: condition, '
			'	value: r.value, '
			'	submitted_at: submitted_at, '
			'	user: user '
			' } '
			' ORDER BY toLower(condition) '
		)
		if record_data['level'] == 'field':
			statement += (
				' , UID, r.start, r.end '
			)
		else:
			statement += (
				' , field_uid, item_id, r.start, r.end '
			)
		return {
			"statement": statement,
			"parameters": parameters
		}

	@staticmethod
	def build_match_item_statement(record_data):
		parameters = {
			'data_type': record_data['data_type'],
			'level': record_data['level'],
			'time': record_data['record_time'],
			'start_time': record_data['start_time'],
			'end_time': record_data['end_time'],
			'features_list': record_data['selected_features'],
			'features_dict': record_data['features_dict']
		}
		statement = (
			' MATCH (:Country '
		)
		if record_data['country']:
			parameters['country'] = record_data['country']
			statement += (
				' {name_lower: toLower($country)} '
			)
		statement += (
			' )<-[:IS_IN]-(:Region '
		)
		if record_data['region']:
			parameters['region'] = record_data['region']
			statement += (
				' {name_lower: toLower($region)} '
			)
		statement += (
			' )<-[:IS_IN]-(:Farm '
		)
		if record_data['farm']:
			parameters['farm'] = record_data['farm']
			statement += (
				' {name_lower: toLower($farm)} '
			)
		if record_data['level'] == 'field':
			statement += (
				' )<-[:IS_IN]-(item:Field '
			)
		else:
			statement += (
				' )<-[:IS_IN]-(field:Field '
			)
		if record_data['field_uid']:
			parameters['field_uid'] = record_data['field_uid']
			statement += (
				' {uid: $field_uid} '
			)
		statement += (
			' ) '
		)
		if any([
			record_data['level'] == 'block',
			all([
				record_data['level'] == 'tree',
				record_data['block_uid']
			])
		]):
			statement += (
				' <-[:IS_IN]-(:FieldBlocks) '
			)
			if record_data['level'] == 'tree':
				statement += (
					'<-[:IS_IN]-(:Block '
				)
			if record_data['level'] == 'block':
				statement += (
					'<-[:IS_IN]-(item:Block '
				)
			if record_data['block_uid']:
				parameters['block_uid'] = record_data['block_uid']
				statement += (
					' {uid: $block_uid} '
				)
			statement += (
				')'
			)
		if record_data['level'] == 'tree':
			if record_data['block_uid']:
				parameters['block_uid'] = record_data['block_uid']
				statement += (
					' <-[:IS_IN]-(:BlockTrees) '
				)
			else:
				statement += (
					' <-[:IS_IN]-(:FieldTrees) '
				)
			statement += (
				' <-[:IS_IN]-(item:Tree) '
			)
		if record_data['tree_id_list']:
			parameters['tree_id_list'] = record_data['tree_id_list']
			statement += (
				' WHERE '
				' item.id in $tree_id_list '
			)
		return statement

	def build_merge_trait_data_query(self, record_data):
		parameters = {
			'username': self.username,
			'data_type': record_data['data_type'],
			'level': record_data['level'],
			'country': record_data['country'],
			'region': record_data['region'],
			'farm': record_data['farm'],
			'field_uid': record_data['field_uid'],
			'block_uid': record_data['block_uid'],
			'tree_id_list': record_data['tree_id_list'],
			'time': record_data['record_time'],
			'features_list': record_data['selected_features'],
			'features_dict': record_data['features_dict']
		}
		statement = self.build_match_item_statement(record_data)
		statement += (
			' UNWIND $features_list as trait_name '
			'	MATCH (trait: Trait '
			'		{name_lower: toLower(trait_name)} '
			'	)-[:AT_LEVEL]->(:Level {name_lower: toLower($level)}) '
			'	MATCH '
			'		(:User '
			'			{ username_lower: toLower($username) } '
			'		)-[:SUBMITTED]->(: Submissions) '
			'		-[:SUBMITTED]->(us: DataSubmissions) '
			'	MERGE (trait) '
		)

		if record_data['level'] == 'field':
			statement += (
				'	<-[:FOR_TRAIT]-(it: ItemTrait) '
				'	-[:FOR_ITEM]->(item) '
			)
		elif record_data['level'] in ['tree', 'block']:
			statement += (
				'	<-[:FOR_TRAIT]-(fit:FieldItemTrait) '
				'	-[:FROM_FIELD]->(field) '
				' MERGE '
				'	(fit)<-[:FOR_CONDITION]-(it: ItemTrait) '
				'	-[:FOR_ITEM]->(item) '
			)
		statement += (
			'	MERGE (d: Data { '
			'		time: $time '
			'	})-[:DATA_FOR]->(it) '
		)
		statement += (
			' ON CREATE SET '
			'	d.found = False, '
			'	d.value = $features_dict[trait_name] '
			' ON MATCH SET '
			'	d.found = True '
			# unlock ItemCondition node, this is set to true to obtain lock in the conflict query
			' SET it._LOCK_ = False '
			# additional statements to occur when new record
			' FOREACH (n IN CASE '
			'		WHEN d.found = False '
			'			THEN [1] ELSE [] END | '
			# track user submissions through /User/Condition container
			'				MERGE '
			'					(us)-[:SUBMITTED]->(ut:UserTrait) '
			'					-[:CONTRIBUTED]->(trait) '
			# then /UserCondition/ItemCondition container
			'				MERGE '
			'					(ut)-[:SUBMITTED]->(uit:UserItemTrait) '
			'					-[:CONTRIBUTED]->(it) '
			# then the record with a timestamp
			'				MERGE '
			'					(uit)-[s1:SUBMITTED]->(d) '
			'					ON CREATE SET '
			'						s1.time = timestamp() '
			' ) '
			' WITH '
			'	d, it, trait, '
			'	item.uid as item_uid '
		)
		if record_data['level'] in ["block", "tree"]:
			statement += (
				' , '
				' field.uid as field_uid, '
				' item.id as item_id '
			)
		statement += (
			' MATCH '
			'	(d) '
			'	<-[s: SUBMITTED]-(: UserItemTrait) '
			'	<-[: SUBMITTED]-(: UserTrait) '
			'	<-[: SUBMITTED]-(: DataSubmissions) '
			'	<-[: SUBMITTED]-(: Submissions) '
			'	<-[: SUBMITTED]-(u: User) '
			' OPTIONAL MATCH '
			'	(u)-[: AFFILIATED {data_shared: True}]->(p: Partner) '
			' OPTIONAL MATCH '
			'	(p)<-[a: AFFILIATED]-(: User {username_lower: toLower($username)}) '
			' RETURN { '
			'	UID: item_uid, '
			'	time: d.time,	'
			'	trait: trait.name, '
			'	value: CASE '
			'		WHEN d.found = False '
			'			THEN '
			'				d.value '
			'		WHEN a.confirmed = True '
			'			THEN '
			'				d.value '
			'		ELSE '
			'			"Affiliation required: " + p.name '
			'		END, '
			'	conflict: CASE'
			'		WHEN d.value = $features_dict[trait.name_lower] '
			'			THEN '
			'				False '
			'		ELSE '
			'			True '
			'		END, '
			'	found: d.found, '
			'	submitted_at: s.time, '
			'	user: CASE '
			'		WHEN a.confirmed = True '
			'			THEN u.name '
			'		ELSE '
			'			p.name '
			'		END '
			' } '
			' ORDER BY trait.name_lower '
		)
		if record_data['level'] == 'field':
			statement += (
				' , item_uid, d.time '
			)
		else:
			statement += (
				' , field_uid, item_id, d.time '
			)
		return {
			'statement': statement,
			'parameters': parameters
		}

	def build_merge_condition_record_query(self, record_data):
		parameters = {
			'username': self.username,
			'data_type': record_data['data_type'],
			'level': record_data['level'],
			'country': record_data['country'],
			'region': record_data['region'],
			'farm': record_data['farm'],
			'field_uid': record_data['field_uid'],
			'block_uid': record_data['block_uid'],
			'tree_id_list': record_data['tree_id_list'],
			'start_time': record_data['start_time'],
			'end_time': record_data['end_time'],
			'features_list': record_data['selected_features'],
			'features_dict': record_data['features_dict']
		}
		statement = self.build_match_item_statement(record_data)
		statement += (
			' UNWIND $features_list as condition_name '
			'	MATCH (condition: Condition '
			'		{name_lower: toLower(condition_name)} '
			'	)-[:AT_LEVEL]->(:Level {name_lower: toLower($level)}) '
			'	MATCH '
			'		(:User '
			'			{ username_lower: toLower($username) } '
			'		)-[:SUBMITTED]->(: Submissions) '
			'		-[:SUBMITTED]->(us: DataSubmissions) '
			'	MERGE (condition) '
		)

		if record_data['level'] == 'field':
			statement += (
				'	<-[:FOR_CONDITION]-(ic: ItemCondition) '
				'	-[:FOR_ITEM]->(item) '
			)
		elif record_data['level'] in ['tree', 'block']:
			statement += (
				'	<-[:FOR_CONDITION]-(fic: FieldItemCondition) '
				'	-[:FROM_FIELD]->(field) '
				' MERGE '
				'	(fic)<-[:FOR_CONDITION]-(ic: ItemCondition) '
				'	-[:FOR_ITEM]->(item) '
			)
		statement += (
			'	MERGE (r: Record {'
		)
		if record_data['start_time']:
			statement += (
				'	start: $start_time, '
			)
		if record_data['end_time']:
			statement += (
				'	end: $end_time, '
			)
		statement += (
			'	value: $features_dict[condition_name] '
			' })-[:RECORD_FOR]->(ic) '
			' ON CREATE SET '
			'	r.found = False '
			' ON MATCH SET '
			'	r.found = True '
			# unlock ItemCondition node, this is set to true to obtain lock in the conflict query
			' SET ic._LOCK_ = False '
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
			# then the record with a timestamp
			'				MERGE '
			'					(uic)-[s1:SUBMITTED]->(r) '
			'					ON CREATE SET '
			'						s1.time = timestamp() '
			' ) '
			' WITH '
			'	r, ic, condition, '
			'	item.uid as item_uid '
		)
		if record_data['level'] in ["block", "tree"]:
			statement += (
				' , '
				' field.uid as field_uid, '
				' item.id as item_id '
			)
		statement += (
			' MATCH '
			'	(r) '
			'	<-[s: SUBMITTED]-(: UserItemCondition) '
			'	<-[: SUBMITTED]-(: UserCondition) '
			'	<-[: SUBMITTED]-(: DataSubmissions) '
			'	<-[: SUBMITTED]-(: Submissions) '
			'	<-[: SUBMITTED]-(u: User) '
			' RETURN { '
			'	UID: item_uid, '
			'	start: r.start,	'
			'	end: r.end, '
			'	condition: condition.name, '
			'	value: r.value, '
			'	found: r.found, '
			'	submitted_at: s.time, '
			'	user: u.name '
			' } '
			' ORDER BY condition.name_lower '
		)
		if record_data['level'] == 'field':
			statement += (
				' , item_uid, r.start, r.end '
			)
		else:
			statement += (
				' , field_uid, item_id, r. start, r.end '
			)
		return {
			'statement': statement,
			'parameters': parameters
		}

	@staticmethod
	def result_table(result_list, data_type):
		header_string = '<tr><th><p>'
		if data_type == 'trait':
			headers = ['UID', 'Trait', 'Time', 'Submitted by', 'Submitted at', 'Value']
		else:  # data_type == 'condition'
			headers = ['UID', 'Condition', 'Start', 'End', 'Submitted by', 'Submitted at', 'Value']
		header_string += '</p></th><th><p>'.join(headers)
		header_string += '</p></th></tr>'
		for record in result_list:
			if record['submitted_at']:
				submitted_at = datetime.datetime.utcfromtimestamp(int(record['submitted_at']) / 1000).strftime("%Y-%m-%d %H:%M:%S")
			else:
				submitted_at = ""
			row_string = '<tr><td>'
			if data_type == 'condition':
				if record['start']:
					start_time = datetime.datetime.utcfromtimestamp(int(record['start']) / 1000).strftime("%Y-%m-%d")
				else:
					start_time = ""
				if record['end']:
					end_time = datetime.datetime.utcfromtimestamp(int(record['end']) / 1000).strftime("%Y-%m-%d")
				else:
					end_time = ""
				row_data = [
						str(record['UID']),
						record['condition'],
						start_time,
						end_time,
						record['user'],
						submitted_at
					]
			else:  # data_type == 'trait':
				if record['time']:
					time = datetime.datetime.utcfromtimestamp(int(record['time']) / 1000).strftime("%Y-%m-%d")
				else:
					time = ""
				row_data = [
						str(record['UID']),
						record['trait'],
						time,
						record['user'],
						submitted_at
					]
			row_string += '</td><td>'.join(row_data)
			# if record was found then highlight the value
			if 'found' in record and record['found']:
				if 'conflict' in record and record['conflict']:
					row_string += '</td><td bgcolor = #FF0000>'
				else:
					row_string += '</td><td bgcolor = #00FF00>'
			else:
				row_string += '</td><td>'
			row_string += record['value'] + '</td></tr>'
			header_string += row_string
		return '<table>' + header_string + '<table>'
