from app import ServiceUnavailable, AuthError, TransactionError

from app.models import ItemList

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
			if record_data['record_type'] == 'condition':
				# for condition data we first query for conflicts since there are many types of conflict
				# for traits we don't need this as it is simpler to perform during the merger
				#    - traits can only have a single value at a single time
				#    - so only conflict with different value at that time
				conflicts_query = self.build_condition_conflicts_query(record_data)
				conflicts = [
					record[0] for record in self.neo4j_session.read_transaction(
						neo4j_query,
						conflicts_query['statement'],
						conflicts_query['parameters']
					)
				]
				if conflicts:
					html_table = self.result_table(conflicts, record_data['record_type'])
					return jsonify({
						'submitted': (
								' Record not submitted. <br><br> '
								' Either the value has been set by another partner and is not visible to you'
								' or the value you are trying to submit conflicts with an existing entry: '
								+ html_table
						),
						'class': 'conflicts'
					})
			if record_data['record_type'] == 'trait':
				merge_query = self.build_merge_trait_data_query(record_data)
			else:  # record_data['record_type'] == 'condition':
				merge_query = self.build_merge_condition_record_query(record_data)
			tx = self.neo4j_session.begin_transaction()
			merged = [
				record[0] for record in tx.run(merge_query['statement'], merge_query['parameters'])
			]
			if record_data['record_type'] == 'trait':
				conflicts = []
				for record in merged:
					if record['found']:
						if not record['access']:
							conflicts.append(record)
						elif record['conflict']:
							conflicts.append(record)
				if conflicts:
					tx.rollback()
					html_table = self.result_table(conflicts, "trait")
					return jsonify({
						'submitted': (
							' Existing values found that are either in conflict with the submitted value '
							' or are not accessible to you. Consider contacting this partner to request access. '
							+ html_table
						),
						'class': 'conflicts'
					})
			tx.commit()
			if merged:
				html_table = self.result_table(merged, record_data['record_type'])
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

	def build_merge_trait_data_query(self, record_data):
		parameters = {
			'username': self.username,
			'record_type': record_data['record_type'],
			'item_level': record_data['item_level'],
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
		statement = ItemList.build_match_item_statement(record_data)[0]
		statement += (
			' UNWIND $features_list as feature_name '
			'	MATCH '
			'		(:RecordType {name_lower:toLower($record_type)})'
			'		<-[:OF_TYPE]-(feature: Feature '
			'			{name_lower: toLower(feature_name)} '
			'		)-[:AT_LEVEL]->(:ItemLevel {name_lower: toLower($item_level)}) '
			'	MATCH '
			'		(:User '
			'			{ username_lower: toLower($username) } '
			'		)-[:SUBMITTED]->(: Submissions) '
			'		-[:SUBMITTED]->(us: DataSubmissions) '
			'	MERGE (feature) '
		)
		if record_data['item_level'] == 'field':
			statement += (
				'	<-[:FOR_FEATURE]-(if: ItemFeature) '
				'	-[:FOR_ITEM]->(item) '
			)
		else:
			statement += (
				'	<-[:FOR_FEATURE]-(ff:FieldFeature) '
				'	-[:FROM_FIELD]->(field) '
				' MERGE '
				'	(ff) '
				'	<-[:FOR_FEATURE]-(it: ItemFeature) '
				'	-[:FOR_ITEM]->(item) '
				)
		statement += (
			'	MERGE '
			'		(it) '
			'		<-[:RECORD_FOR]-(r: Record { '
			'			time: $time '
			'		})'
		)
		statement += (
			' ON CREATE SET '
			'	r.found = False, '
			'	r.value = $features_dict[feature_name] '
			' ON MATCH SET '
			'	r.found = True '
			# additional statements to occur when new record
			' FOREACH (n IN CASE '
			'		WHEN r.found = False '
			'			THEN [1] ELSE [] END | '
			# track user submissions through /User/Field/Feature container
			'				MERGE '
			'					(us)-[:SUBMITTED]->(uff:UserFieldFeature) '
			'					-[:CONTRIBUTED]->(ff) '
			# then the record with a timestamp
			'				CREATE '
			'					(uff)-[s1:SUBMITTED {time: timestamp()}]->(r) '
			' ) '
			' WITH '
			'	r, feature, '
			'	item.uid as item_uid '
		)
		if record_data['item_level'] in ["block", "tree", "sample"]:
			statement += (
				' , '
				' field.uid as field_uid, '
				' item.id as item_id '
			)
		statement += (
			' MATCH '
			'	(r) '
			'	<-[s: SUBMITTED]-(: UserFieldFeature) '
			'	<-[: SUBMITTED]-(: DataSubmissions) '
			'	<-[: SUBMITTED]-(: Submissions) '
			'	<-[: SUBMITTED]-(u: User) '
			' OPTIONAL MATCH '
			'	(u)-[: AFFILIATED {data_shared: True}]->(p: Partner) '
			' OPTIONAL MATCH '
			'	(p)<-[a: AFFILIATED]-(: User {username_lower: toLower($username)}) '
			' RETURN { '
			'	UID: item_uid, '
			'	time: r.time,	'
			'	feature: feature.name, '
			'	value: '
			'		CASE '
			# returning value if just submitted, regardless of access
			'			WHEN r.found '
			'			THEN CASE '
			'				WHEN a.confirmed '
			'				THEN r.value '
			'				ELSE "ACCESS RESTRICTED" '
			'				END '
			'			ELSE r.value '
			'			END, '
			'	access: a.confirmed, '
			'	conflict: '
			'		CASE '
			'			WHEN r.value = $features_dict[feature.name_lower] '
			'			THEN False '
			'			ELSE True '
			'			END, '
			'	found: r.found, '
			'	submitted_at: s.time, '
			'	user: CASE '
			'		WHEN a.confirmed = True '
			'			THEN u.name '
			'		ELSE '
			'			p.name '
			'		END '
			' } '
			' ORDER BY feature.name_lower '
		)
		if record_data['item_level'] == 'field':
			statement += (
				' , item_uid, r.time '
			)
		else:
			statement += (
				' , field_uid, item_id, r.time '
			)
		return {
			'statement': statement,
			'parameters': parameters
		}

	def build_condition_conflicts_query(
			self,
			record_data
	):
		parameters = {
			'username': self.username,
			'record_type': record_data['record_type'],
			'item_level': record_data['item_level'],
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
		statement = ItemList.build_match_item_statement(record_data)[0]
		statement += (
			' WITH item '
		)
		if record_data['item_level'] != 'field':
			statement += (
				', field '
			)
		statement += (
			' UNWIND $features_list as feature_name'
			'	MATCH (item) '
			'	<-[:FOR_ITEM]-(if: ItemFeature) '
			'	-[:FOR_FEATURE*2..]->(feature:Feature'
			'		{name_lower: toLower(feature_name)} '
			'	), '
			'	(if) '
			'	<-[:RECORD_FOR]-(r) '
			'	<-[s: SUBMITTED]-(: UserFieldFeature) '
			'	<-[: SUBMITTED]-(: DataSubmissions) '
			'	<-[: SUBMITTED]-(: Submissions) '
			'	<-[: SUBMITTED]-(u: User) '
			'	-[:AFFILIATED {data_shared: true}]->(p:Partner) '
			'	OPTIONAL MATCH '
			'		(p)<-[: AFFILIATED {confirmed: true}]-(cu: User {username_lower: toLower($username)}) '
			' WITH '
			'	item, feature, if, r, s, u, p, cu '
		)
		if record_data['item_level'] != "field":
			statement += (
				', field '
			)
		statement += (
			'	WHERE '
			# If don't have access or if have access and values don't match then potential conflict 
			# time parsing to allow various degrees of specificity in the relevant time range is below
			'		( '
			'			cu IS NULL '
			'			OR '
			'			r.value <> $features_dict[feature_name] '
			'		) '
		)
		# allowing open ended time-frames but :
		# defined period can not conflict with defined period
		# OR defined start or defined end
		#
		# Here False is stored for undefined instead of the Null value to facilitate specific mergers
		# (see build_merge_condition_record_query)
		# So we have to perform case statement checks on the value before comparing
		#
		if all([record_data['start_time'], record_data['end_time']]):
			statement += (
				' AND (( '
				# - any overlapping records
				'	CASE WHEN r.start <> False THEN r.start ELSE Null END <= $end_time '
				'	AND '
				'	CASE WHEN r.end <> False THEN r.end ELSE Null END >= $start_time '
				'	) OR ( '
				# - OR a record that starts in the defined period and has a False end time
				'	r.end = False'
				'	AND '
				'	CASE WHEN r.start <> False THEN r.start ELSE Null END <= $start_time '
				'	AND '
				'	CASE WHEN r.end <> False THEN r.end ELSE Null END >= $start_time '
				'	) OR ( '
				# - OR a record that ends in the defined period and has a null start time
				'	r.start = False '
				'	AND '
				'	CASE WHEN r.start <> False THEN r.start ELSE Null END <= $end_time '
				'	AND '
				'	CASE WHEN r.end <> False THEN r.end ELSE Null END >= $end_time '
				' )) '
				# set lock on ItemCondition node and only unlock after merge or result
				# we do a rollback if get results so that this is only set when there are no conflicts 
				# (and the merge proceeds)
				' SET if._LOCK_ = True '
				' WITH '
				'	r, '
				'	p.name as partner, '
				'	CASE WHEN cu IS NULL THEN False ELSE True END as access, '
				'	item.uid as UID, '
				'	feature.name as feature, '
				'	s.time as submitted_at, '
				'	u.name as user '
			)
			if record_data['item_level'] != 'field':
				statement += (
					' , field.uid as field_uid, '
					' item.id as item_id '
				)
		# defined start conflicts if:
		elif record_data['start_time']:
			statement += (
				# - defined start time is in existing defined period
				' AND (( '
				'	CASE WHEN r.end <> False THEN r.end ELSE Null END >= $start_time '
				'	AND '
				'	CASE WHEN r.start <> False THEN r.start ELSE Null END <= $start_time '
				# Or start time is the same as an existing record with no end time
				'	) OR ( '
				'	CASE WHEN r.start <> FALSE THEN r.start ELSE Null END = $start_time '
				'	AND '
				'	r.end = False '
				'	) OR ( '
				# - OR defined start time is less than a record with defined end and no defined start
				# AND no "closing" statement (another record with start only defined) between these
				#  - this is checked last with optional match to such records and select nulls
				'	r.start = False '
				'	AND '
				'	CASE WHEN r.end <> False THEN r.end ELSE Null END >= $start_time '
				' )) '
				' WITH '
				'	item, if, feature, r, s, u, p, cu '
			)
			if record_data['item_level'] != 'field':
				statement += (
					', field '
				)
			statement += (
				' OPTIONAL MATCH '
				'	(r)-[:RECORD_FOR]->(if) '
				'	<-[:RECORD_FOR]-(rr:Record) '
				'	WHERE '
				'		rr.end = False '
				'		AND '
				'		rr.value = r.value '
				'		AND '
				'		CASE WHEN rr.start <> False THEN rr.start ELSE Null END >= $start_time '
				'		AND '
				'		CASE WHEN rr.start <> NULL THEN rr.start ELSE Null END <= r.end '
				# set lock on ItemFeature node and only unlock after merge or result
				' SET if._LOCK_ = True '
				' WITH '
				'	r, '
				'	p.name as partner, '
				'	CASE WHEN cu IS NULL THEN False ELSE True END as access, '
				'	item.uid as UID, '
				'	feature.name as feature,'
				'	s.time as submitted_at, '
				'	u.name as user '
			)
			if record_data['item_level'] != 'field':
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
				'	CASE WHEN r.end <> False THEN r.end ELSE Null END >= $end_time '
				'	AND '
				'	CASE WHEN r.start <> False THEN r.start ELSE Null END <= $end_time '
				# Or end time time is the same as an existing record with no start time
				'	) OR ( '
				'	CASE WHEN r.end <> FALSE THEN r.end ELSE Null END = $end_time '
				'	AND '
				'	r.start = False '
				'	) OR ( '
				# - OR defined end time is greater than a record with defined start and no defined end
				# AND no "closing" statement (another record with end only defined) between these
				#  - this is checked last with optional match to such records and select nulls
				'	r.end = False '
				'	AND '
				'	CASE WHEN r.start <> False THEN r.start ELSE Null END <= $end_time '
				' )) '
				' WITH '
				'	item, if, feature, r, s, u, p, cu '
			)
			if record_data['item_level'] != 'field':
				statement += (
					', field '
				)
			statement += (
				' OPTIONAL MATCH '
				'	(r)-[:RECORD_FOR]->(if) '
				'	<-[:RECORD_FOR]-(rr:Record) '
				'	WHERE '
				'		rr.value = r.value '
				'		AND '
				'		CASE WHEN rr.end <> False THEN rr.end ELSE Null END <= $end_time '
				'		AND '
				'		CASE WHEN rr.end <> False THEN rr.end ELSE Null END '
				'			>= '
				'		CASE WHEN r.start <> False THEN r.start ELSE Null END '
				'		AND '
				'		rr.start = False '
				# set lock on ItemCondition node and only unlock after merge or result
				' SET if._LOCK_ = True '
				' WITH '
				'	r, '
				'	p.name as partner, '
				'	CASE WHEN cu IS NULL THEN False ELSE True END as access, '
				'	item.uid as UID, '
				'	feature.name as feature,'
				'	s.time as submitted_at, '
				'	u.name as user '
			)
			if record_data['item_level'] != 'field':
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
				'	r.start = False '
				' AND '
				'	r.end = False '
				# set lock on ItemCondition node and only unlock after merge or result
				' SET if._LOCK_ = True '
				' WITH '
				'	r, '
				'	p.name as partner, '
				'	CASE WHEN cu IS NULL THEN False ELSE True END as access, '
				'	item.uid as UID, '
				'	feature.name as feature, '
				'	s.time as submitted_at, '
				'	u.name as user '
			)
			if record_data['item_level'] != 'field':
				statement += (
					' , field.uid as field_uid, '
					' item.id as item_id '
				)
		statement += (
			' RETURN { '
			'	UID: UID, '
			'	start: CASE WHEN r.start <> False THEN r.start ELSE Null END, '
			'	end: CASE WHEN r.end <> False THEN r.end ELSE Null END, '
			'	feature: feature, '
			'	value: CASE WHEN access THEN r.value ELSE "ACCESS DENIED" END, '
			'	submitted_at: submitted_at, '
			'	user: CASE WHEN access THEN user ELSE partner END, '
			'	access: access '
			' } '
			' ORDER BY toLower(feature) '
		)
		if record_data['item_level'] == 'field':
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

	def build_merge_condition_record_query(self, record_data):
		parameters = {
			'username': self.username,
			'record_type': record_data['record_type'],
			'item_level': record_data['item_level'],
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
		statement = ItemList.build_match_item_statement(record_data)[0]
		statement += (
			' UNWIND $features_list as feature_name '
			'	MATCH '
			'		(:RecordType {name_lower: toLower($record_type)})'
			'		<-[:OF_TYPE]-(feature: Feature '
			'			{name_lower: toLower(feature_name)} '
			'		)-[:AT_LEVEL]->(:ItemLevel {name_lower: toLower($item_level)}) '
			'	MATCH '
			'		(:User '
			'			{ username_lower: toLower($username) } '
			'		)-[:SUBMITTED]->(: Submissions) '
			'		-[:SUBMITTED]->(us: DataSubmissions) '
			'	MERGE (feature) '
		)

		if record_data['item_level'] == 'field':
			statement += (
				'	<-[:FOR_FEATURE]-(if: ItemFeature) '
				'	-[:FOR_ITEM]->(item) '
			)
		else:
			statement += (
				'	<-[:FOR_FEATURE]-(ff: FieldFeature) '
				'	-[:FROM_FIELD]->(field) '
				' MERGE '
				'	(ff) '
				'	<-[:FOR_FEATURE]-(if: ItemFeature) '
				'	-[:FOR_ITEM]->(item) '
			)
		# When merging, if the merger properties agree between input and existing then no new node will be created,
		#  even if other properties in existing are not specified in the input.
		# This means Null is not suitable as a value for "not-specified" start time
		# So we coalesce the value and the boolean False, but it means we have to check for this False value
		# in all comparisons...e.g. in the condition conflict query
		statement += (
			' MERGE '
			'	(r: Record { '
			'		start: COALESCE($start_time, False), '
			'		end: COALESCE($end_time, False), '
			'		value: $features_dict[feature_name] '
			'	})-[:RECORD_FOR]->(if) '
			' ON CREATE SET '
			'	r.found = False '
			' ON MATCH SET '
			'	r.found = True '
			# unlock ItemCondition node, this is set to true to obtain lock in the conflict query
			' SET if._LOCK_ = False '
			# additional statements to occur when new record
			' FOREACH (n IN CASE '
			'		WHEN r.found = False '
			'			THEN [1] ELSE [] END | '
			# track user submissions through /User/Feature container
			'				MERGE '
			'					(us)-[:SUBMITTED]->(uff:UserFieldFeature) '
			'					-[:CONTRIBUTED]->(ff) '
			# then the record with a timestamp
			'				CREATE '
			'					(uff)-[s1:SUBMITTED {time: timestamp()}]->(r) '
			' ) '
			' WITH '
			'	r, feature, '
			'	item.uid as item_uid '
		)
		if record_data['item_level'] != 'field':
			statement += (
				' , '
				' field.uid as field_uid, '
				' item.id as item_id '
			)
		statement += (
			' MATCH '
			'	(r) '
			'	<-[s: SUBMITTED]-(: UserFieldFeature) '
			'	<-[: SUBMITTED]-(: DataSubmissions) '
			'	<-[: SUBMITTED]-(: Submissions) '
			'	<-[: SUBMITTED]-(u: User) '
			' RETURN { '
			'	UID: item_uid, '
			'	start: CASE WHEN r.start <> False THEN r.start ELSE Null END,	'
			'	end: CASE WHEN r.end <> False THEN r.end ELSE Null END, '
			'	feature: feature.name, '
			'	value: r.value, '
			'	found: r.found, '
			'	submitted_at: s.time, '
			'	user: u.name '
			' } '
			' ORDER BY feature.name_lower '
		)
		if record_data['item_level'] == 'field':
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
	def result_table(result_list, record_type):
		header_string = '<tr><th><p>'
		if record_type == 'trait':
			headers = ['UID', 'Feature', 'Time', 'Submitted by', 'Submitted at', 'Value']
		else:  # record_type == 'condition'
			headers = ['UID', 'Feature', 'Start', 'End', 'Submitted by', 'Submitted at', 'Value']
		header_string += '</p></th><th><p>'.join(headers)
		header_string += '</p></th></tr>'
		for record in result_list:
			if record['submitted_at']:
				submitted_at = datetime.datetime.utcfromtimestamp(int(record['submitted_at']) / 1000).strftime("%Y-%m-%d %H:%M:%S")
			else:
				submitted_at = ""
			row_string = '<tr><td>'
			if record_type == 'condition':
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
						record['feature'],
						start_time,
						end_time,
						record['user'],
						submitted_at
					]
			else:  # record_type == 'trait':
				if record['time']:
					time = datetime.datetime.utcfromtimestamp(int(record['time']) / 1000).strftime("%Y-%m-%d")

				else:
					time = ""
				row_data = [
						str(record['UID']),
						record['feature'],
						time,
						record['user'],
						submitted_at
					]
			row_string += '</td><td>'.join(row_data)
			# if existing record then we highlight it, colour depends on value
			# only do the highlighting if have access to the data
			if 'access' in record and record['access']:
				if 'found' in record and record['found']:
					if 'conflict' in record and not record['conflict']:
						row_string += '</td><td bgcolor = #00FF00>'
					else:
						row_string += '</td><td bgcolor = #FF0000>'
				else:
					# found not returned in conflicts query (all records are "found")
					if 'found' not in record:
						row_string += '</td><td bgcolor = #FF0000>'
					else:
						row_string += '</td><td>'
			else:
				# result of condition merger doesn't report access as all records are known to have access
				# due to prior conflicts query, these
				if 'access' not in record and record['found']:
					row_string += '</td><td bgcolor = #00FF00>'
				else:
					row_string += '</td><td>'
			row_string += record['value'] + '</td></tr>'
			header_string += row_string
		return '<table>' + header_string + '<table>'

