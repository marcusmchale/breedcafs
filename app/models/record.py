from app import ServiceUnavailable, SecurityError, TransactionError

from app.models import ItemList

from app.cypher import Cypher

from flask import jsonify

from neo4j_driver import get_driver, neo4j_query

import datetime


class Record:
	def __init__(self, username):
		self.username = username
		self.neo4j_session = get_driver().session()

	def submit_records(self, record_data):
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
			elif record_data['record_type'] == 'condition':
				merge_query = self.build_merge_condition_record_query(record_data)
			elif record_data['record_type'] == 'property':
				merge_query = self.build_merge_property_value_query(record_data)
			else:
				return jsonify({
					'submitted': (
						'This record type is not yet defined, please contact an administrator'
					)
				})

			tx = self.neo4j_session.begin_transaction()
			# if any updates to perform:
			if bool(
					{
						'variety name',
						'variety code',
						'custom id',
						'assign to block',
						'assign to trees'
					} & set(record_data['selected_features'])
			):
				match_item_query = ItemList.build_match_item_statement(record_data)
				if bool(
						{'variety name', 'variety code'} & set(record_data['selected_features'])
				):
					update_variety_statement = match_item_query[0] + ' WITH DISTINCT item '
					if record_data['item_level'] != 'field':
						update_variety_statement += ', field '
					update_variety_parameters = match_item_query[1]
					update_variety_parameters['username'] = record_data['username']
					if 'variety name' in record_data['selected_features']:
						update_variety_parameters['variety_name'] = record_data['features_dict']['variety name']
						update_variety_statement += ' MATCH (update_variety:Variety {name_lower: $variety_name}) '
					else:  # 'variety code' in record_data['selected_features']:
						update_variety_parameters['variety_code'] = record_data['features_dict']['variety code']
						update_variety_statement += ' MATCH (update_variety:Variety {code: $variety_code}) '
					update_variety_statement += (
						' OPTIONAL MATCH '
						'	(item)'
						'	-[of_current_variety: OF_VARIETY]->(:FieldVariety) '
						' OPTIONAL MATCH '
						'	(item)-[:FROM]->(source_sample:Sample) '
						' WITH item, update_variety '
						'	WHERE of_current_variety IS NULL '
						'	AND source_sample IS NULL '
					)
					if record_data['item_level'] == 'field':
						update_variety_statement += (
							' MERGE (item)-[:CONTAINS_VARIETY]->(fv:FieldVariety)-[:OF_VARIETY]->(update_variety) '
						)
					else:
						update_variety_statement += (
							' MERGE (field)-[:CONTAINS_VARIETY]->(fv:FieldVariety)-[:OF_VARIETY]->(update_variety) '
						)
					update_variety_statement += (
						' MERGE (item)-[s1:OF_VARIETY]->(fv) '
						'	ON CREATE SET '
						'		s1.time = timestamp(), '
						'		s1.username = $username '
					)
					tx.run(update_variety_statement, update_variety_parameters)
				if 'assign to block' in record_data['selected_features']:
					update_block_statement = match_item_query[0]
					update_block_parameters = match_item_query[1]
					update_block_parameters['username'] = record_data['username']
					update_block_parameters['assign_to_block'] = int(record_data['features_dict']['assign to block'])
					update_block_statement += (
						' WITH DISTINCT item, field '
						' MATCH '
						'	(block_update: Block {id: $assign_to_block})-[:IS_IN]->(:FieldBlocks)-[:IS_IN]->(field) '
						' OPTIONAL MATCH '
						'	(item)-[:IS_IN]->(bt: BlockTrees) '
						' WITH '
						'	item, block_update '
						' WHERE bt IS NULL '
						' MERGE '
						' 	(block_trees_update: BlockTrees)-[:IS_IN]-> '
						' 	(block_update) '
						' MERGE '
						' 	(block_tree_counter_update: Counter { '
						' 		name: "tree", '
						' 		uid: (block_update.uid + "_tree") '
						' 	})-[:FOR]->(block_trees_update) '
						' 	ON CREATE SET '
						' 	block_tree_counter_update.count = 0 '
						' MERGE '
						' 	(item)-[s1:IS_IN]->(block_trees_update) '
						' ON CREATE SET '
						' 	s1.time = timestamp(), '
						' 	s1.user = $username '
						' SET '
						' 	block_tree_counter_update._LOCK_ = True, '
						' 	block_tree_counter_update.count = block_tree_counter_update.count + 1 '
						' REMOVE '
						' 	block_tree_counter_update._LOCK_ '
					)
					tx.run(update_block_statement, update_block_parameters)
				if 'assign to trees' in record_data['selected_features']:
					update_trees_statement = match_item_query[0]
					update_trees_parameters = match_item_query[1]
					update_trees_parameters['username'] = record_data['username']
					update_trees_parameters['assign_to_trees'] = int(record_data['features_dict']['assign to trees'])
					update_trees_statement += (
						' WITH DISTINCT item, field '
						' OPTIONAL MATCH '
						'	(item)-[:FROM]->(:ItemSamples)-[:FROM]->(assigned_tree:Tree) '
						' OPTIONAL MATCH '
						'	(item)-[:FROM]->(source_sample:Sample) '
						' WITH item, field '
						' WHERE assigned_tree IS NULL '
						' AND source_sample IS NULL '
						' MATCH '
						'	(tree: Tree)-[:IS_IN]->(:FieldTrees)-[:IS_IN]->(field) '
						' WHERE tree.id IN $assign_to_trees '
						' MERGE '
						' 	(item_samples: ItemSamples)'
						'	-[: FROM]->(tree) '
						' MERGE '
						' 	(item)-[s1: FROM]->(item_samples) '
						' ON CREATE SET '
						' 	s1.time = timestamp(), '
						' 	s1.user = $username '
					)
					tx.run(update_trees_statement, update_trees_parameters)
				if 'custom id' in record_data['selected_features']:
					update_custom_id_statement = match_item_query[0] + ' WITH DISTINCT item '
					update_custom_id_parameters = match_item_query[1]
					update_custom_id_parameters['custom_id'] = record_data['features_dict']['custom id']
					update_custom_id_statement += (
						' SET item.custom_id = CASE WHEN item.custom_id IS NULL '
						'	THEN $custom_id '
						'	ELSE item.custom_id '
						'	END '
					)
					tx.run(update_custom_id_statement, update_custom_id_parameters)
				if 'tissue type' in record_data['selected_features']:
					update_tissue_type_statement = match_item_query[0] + ' WITH DISTINCT item '
					update_tissue_type_parameters = match_item_query[1]
					update_tissue_type_parameters['tissue_type'] = record_data['features_dict']['tissue type']
					update_tissue_type_statement += (
						' SET item.tissue = CASE WHEN item.tissue IS NULL '
						'	THEN $tissue_type '
						'	ELSE item.tissue '
						'	END '
					)
					tx.run(update_custom_id_statement, update_custom_id_parameters)
				# we only store the harvest time in the source sample, although tissue type can change
				# e.g. splitting subsequent to harvest
				if 'harvest time' in record_data['selected_features']:
					update_harvest_time_statement = match_item_query[0] + ' WITH DISTINCT item '
					update_harvest_time_parameters = match_item_query[1]
					update_harvest_time_parameters['harvest_time'] = record_data['features_dict']['harvest time']
					update_harvest_time_statement += (
						' OPTIONAL MATCH '
						'	(item)-[:FROM]->(source_sample:Sample) '
						' WITH item WHERE source_sample IS NULL '
						' SET item.`harvest time` = CASE '
						'	WHEN item.`harvest time` IS NULL '
						'	THEN $harvest_time '
						'	ELSE item.`harvest time` '
						'	END '
						' SET item.time = CASE '
						'	WHEN item.`harvest date` IS NULL '
						'	THEN null '
						'	ELSE CASE WHEN item.`harvest time` IS NULL '
						'		THEN apoc.date.parse(value + " 12:00", "ms", "yyyy-MM-dd HH:mm") '
						'		ELSE apoc.date.parse(value + " " + item.`harvest time`, "ms", "yyyy-MM-dd HH:mm") '
						'		END '
						'	END '
					)
					tx.run(update_harvest_time_statement, update_harvest_time_parameters)
				if 'harvest date' in record_data['selected_features']:
					update_harvest_date_statement = match_item_query[0] + ' WITH DISTINCT item '
					update_harvest_date_parameters = match_item_query[1]
					update_harvest_date_parameters['harvest_date'] = record_data['features_dict']['harvest date']
					update_harvest_date_statement += (
						' OPTIONAL MATCH '
						'	(item)-[:FROM]->(source_sample:Sample) '
						' WITH item WHERE source_sample IS NULL '
						' SET item.`harvest date` = CASE '
						'	WHEN item.`harvest date` IS NULL '
						'	THEN $harvest_date '
						'	ELSE item.`harvest date` '
						'	END '
						' SET item.time = CASE '
						'	WHEN item.`harvest date` IS NULL '
						'	THEN null '
						'	ELSE CASE '
						'		WHEN item.`harvest time` IS NULL '
						'		THEN apoc.date.parse(value + " 12:00", "ms", "yyyy-MM-dd HH:mm") '
						'		ELSE apoc.date.parse(value + " " + item.`harvest time`, "ms", "yyyy-MM-dd HH:mm")'
						'		END '
						'	END'

					)
					tx.run(update_harvest_date_statement, update_harvest_date_parameters)
			merged = [
				record[0] for record in tx.run(merge_query['statement'], merge_query['parameters'])
			]
			if record_data['record_type'] in ['trait', 'property']:
				conflicts = []
				for record in merged:
					if record['found']:
						if not record['access']:
							conflicts.append(record)
						elif record['conflict']:
							conflicts.append(record)
				if conflicts:
					tx.rollback()
					html_table = self.result_table(conflicts, record_data['record_type'])
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
		except (TransactionError, SecurityError, ServiceUnavailable):
			return jsonify({
				'submitted': (
					'An error occurred, please try again later'
				)
			})

	def build_merge_property_value_query(self, record_data):
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
			'sample_id_list': record_data['sample_id_list'],
			'features_list': record_data['selected_features'],
			'features_dict': record_data['features_dict']
		}
		statement = ItemList.build_match_item_statement(record_data)[0]
		statement += (
			' WITH DISTINCT item '
		)
		if record_data['item_level'] != 'field':
			statement += (
				', field '
			)
		statement += (
			' UNWIND $features_list as feature_name '
			'	WITH item, feature_name, $features_dict[feature_name] as value '
		)
		if record_data['item_level'] != 'field':
			statement += (
				', field '
			)
		statement += (
			'	MATCH '
			'		(:RecordType {name_lower:toLower($record_type)})'
			'		<-[:OF_TYPE]-(feature: Feature '
			'			{name_lower: toLower(feature_name)} '
			'		)-[:AT_LEVEL]->(:ItemLevel {name_lower: toLower($item_level)}) '
			'	MATCH '
			'		(:User '
			'			{ username_lower: toLower($username) } '
			'		)-[:SUBMITTED]->(: Submissions) '
			'		-[:SUBMITTED]->(us: Records) '
			'	MERGE (feature) '
		)
		if record_data['item_level'] == 'field':
			statement += (
				'	<-[:FOR_FEATURE]-(if: ItemFeature) '
				'	-[:FOR_ITEM]->(item) '
				' WITH '
				'	item, feature, if, us, '
			)
		else:
			statement += (
				'	<-[:FOR_FEATURE]-(ff:FieldFeature) '
				'	-[:FROM_FIELD]->(field) '
				' MERGE '
				'	(ff) '
				'	<-[:FOR_FEATURE]-(if: ItemFeature) '
				'	-[:FOR_ITEM]->(item) '
				' WITH '
				'	item, feature, if, ff, us, '
				)
		statement += (
			Cypher.upload_check_value +
			' as value '
		)
		if record_data['item_level'] != 'field':
			statement += (
				', field '
			)
		statement += (
			'	MERGE '
			'		(if) '
			'		<-[:RECORD_FOR]-(r: Record) '
		)
		statement += (
			' ON CREATE SET '
			'	r.found = False, '
			'	r.value = value, '
			'	r.person = $username '
			' ON MATCH SET '
			'	r.found = True '
			# additional statements to occur when new record
			' FOREACH (n IN CASE '
			'		WHEN r.found = False '
			'			THEN [1] ELSE [] END | '
			# track user submissions through /User/Field/Feature container
			'				MERGE '
			'					(us)-[:SUBMITTED]->(uff:UserFieldFeature) '
		)
		if record_data['item_level'] == 'field':
			statement += (
				'					-[:CONTRIBUTED]->(if) '
			)
		else:
			statement += (
				'					-[:CONTRIBUTED]->(ff) '
			)
		statement += (
			# then the record with a timestamp
			'				CREATE '
			'					(uff)-[s1:SUBMITTED {time: timestamp()}]->(r) '
			' ) '
			' WITH '
			'	r, feature, value, '
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
			'	<-[: SUBMITTED]-(: Records) '
			'	<-[: SUBMITTED]-(: Submissions) '
			'	<-[: SUBMITTED]-(u: User) '
			' OPTIONAL MATCH '
			'	(u)-[: AFFILIATED {data_shared: True}]->(p: Partner) '
			' OPTIONAL MATCH '
			'	(p)<-[a: AFFILIATED]-(: User {username_lower: toLower($username)}) '
			' RETURN { '
			'	UID: item_uid, '
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
			'			ELSE value '
			'			END, '
			'	access: a.confirmed, '
			'	conflict: '
			'		CASE '
			'			WHEN r.value = value '
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
			'sample_id_list': record_data['sample_id_list'],
			'time': record_data['record_time'],
			'features_list': record_data['selected_features'],
			'features_dict': record_data['features_dict']
		}
		statement = ItemList.build_match_item_statement(record_data)[0]
		statement += (
			' WITH DISTINCT item '
		)
		if record_data['item_level'] != 'field':
			statement += (
				', field '
			)
		statement += (
			' UNWIND $features_list as feature_name '
			'	WITH item, feature_name, $features_dict[feature_name] as value '
		)
		if record_data['item_level'] != 'field':
			statement += (
				', field '
			)
		statement += (
			'	MATCH '
			'		(:RecordType {name_lower:toLower($record_type)})'
			'		<-[:OF_TYPE]-(feature: Feature '
			'			{name_lower: toLower(feature_name)} '
			'		)-[:AT_LEVEL]->(:ItemLevel {name_lower: toLower($item_level)}) '
			'	MATCH '
			'		(:User '
			'			{ username_lower: toLower($username) } '
			'		)-[:SUBMITTED]->(: Submissions) '
			'		-[:SUBMITTED]->(us: Records) '
			'	MERGE (feature) '
		)
		if record_data['item_level'] == 'field':
			statement += (
				'	<-[:FOR_FEATURE]-(if: ItemFeature) '
				'	-[:FOR_ITEM]->(item) '
				' WITH '
				'	item, feature, if, us, '
			)
		else:
			statement += (
				'	<-[:FOR_FEATURE]-(ff:FieldFeature) '
				'	-[:FROM_FIELD]->(field) '
				' MERGE '
				'	(ff) '
				'	<-[:FOR_FEATURE]-(if: ItemFeature) '
				'	-[:FOR_ITEM]->(item) '
				' WITH '
				'	item, feature, if, ff, us, '
				)
		statement += (
			Cypher.upload_check_value +
			' as value '
		)
		if record_data['item_level'] != 'field':
			statement += (
				', field '
			)
		statement += (
			'	MERGE '
			'		(if) '
			'		<-[:RECORD_FOR]-(r: Record { '
			'			time: $time '
			'		})'
		)
		statement += (
			' ON CREATE SET '
			'	r.found = False, '
			'	r.value = value, '
			'	r.person = $username '
			' ON MATCH SET '
			'	r.found = True '
			# additional statements to occur when new record
			' FOREACH (n IN CASE '
			'		WHEN r.found = False '
			'			THEN [1] ELSE [] END | '
			# track user submissions through /User/Field/Feature container
			'				MERGE '
			'					(us)-[:SUBMITTED]->(uff:UserFieldFeature) '
		)
		if record_data['item_level'] == 'field':
			statement += (
				'					-[:CONTRIBUTED]->(if) '
			)
		else:
			statement += (
				'					-[:CONTRIBUTED]->(ff) '
			)
		statement += (
			# then the record with a timestamp
			'				CREATE '
			'					(uff)-[s1:SUBMITTED {time: timestamp()}]->(r) '
			' ) '
			' WITH '
			'	r, feature, value, '
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
			'	<-[: SUBMITTED]-(: Records) '
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
			'			WHEN r.value = value '
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
			' WITH DISTINCT item '
		)
		if record_data['item_level'] != 'field':
			statement += (
				', field '
			)
		statement += (
			' UNWIND $features_list as feature_name '
			'	WITH item, feature_name, $features_dict[feature_name] as value '
		)
		if record_data['item_level'] != 'field':
			statement += (
				', field '
			)
		statement += (
			'	MATCH (item) '
			'	<-[:FOR_ITEM]-(if: ItemFeature) '
			'	-[:FOR_FEATURE*..2]->(feature:Feature'
			'		{name_lower: toLower(feature_name)} '
			'	), '
			'	(if) '
			'	<-[:RECORD_FOR]-(r: Record) '
			'	<-[s: SUBMITTED]-(: UserFieldFeature) '
			'	<-[: SUBMITTED]-(: Records) '
			'	<-[: SUBMITTED]-(: Submissions) '
			'	<-[: SUBMITTED]-(u: User) '
			'	-[:AFFILIATED {data_shared: true}]->(p:Partner) '
			'	OPTIONAL MATCH '
			'		(p)<-[: AFFILIATED {confirmed: true}]-(cu: User {username_lower: toLower($username)}) '
			# set lock on ItemCondition node and only unlock after merge or result
			# this is either rolled back or set to false on subsequent merger, 
			# prevents conflicts (per item/feature) emerging from race condition 
			' SET if._LOCK_ = True '
			' WITH '
			'	item, feature, if, r, s, u, p, cu, '
			+ Cypher.upload_check_value +
			' as value '
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
			'			r.value <> value '
			'		) '
		)
		# allowing unbound time ranges but, where values don't match:
		#   - bound cannot be set in overlapping range with existing bound
		#   - lower/upper bounded cannot be set over existing bound
		#   - Lower or upper bound cannot be set at same time as existing lower or upper bound, respectively.
		# Lower bound is inclusive, upper bound is exclusive
		# Unbound is stored as False instead of Null to facilitate specific mergers
		if all([record_data['start_time'], record_data['end_time']]):
			statement += (
				' AND (( '
				# - any overlapping records
				'	CASE WHEN r.start <> False THEN r.start ELSE Null END < $end_time '
				'	AND '
				'	CASE WHEN r.end <> False THEN r.end ELSE Null END >= $start_time '
				'	) OR ( '
				# - OR a record that has a lower bound in the bound period and unbound upper
				'	r.end = False'
				'	AND '
				'	CASE WHEN r.start <> False THEN r.start ELSE Null END >= $start_time '
				'	AND '
				'	CASE WHEN r.start <> False THEN r.start ELSE Null END < $end_time '
				'	) OR ( '
				# - OR a record that has an upper bound in the bound period and unbound lower
				'	r.start = False '
				'	AND '
				'	CASE WHEN r.end <> False THEN r.end ELSE Null END > $start_time '
				'	AND '
				'	CASE WHEN r.end <> False THEN r.end ELSE Null END <= $end_time '
				' )) '
			)
		# defined start conflicts if:
		elif record_data['start_time']:
			statement += (
				# - defined start time is in existing bound period
				' AND (( '
				'	CASE WHEN r.end <> False THEN r.end ELSE Null END > $start_time '
				'	AND '
				'	CASE WHEN r.start <> False THEN r.start ELSE Null END <= $start_time '
				# Or lower bound is the same as an existing record with no upper bound
				'	) OR ( '
				'	r.start = $start_time '
				'	AND '
				'	r.end = False '
				' )) '
			)
		# defined start conflicts if:
		elif record_data['end_time']:
			statement += (
				# - defined end time is in existing bound period
				' AND (( '
				'	CASE WHEN r.end <> False THEN r.end ELSE Null END >= $end_time '
				'	AND '
				'	CASE WHEN r.start <> False THEN r.start ELSE Null END < $end_time '
				# Or end time is the same as an existing record with no start time
				'	) OR ( '
				'	r.end = $end_time '
				'	AND '
				'	r.start = False '
				' )) '
			)
		# No defined start or end will only conflict with another record that has no-defined start or end
		elif not any([record_data['start_time'], record_data['end_time']]):
			statement += (
				' AND '
				'	r.start = False '
				' AND '
				'	r.end = False '
			)
		statement += (
			' WITH '
			'	r, value, '
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
			' WITH DISTINCT item '
		)
		if record_data['item_level'] != 'field':
			statement += (
				', field '
			)
		statement += (
			' UNWIND $features_list as feature_name '
			'	WITH item, feature_name, $features_dict[feature_name] as value '
		)
		if record_data['item_level'] != 'field':
			statement += (
				', field '
			)
		statement += (
			'	MATCH '
			'		(:RecordType {name_lower: toLower($record_type)})'
			'		<-[:OF_TYPE]-(feature: Feature '
			'			{name_lower: toLower(feature_name)} '
			'		)-[:AT_LEVEL]->(:ItemLevel {name_lower: toLower($item_level)}) '
			'	MATCH '
			'		(:User '
			'			{ username_lower: toLower($username) } '
			'		)-[:SUBMITTED]->(: Submissions) '
			'		-[:SUBMITTED]->(us: Records) '
			'	MERGE (feature) '
		)

		if record_data['item_level'] == 'field':
			statement += (
				'	<-[:FOR_FEATURE]-(if: ItemFeature) '
				'	-[:FOR_ITEM]->(item) '
				' WITH '
				'	item, feature, if, us, '
			)
		else:
			statement += (
				'	<-[:FOR_FEATURE]-(ff: FieldFeature) '
				'	-[:FROM_FIELD]->(field) '
				' MERGE '
				'	(ff) '
				'	<-[:FOR_FEATURE]-(if: ItemFeature) '
				'	-[:FOR_ITEM]->(item) '
				' WITH '
				'	item, feature, if, ff, us, '
			)
		# When merging, if the merger properties agree between input and existing then no new node will be created,
		#  even if other properties in existing are not specified in the input.
		# This means Null is not suitable as a value for "not-specified" start time
		# So we coalesce the value and the boolean False, but it means we have to check for this False value
		# in all comparisons...e.g. in the condition conflict query
		statement += (
			Cypher.upload_check_value +
			' as value '
		)
		if record_data['item_level'] != 'field':
			statement += (
				', field '
			)
		statement += (
			' MERGE '
			'	(r: Record { '
			'		start: COALESCE($start_time, False), '
			'		end: COALESCE($end_time, False), '
			'		value: value '
			'	})-[:RECORD_FOR]->(if) '
			' ON CREATE SET '
			'	r.found = False, '
			'	r.person = $username '
			' ON MATCH SET '
			'	r.found = True '
			# unlock ItemCondition node, this is set to true to obtain lock in the conflict query
			' SET if._LOCK_ = False '
			# additional statements to occur when new record
			' FOREACH (n IN CASE '
			'		WHEN r.found = False '
			'			THEN [1] ELSE [] END | '
			# track user submissions through /User/Field/Feature container
			'				MERGE '
			'					(us)-[:SUBMITTED]->(uff:UserFieldFeature) '
		)
		if record_data['item_level'] == 'field':
			statement += (
				'					-[:CONTRIBUTED]->(if) '
			)
		else:
			statement += (
				'					-[:CONTRIBUTED]->(ff) '
			)
		statement += (
			# then the record with a timestamp
			'				CREATE '
			'					(uff)-[s1:SUBMITTED {time: timestamp()}]->(r) '
			'				'
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
			'	<-[: SUBMITTED]-(: Records) '
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
		elif record_type == 'condition':
			headers = ['UID', 'Feature', 'Start', 'End', 'Submitted by', 'Submitted at', 'Value']
		else:  # record_type == 'property'
			headers = ['UID', 'Feature', 'Submitted by', 'Submitted at', 'Value']
		header_string += '</p></th><th><p>'.join(headers)
		header_string += '</p></th></tr>'
		for record in result_list:
			if record['submitted_at']:
				submitted_at = datetime.datetime.utcfromtimestamp(int(record['submitted_at']) / 1000).strftime("%Y-%m-%d %H:%M")
			else:
				submitted_at = ""
			row_string = '<tr><td>'
			if record_type == 'condition':
				if record['start']:
					start_time = datetime.datetime.utcfromtimestamp(int(record['start']) / 1000).strftime("%Y-%m-%d %H:%M")
				else:
					start_time = ""
				if record['end']:
					end_time = datetime.datetime.utcfromtimestamp(int(record['end']) / 1000).strftime("%Y-%m-%d %H:%M")
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
			elif record_type == 'trait':
				if record['time']:
					time = datetime.datetime.utcfromtimestamp(int(record['time']) / 1000).strftime("%Y-%m-%d %H:%M")

				else:
					time = ""
				row_data = [
						str(record['UID']),
						record['feature'],
						time,
						record['user'],
						submitted_at
					]
			else:
				row_data = [
					str(record['UID']),
					record['feature'],
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
			row_string += str(record['value']) + '</td></tr>'
			header_string += row_string
		return '<table>' + header_string + '<table>'

