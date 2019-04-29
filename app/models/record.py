from app import ServiceUnavailable, SecurityError, TransactionError

from app.models import ItemList

from app.cypher import Cypher

from flask import jsonify

from neo4j_driver import get_driver, neo4j_query, bolt_result

import datetime


class Record:
	def __init__(self, username):
		self.username = username
		self.neo4j_session = get_driver().session()

	def submit_records(self, record_data):
		record_data['username'] = self.username
		try:
			tx = self.neo4j_session.begin_transaction()
			if record_data['record_type'] == 'condition':
				# for condition data we first query for conflicts since there are many types of conflict
				# for traits we don't do this as it is simpler to perform during the merger
				conflicts_query = self.build_condition_conflicts_query(record_data)
				conflicts = tx.run(
						conflicts_query['statement'],
						conflicts_query['parameters']
					)
				if conflicts.peek():
					tx.close()
					html_table = self.result_table(conflicts, record_data['record_type'])['table']
					return jsonify({
						'submitted': (
								' Record not submitted. <br><br> '
								' A value you are trying to submit conflicts with an existing entry. '
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
			# if any updates to perform:
			if bool(
					{
						'variety name',
						'variety code',
						'custom id',
						'assign to block',
						'assign to trees',
						'assign to samples'
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
						' MERGE (item)-[:OF_VARIETY]->(fv) '
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
					update_trees_parameters['assign_to_trees'] = record_data['features_dict']['assign to trees']
					update_trees_statement += (
						' WITH DISTINCT item, field '
						' MATCH '
						'	(item)'
						'	-[: FROM]->(: ItemSamples) '
						'	-[: FROM]->(field) '
						'	<-[:IS_IN]-(: FieldTrees) '
						'	<-[:IS_IN]-(tree: Tree) '
						' WHERE tree.id IN extract(x in split($assign_to_trees, ",") | toInteger(trim(x))) '
						' MERGE '
						' 	(item_samples: ItemSamples)'
						'	-[: FROM]->(tree) '
						' CREATE '
						' 	(item)-[:FROM]->(item_samples) '
					)
					tx.run(update_trees_statement, update_trees_parameters)
				if 'assign_to_samples' in record_data['selected_features']:
					update_samples_statement = match_item_query[0]
					update_samples_parameters = match_item_query[1]
					update_samples_parameters['username'] = record_data['username']
					update_samples_parameters['assign_to_samples'] = record_data['features_dict']['assign to samples']
					update_samples_statement += (
						' WITH DISTINCT item, field '
						' MATCH '
						'	(item)'
						'	-[from_field: FROM]->(: ItemSamples) '
						'	-[: FROM]->(field) '
						' MATCH '
						'	(field)'
						'	<-[:FROM | IS_IN*]-(sample: Sample) '
						' WHERE sample.id IN extract(x in split($assign_to_samples, ",") | toInteger(trim(x))) '
						' AND item.id <> sample.id '
						' CREATE '
						' 	(item)-[from_sample:FROM]->(sample) '
						' WITH item, sample, from_field, from_sample '
						# prevent cycles 
						'	OPTIONAL MATCH cycle = (sample)-[: FROM *]->(sample) '
						'	FOREACH (n IN CASE WHEN cycle IS NULL THEN [1] ELSE [] END | '
						'		DELETE from_field '
						'	) '
						'	FOREACH (n IN CASE WHEN cycle IS NOT NULL THEN [1] ELSE [] END | '
						'		DELETE from_sample '
						'	) '
					)
					tx.run(update_samples_statement, update_samples_parameters)
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
				if 'harvest date' in record_data['selected_features']:
					update_harvest_time_statement = match_item_query[0] + ' WITH DISTINCT item '
					update_harvest_time_parameters = match_item_query[1]
					update_harvest_time_parameters['harvest_date'] = record_data['features_dict']['harvest date']
					if 'harvest time' in record_data['selected_features']:
						update_harvest_time_parameters['harvest_time'] = record_data['features_dict']['harvest time']
					else:
						update_harvest_time_parameters['harvest_time'] = None
					update_harvest_time_statement += (
						' MATCH '
						'	(item)-[:FROM]->(:ItemSamples) '
						'	WHERE '
						'		item.time IS NULL  '
						'	SET item.time = CASE '
						'		WHEN $harvest_time IS NOT NULL '
						'		THEN apoc.date.parse(uid_date_time[1] + " " + uid_date_time[2], "ms", "yyyy-MM-dd HH:mm") '
						'		ELSE apoc.date.parse(uid_date_time[1] + " 12:00, "ms", "yyyy-MM-dd HH:mm") '
						'		END '
					)
					tx.run(update_harvest_time_statement, update_harvest_time_parameters)
			merged = tx.run(merge_query['statement'], merge_query['parameters'])
			result_table = self.result_table(merged, record_data['record_type'])
			if result_table['conflicts']:
				tx.rollback()
				html_table = result_table['table']
				return jsonify({
					'submitted': (
						' Record not submitted. <br><br> '
						' A value you are trying to submit conflicts with an existing entry. '
						+ html_table
					),
					'class': 'conflicts'
				})
			else:
				tx.commit()
				html_table = result_table['table']
				return jsonify({'submitted': (
					' Records submitted or found (highlighted): '
					+ html_table
					)
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
			'	(p)<-[access: AFFILIATED]-(: User {username_lower: toLower($username)}) '
			' RETURN { '
			'	UID: item_uid, '
			'	feature: feature.name, '
			'	value: '
			'		CASE '
			# returning value if just submitted, regardless of access
			'			WHEN r.found '
			'			THEN CASE '
			'				WHEN access.confirmed '
			'				THEN r.value '
			'				ELSE "ACCESS DENIED" '
			'				END '
			'			ELSE value '
			'			END, '
			'	access: access.confirmed, '
			'	conflict: '
			'		CASE '
			'			WHEN r.value = value '
			'			THEN False '
			'			ELSE True '
			'			END, '
			'	found: r.found, '
			'	submitted_at: s.time, '
			'	user: CASE '
			'		WHEN access.confirmed = True '
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
			'	r.replicate = 1, '
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
			'				ELSE "ACCESS DENIED" '
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
			'		WHEN a.confirmed '
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
			# prevents conflicts (per item/feature) emerging from race conditions
			' SET if._LOCK_ = True '
			' WITH '
			'	$end_time as end, '
			'	$start_time as start, '
			'	item,'
			'	feature, '
			'	if, '
			'	r, '
			'	CASE WHEN r.start <> False THEN r.start ELSE Null END as r_start, '
			'	CASE WHEN r.end <> False THEN r.end ELSE Null END as r_end, '
			'	s, '
			'	u, '
			'	p, '
			'	cu, '
			+ Cypher.upload_check_value +
			' as value '
		)
		if record_data['item_level'] != "field":
			statement += (
				', field '
			)
		statement += (
			' WHERE '
			# If don't have access or if have access and values don't match then potential conflict 
			# time parsing to allow various degrees of specificity in the relevant time range is below
			' ( '
			'		cu IS NULL '
			'		OR '
			'		r.value <> value '
			' ) AND ( '
			'	( '
			# handle fully bound records
			# - any overlapping records
			'		r_start < end '
			'		AND '
			'		r_end > start '
			'	) OR ( '
			# - a record that has a lower bound in the bound period 
			'		r_start >= start '
			'		AND '
			'		r_start < end '
			'	) OR ( '
			# - a record that has an upper bound in the bound period
			'		r_end > start '
			'		AND '
			'		r_end <= end '
			'	) OR ( '
			# now handle lower bound only records
			'		end IS NULL '
			'		AND ( '
			# - existing bound period includes start
			'			r_end > start '
			'			AND '
			'			r_start <= start '
			# - record with same lower bound
			'		) OR ( '
			'			r_start = start '
			# - record with upper bound only greater than this lower bound
			'		) OR ( '
			'			r_start IS NULL '
			'			AND '
			'			r_end > start '
			'		) '
			'	) OR ( '
			# now handle upper bound only records 
			'		start IS NULL '
			'		AND ( '
			# - existing bound period includes end
			'			r_end >= end '
			'			AND '
			'			r_start < end '
			# - record with same upper bound
			'		) OR ( '
			'			r_end = end '
			# - record with lower bound only less than this upper bound
			'		) OR ( '
			'			r_end IS NULL '
			'			AND '
			'			r_start < end '
			'		) '
			'	) OR ( '
			# always conflict with unbound records
			'		r_end IS NULL '
			'		AND '
			'		r_start IS NULL '
			'	)'
			' ) '
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
			'	start: r.start, '
			'	end: r.end, '
			'	feature: feature, '
			'	value: CASE WHEN access THEN r.value ELSE "ACCESS DENIED" END, '
			'	submitted_at: submitted_at, '
			'	user: CASE WHEN access THEN user ELSE partner END, '
			'	access: access, '
			'	found: True, '
			'	conflict: True '
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
			'		start: CASE WHEN $start_time IS NOT NULL THEN $start_time ELSE False END, '
			'		end: CASE WHEN $end_time IS NOT NULL THEN $end_time ELSE False END, '
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
			'	-[:AFFILIATED {data_shared: true}]->(p:Partner) '
			'	OPTIONAL MATCH '
			'		(p)<-[access: AFFILIATED]-(: User {username_lower: toLower($username)}) '
			' RETURN { '
			'	UID: item_uid, '
			'	start: r.start,	'
			'	end: r.end, '
			'	feature: feature.name, '
			'	value: '
			'		CASE '
			'			WHEN r.found '
			'			THEN CASE '
			'				WHEN access IS NOT NULL '
			'				THEN r.value '
			'				ELSE "ACCESS DENIED" '
			'				END '
			'			ELSE r.value '
			'			END, '
			'	found: r.found, '
			'	submitted_at: s.time, '
			'	user: u.name, '
			'	access: access.confirmed, '
			'	conflict: False'
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
	def result_table(result, record_type):
		conflicts_found = False
		header_string = '<tr><th><p>'
		if record_type == 'trait':
			headers = ['UID', 'Feature', 'Time', 'Submitted by', 'Submitted at', 'Value']
		elif record_type == 'condition':
			headers = ['UID', 'Feature', 'Start', 'End', 'Submitted by', 'Submitted at', 'Value']
		else:  # record_type == 'property'
			headers = ['UID', 'Feature', 'Submitted by', 'Submitted at', 'Value']
		header_string += '</p></th><th><p>'.join(headers)
		header_string += '</p></th></tr>'
		for record in result:
			record = record[0]
			# iterate through the result, building the table.
			# if we find a conflict we drop the existing table and start only including the conflicts to report
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
			if record['found']:
				if record['access']:
					if 'conflict' in record and record['conflict']:
						conflicts_found = True
						row_string += '</td><td bgcolor = #FF0000>'
					else:
						row_string += '</td><td bgcolor = #00FF00>'
				else:
					conflicts_found = True
					row_string += '</td><td bgcolor = #FF0000>'
			else:
				row_string += '</td><td>'
			row_string += str(record['value']) + '</td></tr>'
			header_string += row_string
		return {
			'conflicts': conflicts_found,
			'table': '<div id="response_table_div"><table>' + header_string + '<table></div>'
		}

