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

	def add_input_group(
			self,
			input_group_name,
			partner_to_copy=None,
			group_to_copy=None
	):
		tx = self.neo4j_session.begin_transaction()
		parameters = {
			'input_group_name': input_group_name,
			'partner_to_copy':partner_to_copy,
			'group_to_copy': group_to_copy,
			'username': self.username
		}
		# need to check if name is already registered by anyone with [AFFILIATED {data_shared:True}] to this partner
		check_existing_statement = (
			' MATCH '
			'	(partner: Partner)'
			'	<-[affiliated: AFFILIATED {'
			'		data_shared: True'
			'	}]-(: User {'
			'		username_lower: toLower(trim($username))'
			'	}) '
			' MATCH '
			'	(partner)<-[: AFFILIATED {'
			'		data_shared: True '
			'	}]-(user: User) '
			'	-[:SUBMITTED]->(: Submissions) '
			'	-[:SUBMITTED]->(: InputGroups) '
			'	-[:SUBMITTED]->(ig: InputGroup { '
			'		name_lower: toLower(trim($input_group_name)) '
			'	}) '
			' RETURN [ '
			'	True, '
			'	ig.name_lower, '
			'	ig.name '
			' ] '
		)
		existing_for_partner = tx.run(
			check_existing_statement,
			parameters
		).single()
		if existing_for_partner:
			return existing_for_partner[0]
		# now create a new group
		# first make sure the user has the InputGroups submission node
		statement = (
			' MATCH '
			'	(: User { '
			'		username_lower: toLower(trim($username)) '
			'	})-[:SUBMITTED]->(sub: Submissions) '
			' MERGE '
			'	(sub)-[:SUBMITTED]->(igs: InputGroups) '
			' WITH igs '
		)
		if partner_to_copy and group_to_copy:
			statement += (
				' MATCH '
				'	(source_partner: Partner { '
				'		name_lower:toLower(trim($partner_to_copy)) '
				'	}) '
				'	<-[: AFFILIATED {'
				'		data_shared: True'
				'	}]-(: User)-[: SUBMITTED]->(: Submissions) '
				'	-[:SUBMITTED]->(: InputGroups) '
				'	-[:SUBMITTED]->(ig: InputGroup { '
				'		name_lower: toLower(trim($group_to_copy)) '
				'	}) '
			)
		elif group_to_copy and not partner_to_copy:
			statement += (
				' MATCH '
				' (source_ig: InputGroup { '
				'		name_lower: toLower(trim($group_to_copy)) '
				' }) '
				' OPTIONAL MATCH (source_ig)<-[s:SUBMITTED]-() '
				# prioritise selection of the oldest if partner not specified 
				# (including original 'unsubmitted' groups )
				' WITH igs, source_ig ORDER BY s.time DESC LIMIT 1 '
				' CREATE '
				'	(igs)-[: SUBMITTED { '
				'		time: datetime.transaction().epochMillis '
				'	}]->(ig: InputGroup {'
				'		name_lower: toLower(trim($input_group_name)), '
				'		name: trim($input_group_name), '
				'		found: False '
				'	}) '
				' WITH ig, source_ig '
				' MATCH '
				'	(source_ig) '
				'	<-[in_rel: IN_GROUP]-(input: Input) '
				' WITH '
				'	ig, in_rel, input '
				# we copy the in_rel as this is where we store the order of appearance
				' CREATE (input)-[new_in_rel:IN_GROUP]->(ig) '
				' SET new_in_rel = in_rel '
			)
		else:
			statement += (
				' CREATE '
				'	(igs)-[: SUBMITTED {'
				'		time: datetime.transaction().epochMillis'
				'	}]->(ig: InputGroup {'
				'		name_lower: toLower(trim($input_group_name)), '
				'		name: trim($input_group_name), '
				'		found: False'
				'	}) '
			)
		statement += (
			' RETURN DISTINCT ['
			'	ig.found, '
			'	ig.name_lower, '
			'	ig.name '
			' ] '
		)
		result = tx.run(
			statement,
			parameters
		).single()
		if result:
			tx.commit()
			return result[0]

	def update_group(self, input_group, inputs):
		parameters = {
			'username': self.username,
			'input_group': input_group,
			'inputs': inputs
		}
		statement = (
			' MATCH '
			'	(user:User {username_lower:toLower(trim($username))}) '
			'	-[:SUBMITTED]->(sub:Submissions) '
			' MATCH (user)-[: AFFILIATED { '
			'	data_shared: True '
			'	}]->(p:Partner) '
			' MATCH '
			'	(p)<-[:AFFILIATED {data_shared: True}]-(:User) '
			'	-[:SUBMITTED]->(: Submissions) '
			'	-[:SUBMITTED]->(: InputGroups) '
			'	-[:SUBMITTED]->(ig: InputGroup {'
			'		name_lower: toLower(trim($input_group)) '
			'	}) '
			' MERGE '
			'	(sub)-[:SUBMITTED]->(igs:InputGroups) '
			' MERGE '
			'	(igs)-[mod:MODIFIED]->(ig) '
			'	ON CREATE SET mod.times = [datetime.transaction().epochMillis] '
			'	ON MATCH SET mod.times = mod.times + datetime.transaction().epochMillis '
			' WITH user, ig '
			' OPTIONAL MATCH '
			'	(ig)<-[in_rel:IN_GROUP]-(:Input) '
			' DELETE in_rel '
			' WITH DISTINCT '
			'	user, ig, range(0, size($inputs)) as index '
			' UNWIND index as i '
			'	MATCH (input:Input {name_lower:toLower(trim($inputs[i]))}) '
			'	CREATE (input)-[rel:IN_GROUP {position: i}]->(ig) '
			' RETURN '
			'	input.name '
			' ORDER BY rel.position '
		)
		result = self.neo4j_session.run(
				statement,
				parameters
		)
		return [record[0] for record in result]

	def add_inputs_to_group(self, input_group, inputs):
		parameters = {
			'username': self.username,
			'input_group': input_group,
			'inputs': inputs
		}
		statement = (
			' MATCH '
			'	(user:User {username_lower:toLower(trim($username))}) '
			'	-[:SUBMITTED]->(sub:Submissions) '
			' MATCH '
			'	(ig: InputGroup {'
			'		name_lower: toLower(trim($input_group)) '
			'	}) '
			' MERGE '
			'	(sub)-[:SUBMITTED]->(igs:InputGroups) '
			' MERGE '
			'	(igs)-[:MODIFIED]->(ig) '
			' WITH '
			'	user, ig '
			' UNWIND $inputs as input_name '
			'	MATCH (input:Input {name_lower:toLower(trim(input_name))}) '
			'	MERGE (input)-[add:IN_GROUP]->(ig) '
			'		ON CREATE SET '
			'			add.user = user.username, '
			'			add.time = datetime.transaction().epochMillis, '
			'			add.found = False '
			'		ON MATCH SET '
			'			add.found = True '
			' RETURN [ '
			'	add.found, '
			'	input.name '
			' ] '
		)
		result = self.neo4j_session.run(
				statement,
				parameters
		)
		return [record[0] for record in result]

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
						'This record type is not yet supported by direct submission'
					)
				})
			# if any updates to perform:
			if bool(
					{
						'variety name',
						'variety code',
						'assign custom id',
						'assign to block',
						'assign to trees',
						'assign to samples',
						'specify tissue',
						'harvest date',
						# 'harvest time'  # this only updates when set at same time as harvest date
						'planting date'
					} & set(record_data['selected_inputs'])
			):
				match_item_query = ItemList.build_match_item_statement(record_data)
				if bool(
						{'variety name', 'variety code'} & set(record_data['selected_inputs'])
				):
					# before going any further check that code and name match if both are provided
					if {'variety name', 'variety code'}.issubset(set(record_data['selected_inputs'])):
						same_variety = tx.run(
							' MATCH '
							'	(v:Variety {name:$name, code:$code}) '
							' RETURN v.name ',
							name=record_data['inputs_dict']['variety name'],
							code=record_data['inputs_dict']['variety code']
							).single()
						if not same_variety:
							tx.rollback()
							return jsonify({
								'submitted': (
									' Submission rejected: '
									' The entered variety name and variety code do not match. '
									' Please note that it is not necessary to enter both. '
								),
								'class': 'conflict'
							})
					update_variety_statement = match_item_query[0] + ' WITH DISTINCT item '
					if record_data['item_level'] != 'field':
						update_variety_statement += ', field '
					update_variety_parameters = match_item_query[1]
					update_variety_parameters['username'] = record_data['username']
					if 'variety name' in record_data['selected_inputs']:
						update_variety_parameters['variety_name'] = record_data['inputs_dict']['variety name']
						update_variety_statement += ' MATCH (update_variety:Variety {name_lower: toLower($variety_name)}) '
					else:  # 'variety code' in record_data['selected_inputs']:
						update_variety_parameters['variety_code'] = record_data['inputs_dict']['variety code']
						update_variety_statement += ' MATCH (update_variety:Variety {code: $variety_code}) '
					update_variety_statement += (
						' OPTIONAL MATCH '
						'	(item) '
						'	-[of_current_variety: OF_VARIETY]->(:FieldVariety) '
						' OPTIONAL MATCH '
						'	(item)-[:FROM]->(source_sample:Sample) '
						'	OPTIONAL MATCH '
						'		(item)<-[:FOR_ITEM]-(ii:ItemInput) '
						'		-[:FOR_INPUT]->(:FieldInput)-[:FOR_INPUT]->(input:Input), '
						'		(ii)<-[:RECORD_FOR]-(record:Record) '
						'		<-[s:SUBMITTED]-() '
						'		<-[:SUBMITTED*..4]-(user:User) '
						'		-[:AFFILIATED {data_shared: True}]->(p:Partner) '
						'		WHERE input.name_lower CONTAINS "variety" '
						'	OPTIONAL MATCH '
						'		(:User {username_lower:toLower(trim($username))}) '
						'		-[access:AFFILIATED]->(p) '
						' WITH '
						'	item, '
					)
					if record_data['item_level'] != 'field':
						update_variety_statement += ', field '
					update_variety_statement += (
						'	update_variety, '
						'	current_variety, '
						'	s.time as `Submitted at`, '
						'	CASE '
						'		WHEN access.confirmed THEN user.name + "(" + p.name + ")" '
						'		ELSE p.name '
						'	END as `Submitted by`, '						
						' WHERE s.time <> datetime.transaction().epochMillis '
						# removing these because
						# we want to keep all and roll back to be able to provide feedback about conflicts
						# and prevent the record being created
						#' WITH item, update_variety '
						#'	WHERE of_current_variety IS NULL '
						#'	AND source_sample IS NULL '
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
						'	RETURN { '
						'		UID: item.uid, '
						'		set_variety: update_variety.name, '
						'		current_variety: current_variety.name, '
						'		`Submitted at`: `Submitted at`, '
						'		`Submitted by`: `Submitted by`, '
						'	} '
					)
					result = tx.run(update_variety_statement, update_variety_parameters)
					variety_update_errors = []
					while all([
						len(variety_update_errors) <= 10,
						result.peek()
					]):
						for record in result:
							if all([
								record[0]['current_variety'] and record[0]['set_variety'],
								record[0]['current_variety'] != record[0]['set_variety']
							]):
								variety_update_errors.append(
									'For '
									+ str(record[0]['UID'])
									+ ' the variety submitted ('
									+ record[0]['set_variety']
									+ ') does not match an earlier submission ('
									+ record[0]['current_variety']
									+ ') by '
									+ record[0]['Submitted by']
									+ '('
									+ datetime.datetime.utcfromtimestamp(
										int(record[0]['Submitted at']) / 1000
									).strftime("%Y-%m-%d %H:%M:%S")
									+ ')'
								)
					if variety_update_errors:
						tx.rollback()
						return jsonify({
							'submitted': (
									'<br> - '.join(variety_update_errors)
							),
							'class': 'conflicts'
						})
				if 'assign to block' in record_data['selected_inputs']:
					update_block_statement = match_item_query[0]
					update_block_parameters = match_item_query[1]
					update_block_parameters['username'] = record_data['username']
					update_block_parameters['assign_to_block'] = record_data['inputs_dict']['assign to block']
					update_block_statement += (
						' WITH DISTINCT item, field '
						' MATCH '
						'	(block_update: Block {name_lower: toLower(trim($assign_to_block))})-[:IS_IN]->(:FieldBlocks)-[:IS_IN]->(field) '
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
						' 	s1.time = datetime.transaction().epochMillis, '
						' 	s1.user = $username '
						' SET '
						' 	block_tree_counter_update._LOCK_ = True, '
						' 	block_tree_counter_update.count = block_tree_counter_update.count + 1 '
						' REMOVE '
						' 	block_tree_counter_update._LOCK_ '
					)
					tx.run(update_block_statement, update_block_parameters)
				if 'assign to trees' in record_data['selected_inputs']:
					update_trees_statement = match_item_query[0]
					update_trees_parameters = match_item_query[1]
					update_trees_parameters['username'] = record_data['username']
					update_trees_parameters['assign_to_trees'] = record_data['inputs_dict']['assign to trees']
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
				if 'assign_to_samples' in record_data['selected_inputs']:
					update_samples_statement = match_item_query[0]
					update_samples_parameters = match_item_query[1]
					update_samples_parameters['username'] = record_data['username']
					update_samples_parameters['assign_to_samples'] = record_data['inputs_dict']['assign to samples']
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
				if 'assign custom id' in record_data['selected_inputs']:
					update_custom_id_statement = match_item_query[0] + ' WITH DISTINCT item '
					update_custom_id_parameters = match_item_query[1]
					update_custom_id_parameters['assign custom_id'] = record_data['inputs_dict']['assign custom id']
					update_custom_id_statement += (
						' SET item.custom_id = CASE WHEN item.custom_id IS NULL '
						'	THEN $custom_id '
						'	ELSE item.custom_id '
						'	END '
					)
					tx.run(update_custom_id_statement, update_custom_id_parameters)
				if 'specify tissue' in record_data['selected_inputs']:
					update_tissue_statement = match_item_query[0] + ' WITH DISTINCT item '
					update_tissue_parameters = match_item_query[1]
					update_tissue_parameters['tissue'] = record_data['inputs_dict']['specify tissue']
					update_tissue_statement += (
						' SET item.tissue = CASE WHEN item.tissue IS NULL '
						'	THEN $tissue '
						'	ELSE item.tissue '
						'	END '
					)
					tx.run(update_tissue_statement, update_tissue_parameters)
				if 'planting date' in record_data['selected_inputs']:
					update_planted_statement = match_item_query[0] + ' WITH DISTINCT item '
					update_planted_parameters = match_item_query[1]
					update_planted_parameters['planted'] = record_data['inputs_dict']['planting date']
					update_planted_statement += (
						' SET item.planted = CASE WHEN item.planted IS NULL '
						'	THEN $planted '
						'	ELSE item.planted '
						'	END '
					)
					tx.run(update_planted_statement, update_planted_parameters)
				if 'harvest date' in record_data['selected_inputs']:
					update_harvest_time_statement = match_item_query[0] + ' WITH DISTINCT item '
					update_harvest_time_parameters = match_item_query[1]
					update_harvest_time_parameters['harvest_date'] = record_data['inputs_dict']['harvest date']
					if 'harvest time' in record_data['selected_inputs']:
						update_harvest_time_parameters['harvest_time'] = record_data['inputs_dict']['harvest time']
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
			'inputs_list': record_data['selected_inputs'],
			'inputs_dict': record_data['inputs_dict']
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
			' UNWIND $inputs_list as input_name '
			'	WITH item, input_name, $inputs_dict[input_name] as value '
		)
		if record_data['item_level'] != 'field':
			statement += (
				', field '
			)
		statement += (
			'	MATCH '
			'		(:RecordType {name_lower:toLower($record_type)})'
			'		<-[:OF_TYPE]-(input: Input '
			'			{name_lower: toLower(input_name)} '
			'		)-[:AT_LEVEL]->(:ItemLevel {name_lower: toLower($item_level)}) '
			'	MATCH '
			'		(:User '
			'			{ username_lower: toLower($username) } '
			'		)-[:SUBMITTED]->(: Submissions) '
			'		-[:SUBMITTED]->(us: Records) '
			'	MERGE (input) '
		)
		if record_data['item_level'] == 'field':
			statement += (
				'	<-[:FOR_INPUT]-(if: ItemInput) '
				'	-[:FOR_ITEM]->(item) '
				' WITH '
				'	item, input, if, us, '
			)
		else:
			statement += (
				'	<-[:FOR_INPUT]-(ff:FieldInput) '
				'	-[:FROM_FIELD]->(field) '
				' MERGE '
				'	(ff) '
				'	<-[:FOR_INPUT]-(if: ItemInput) '
				'	-[:FOR_ITEM]->(item) '
				' WITH '
				'	item, input, if, ff, us, '
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
			# track user submissions through /User/Field/Input container
			'				MERGE '
			'					(us)-[:SUBMITTED]->(uff:UserFieldInput) '
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
			'					(uff)-[s1:SUBMITTED {time: datetime.transaction().epochMillis}]->(r) '
			' ) '
			' WITH '
			'	r, input, value, '
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
			'	<-[s: SUBMITTED]-(: UserFieldInput) '
			'	<-[: SUBMITTED]-(: Records) '
			'	<-[: SUBMITTED]-(: Submissions) '
			'	<-[: SUBMITTED]-(u: User) '
			' OPTIONAL MATCH '
			'	(u)-[: AFFILIATED {data_shared: True}]->(p: Partner) '
			' OPTIONAL MATCH '
			'	(p)<-[access: AFFILIATED]-(: User {username_lower: toLower($username)}) '
			' RETURN { '
			'	UID: item_uid, '
			'	input: input.name, '
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
			' ORDER BY input.name_lower '
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
			'inputs_list': record_data['selected_inputs'],
			'inputs_dict': record_data['inputs_dict']
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
			' UNWIND $inputs_list as input_name '
			'	WITH item, input_name, $inputs_dict[input_name] as value '
		)
		if record_data['item_level'] != 'field':
			statement += (
				', field '
			)
		statement += (
			'	MATCH '
			'		(:RecordType {name_lower:toLower($record_type)})'
			'		<-[:OF_TYPE]-(input: Input '
			'			{name_lower: toLower(input_name)} '
			'		)-[:AT_LEVEL]->(:ItemLevel {name_lower: toLower($item_level)}) '
			'	MATCH '
			'		(:User '
			'			{ username_lower: toLower($username) } '
			'		)-[:SUBMITTED]->(: Submissions) '
			'		-[:SUBMITTED]->(us: Records) '
			'	MERGE (input) '
		)
		if record_data['item_level'] == 'field':
			statement += (
				'	<-[:FOR_INPUT]-(if: ItemInput) '
				'	-[:FOR_ITEM]->(item) '
				' WITH '
				'	item, input, if, us, '
			)
		else:
			statement += (
				'	<-[:FOR_INPUT]-(ff:FieldInput) '
				'	-[:FROM_FIELD]->(field) '
				' MERGE '
				'	(ff) '
				'	<-[:FOR_INPUT]-(if: ItemInput) '
				'	-[:FOR_ITEM]->(item) '
				' WITH '
				'	item, input, if, ff, us, '
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
			# track user submissions through /User/Field/Input container
			'				MERGE '
			'					(us)-[:SUBMITTED]->(uff:UserFieldInput) '
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
			'					(uff)-[s1:SUBMITTED {time: datetime.transaction().epochMillis}]->(r) '
			' ) '
			' WITH '
			'	r, input, value, '
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
			'	<-[s: SUBMITTED]-(: UserFieldInput) '
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
			'	input: input.name, '
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
			' ORDER BY input.name_lower '
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
			'inputs_list': record_data['selected_inputs'],
			'inputs_dict': record_data['inputs_dict']
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
			' UNWIND $inputs_list as input_name '
			'	WITH item, input_name, $inputs_dict[input_name] as value '
		)
		if record_data['item_level'] != 'field':
			statement += (
				', field '
			)
		statement += (
			'	MATCH (item) '
			'	<-[:FOR_ITEM]-(if: ItemInput) '
			'	-[:FOR_INPUT*..2]->(input: Input'
			'		{name_lower: toLower(input_name)} '
			'	), '
			'	(if) '
			'	<-[:RECORD_FOR]-(r: Record) '
			'	<-[s: SUBMITTED]-(: UserFieldInput) '
			'	<-[: SUBMITTED]-(: Records) '
			'	<-[: SUBMITTED]-(: Submissions) '
			'	<-[: SUBMITTED]-(u: User) '
			'	-[:AFFILIATED {data_shared: true}]->(p:Partner) '
			'	OPTIONAL MATCH '
			'		(p)<-[: AFFILIATED {confirmed: true}]-(cu: User {username_lower: toLower($username)}) '
			# set lock on ItemCondition node and only unlock after merge or result
			# this is either rolled back or set to false on subsequent merger, 
			# prevents conflicts (per item/input) emerging from race conditions
			' SET if._LOCK_ = True '
			' WITH '
			'	$end_time as end, '
			'	$start_time as start, '
			'	item,'
			'	input, '
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
			# time parsing to allow reduced specificity in the relevant time range is below
			' ( '
			'		cu IS NULL '
			'		OR '
			'		r.value <> value '
			' ) AND ('
			'	( '
			'		( '
			# handle fully bound records
			# - any overlapping records
			'			r_start < end '
			'			AND '
			'			r_end > start '
			'		) OR ( '
			# - a record that has a lower bound in the bound period 
			'			r_start >= start '
			'			AND '
			'			r_start < end '
			'		) OR ( '
			# - a record that has an upper bound in the bound period
			'			r_end > start '
			'			AND '
			'			r_end <= end '
			'		) '
			'	) OR ( '
			# now handle lower bound only records
			'		end IS NULL '
			'		AND ( '
			# - existing bound period includes start
			'			( '
			'				r_end > start '
			'				AND '
			'				r_start <= start '
			# - record with same lower bound
			'			) OR ( '
			'				r_start = start '
			# - record with upper bound only greater than this lower bound
			'			) OR ( '
			'				r_start IS NULL '
			'				AND '
			'				r_end > start '
			'			) '
			'		) '
			'	) OR ( '
			# now handle upper bound only records 
			'		start IS NULL '
			'		AND ( '
			# - existing bound period includes end
			'			( '
			'				r_end >= end '
			'				AND '
			'				r_start < end '
			# - record with same upper bound
			'			) OR ( '
			'				r_end = end '
			# - record with lower bound only less than this upper bound
			'			) OR ( '
			'				r_end IS NULL '
			'				AND '
			'				r_start < end '
			'			) '
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
			'	input.name as input, '
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
			'	input: input, '
			'	value: CASE WHEN access THEN r.value ELSE "ACCESS DENIED" END, '
			'	submitted_at: submitted_at, '
			'	user: CASE WHEN access THEN user ELSE partner END, '
			'	access: access, '
			'	found: True, '
			'	conflict: True '
			' } '
			' ORDER BY toLower(input) '
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
			'inputs_list': record_data['selected_inputs'],
			'inputs_dict': record_data['inputs_dict']
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
			' UNWIND $inputs_list as input_name '
			'	WITH item, input_name, $inputs_dict[input_name] as value '
		)
		if record_data['item_level'] != 'field':
			statement += (
				', field '
			)
		statement += (
			'	MATCH '
			'		(:RecordType {name_lower: toLower($record_type)})'
			'		<-[:OF_TYPE]-(input: Input '
			'			{name_lower: toLower(input_name)} '
			'		)-[:AT_LEVEL]->(:ItemLevel {name_lower: toLower($item_level)}) '
			'	MATCH '
			'		(:User '
			'			{ username_lower: toLower($username) } '
			'		)-[:SUBMITTED]->(: Submissions) '
			'		-[:SUBMITTED]->(us: Records) '
			'	MERGE (input) '
		)

		if record_data['item_level'] == 'field':
			statement += (
				'	<-[:FOR_INPUT]-(if: ItemInput) '
				'	-[:FOR_ITEM]->(item) '
				' WITH '
				'	item, input, if, us, '
			)
		else:
			statement += (
				'	<-[:FOR_INPUT]-(ff: FieldInput) '
				'	-[:FROM_FIELD]->(field) '
				' MERGE '
				'	(ff) '
				'	<-[:FOR_INPUT]-(if: ItemInput) '
				'	-[:FOR_ITEM]->(item) '
				' WITH '
				'	item, input, if, ff, us, '
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
			# track user submissions through /User/Field/Input container
			'				MERGE '
			'					(us)-[:SUBMITTED]->(uff:UserFieldInput) '
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
			'					(uff)-[s1:SUBMITTED {time: datetime.transaction().epochMillis}]->(r) '
			'				'
			' ) '
			' WITH '
			'	r, input, '
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
			'	<-[s: SUBMITTED]-(: UserFieldInput) '
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
			'	input: input.name, '
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
			' ORDER BY input.name_lower '
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
		if record_type in ['trait', 'curve']:
			headers = ['UID', 'Input', 'Time', 'Submitted by', 'Submitted at', 'Value']
		elif record_type == 'condition':
			headers = ['UID', 'Input', 'Start', 'End', 'Submitted by', 'Submitted at', 'Value']
		else:  # record_type == 'property'
			headers = ['UID', 'Input', 'Submitted by', 'Submitted at', 'Value']
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
						record['input'],
						start_time,
						end_time,
						record['user'],
						submitted_at
					]
			elif record_type in ['trait', 'curve']:
				if record['time']:
					time = datetime.datetime.utcfromtimestamp(int(record['time']) / 1000).strftime("%Y-%m-%d %H:%M")

				else:
					time = ""
				row_data = [
						str(record['UID']),
						record['input'],
						time,
						record['user'],
						submitted_at
					]
			else:
				row_data = [
					str(record['UID']),
					record['input'],
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

