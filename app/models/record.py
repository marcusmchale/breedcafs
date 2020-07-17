from app import app, logging
from app.models.queries import Query
from app.cypher import Cypher
from datetime import datetime
from app.models.parsers import Parsers


class TableRowParser:
	def __init__(self, record_type, input_variables, row_fields, value_fields, row_errors):
		self.record_type = record_type
		self.input_variables = input_variables
		self.required = app.config['REQUIRED_FIELDNAMES'][record_type]
		self.row_fields = row_fields
		self.value_fields = value_fields
		self.row_errors = row_errors


	def add_error(self, row_index, field, message):
		if row_index not in self.row_errors:
			self.row_errors[row_index] = {}
		if field not in self.row_errors[row_index]:
			self.row_errors[row_index][field] = set()
		self.row_errors[row_index][field].add(message)

	def check_row_fields(self, row_index, row_dict):
		for field in self.row_fields:
			value = row_dict[field].strip()
			if field in self.required and not value:
				self.add_error(row_index, field, '%s is required' % field)
				return
			if field == 'uid':
				self.check_uid_format(row_index, field, value)
			elif field == 'person':
				self.check_person_format(row_index, field, value)
			elif field in ['date', 'start date', 'end date']:
				self.check_date_format(row_index, field, value)
			elif field in ['time', 'start time', 'end time']:
				self.check_time_format(row_index, field, value)

	def check_uid_format(self, row_index, field, value):
		uid = value.lower()
		if '.' in uid:
			try:
				uid, replicate = uid.split('.')
				try:
					int(replicate)
				except ValueError:
					self.add_error(row_index, field, 'Replicate code must be an integer')
					return
			except ValueError:
				self.add_error(row_index, field, 'Malformed UID')
				return
		uid_letters = [k for k in uid if k in app.config['UID_LETTERS']]
		if len(uid_letters) > 1:
			self.add_error(row_index, field, 'Malformed UID')
		if not uid_letters:  # if no recognised letters then should be a field uid
			ids = [uid]
		else:
			ids = uid.split(uid_letters[0])
		try:
			[int(n) for n in ids]
		except ValueError:
			self.add_error(row_index, field, 'Malformed UID')

	def check_person_format(self, row_index, field, value):
		max_len = app.config['PERSON_FIELD_MAX_LEN']
		if len(value) > max_len:
			self.add_error(row_index, field, ('Maximum character length for %s is %s' % (field, max_len)))

	def check_date_format(self, row_index, field, value):
		try:
			datetime.strptime(value, '%Y-%m-%d')
		except ValueError:
			self.add_error(row_index, field, '%s must be in "YYYY-MM-DD" format (ISO 8601), e.g. "2020-07-14"' % field)

	def check_time_format(self, row_index, field, value):
		try:
			datetime.strptime(value, '%H:%M')
		except ValueError:
			self.add_error(row_index, field, '%s must be in "hh:mm" format (ISO 8601), e.g. "13:01"' % field)

	# these aren't relevant to tables, keeping for copy to db format parser
	#def check_integer_format(self, row_index, field, value):
	#	try:
	#		int(value)
	#	except ValueError:
	#		self.add_error(row_index, field, '%s must be an integer' % field)
	#
	#def check_date_time_format(self, row_index, field, value):
	#	try:
	#		datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
	#	except ValueError:
	#		self.add_error(
	#			row_index,
	#			field,
	#			'%s must be in "YYYY-MM-DD hh:mm:ss" format (ISO 8601), e.g. "2020-07-14"' % field
	#		)
	#
	#def check_period_format(self, row_index, field, value):
	#	try:
	#		start, end = value.split(' - ')
	#	except ValueError:
	#		self.add_error(
	#			row_index,
	#			field,
	#			'%s must contain both start and end details separated by " - ". ' % field
	#		)
	#		return
	#	try:
	#		if not start.lower() == 'undefined':
	#			datetime.strptime(start, '%Y-%m-%d %H:%M')
	#		if not end.lower() == 'undefined':
	#			datetime.strptime(end, '%Y-%m-%d %H:%M')
	#	except ValueError:
	#		self.add_error(
	#			row_index,
	#			field,
	#			'%s start and end must be either "Undefined" or in "YYYY-MM-DD hh:mm" format (ISO 8601),'
	#			' e.g. "2020-07-14 13:01"' % field
	#		)


class Record:
	@classmethod
	def select(cls, record_type):
		if record_type == 'property':
			return Property
		elif record_type == 'trait':
			return Trait
		elif record_type == 'condition':
			return Condition
		elif record_type == 'curve':
			return Curve
		else:
			raise ValueError('Record type not recognised')

	def __init__(self, row_index, row_dict, row_errors):
		self.row_index = row_index
		self.uid = row_dict['uid'].strip().lower()
		if 'person' in row_dict and row_dict['person']:
			self.person = row_dict['person']
		else:
			self.person = None
		self.row_errors = row_errors

	def add_error(self, field, message):
		if self.row_index not in self.row_errors:
			self.row_errors[self.row_index] = {}
		if field not in self.row_errors[self.row_index]:
			self.row_errors[self.row_index][field] = set()
		self.row_errors[self.row_index][field].add(message)


class Curve(Record):
	def __init__(self, row_index, row_dict, row_errors):
		super().__init__(row_index, row_dict, row_errors)
		self.x_values = []
		self.y_values = []
		self.date = None
		self.time = None

	def create(self, field, row_dict):
		if not isinstance(field, list):
			raise TypeError(
				'Curve records take a list of fields rather than a single field. '
				'These correspond to the x-values.'
			)
		for x in field:
			try:
				y = float(row_dict[x])
				if y:
					self.x_values.append(float(x))
					self.y_values.append(y)
			except ValueError:
				self.add_error(x, 'Curve record types only support numeric values')
		date = row_dict['date'].strip()
		if not date:
			self.add_error('date', 'Date value required')
		else:
			self.date = date
		if 'time' in row_dict:
			time = row_dict['time'].strip()
			if time:
				self.time = time


class GenericRecord(Record):
	def __init__(self, row_index, row_dict, row_errors):
		super().__init__(row_index, row_dict, row_errors)
		self.value = None

	def parse_value(self, field, value, input_format, categories = None):
		value = value.strip()
		if input_format == 'text':
			self.parse_text_value(field, value)
		elif input_format == 'numeric':
			self.parse_numeric_value(field, value)
		elif input_format == 'percent':
			self.parse_percent_value(field, value)
		elif input_format == 'counter':
			self.parse_counter_value(field, value)
		elif input_format == 'boolean':
			self.parse_boolean_value(field, value)
		elif input_format == 'location':
			self.parse_location_value(field, value)
		elif input_format == 'date':
			self.parse_date_value(field, value)
		elif input_format == 'categorical':
			self.parse_categorical_value(field, value, categories)
		elif input_format == 'multicat':
			self.parse_multicat_value(field, value, categories)

	def parse_text_value(self, field, value):
		if field == 'assign tree to block by id':
			try:
				self.value = int(value)
			except ValueError:
				self.add_error(field, 'Value for "%s" must be an integer' % field)
		elif field == 'assign tree to block by name':
			self.value = value.lower()
		elif field == 'assign sample to block(s) by name':
			self.value =  Parsers.parse_name_list(value)
		elif field in [
			'assign sample to sample(s) by id',
			'assign sample to tree(s) by id',
			'assign sample to block(s) by id'
		]:
			try:
				self.value = Parsers.parse_range_list()
			except ValueError:
				self.add_error(
					field,
					'Value for "%s" can be a single integer, a range of integers separated by "-" '
					'or a comma separated list of either of these.' %field
				)
		elif 'time' in field:
			try:
				datetime.strptime(value, '%H:%M')
				self.value = value
			except ValueError:
				self.add_error(field, '%s must be in "hh:mm" format (ISO 8601), e.g. "13:01"' % field)

	def parse_numeric_value(self, field, value):
		try:
			self.value = float(value)
		except ValueError:
			self.add_error(field, '%s values must be numeric' % field)

	def parse_percent_value(self, field, value):
		try:
			self.value = float(value.replace('%', ''))
		except ValueError:
			self.add_error(field, '%s values must be numeric (optional "%" character is ignored)' % field)

	def parse_counter_value(self, field, value):
		try:
			value = int(value)
			if not value >= 0:
				self.add_error(field, '%s values must be positive integers or 0' % field)
		except ValueError:
			self.add_error(field, '%s values must be integers' % field)
		else:
			self.value = value

	def parse_boolean_value(self, field, value):
		value = value.strip().lower()
		yes = ['yes', 'y','true','t', '1']
		no = ['no', 'n','0','false','f', '0']
		if value in yes:
			self.value = True
		elif value in no:
			self.value = False
		else:
			self.add_error(
				field,
				'%s values must be either True (%s) or False (%s)' % (field, ', '.join(yes), ', '.join(no))
			)

	def parse_date_value(self, field, value):
		try:
			datetime.strptime(value, '%Y-%m-%d')
		except ValueError:
			self.add_error(field, '%s must be in YYYY-MM-DD format, e.g. "2020-07-14"' % field)

	def parse_location_value(self, field, value):
		try:
			lat, long = value.split(';')
			float(lat)
			float(long)
			self.value = lat, long
		except ValueError:
			self.add_error(
				field,
				'%s value must be two numeric values (corresponding to latitude and longitude) '
				'separated by ";", e.g. (53.270962;-9.062691)' % field)

	def parse_categorical_value(self, field, value, categories):
		if value.lower() in [cat.lower() for cat in categories]:
			self.value = value
		else:
			self.add_error(
				field,
				'%s values must be one of the following categories: \n %s' % (field, '\n'.join(categories))
			)

	def parse_multicat_value(self, field, value, categories):
		values = value.split(',')
		all_valid = True
		invalid_values = []
		for v in set(values):
			if v.strip() and v.lower() not in [cat.lower() for cat in categories]:
				all_valid = False
				invalid_values.append(v)
		if not all_valid:
			self.add_error(
				field,
				'The following values are not supported categories for %s: '
				'\n %s \n The following categories are supported: %s' %
				(field, '\n'.join(invalid_values), '\n'.join(categories))
			)








class Property(GenericRecord):
	def __init__(self, row_index, row_dict, row_errors):
		super().__init__(row_index, row_dict, row_errors)
		self.value = None


	def create(self, field, row_dict):

		if not field in row_dict:
			raise TypeError('Field must be in the row dict')

		if self.replicate:
			self.add_error('uid', 'Replicates are not supported for property records')
		value = row_dict[field].strip()
		self.check_value(field, value)


class Trait(GenericRecord):
	def __init__(self, row_index, row_dict, row_errors):
		super().__init__(row_index, row_dict, row_errors)
		self.value = None
		self.date = None
		self.time = None

	def create(self, row_dict, input_variable=None):
		if not input_variable:
			raise TypeError('Input variable required for trait records to look up the value in the row_dict')

		self.value = row_dict[input_variable].strip()
		date = row_dict['date'].strip()
		if not date:
			self.add_error('date', 'Date value required')
		else:
			self.date = date
			self.check_date('date', date)
		if 'time' in row_dict:
			time = row_dict['time'].strip()
			if time:
				self.time = time
				self.check_time('time', time)


class Condition(GenericRecord):
	def __init__(self, row_index, row_dict, row_errors):
		super().__init__(row_index, row_dict, row_errors)
		self.value = None
		self.start_date = None
		self.start_time = None
		self.end_date = None
		self.end_time = None

	def create(self, row_dict, input_variable=None):
		if not input_variable:
			raise TypeError('Input variable required in condition records to look up the value in row_dict')
		self.value = row_dict[input_variable].strip()
		if 'start date' in row_dict:
			start_date = row_dict['start date'].strip()
			if start_date:
				self.start_date = start_date
				self.check_date('start date', start_date)
		if 'start time' in row_dict:
			start_time = row_dict['start time'].strip()
			if start_time:
				self.start_time = start_time
				self.check_time('start time', start_time)
		if 'end date' in row_dict:
			end_date = row_dict['end date'].strip()
			if end_date:
				self.end_date = end_date
				self.check_date('end date', end_date)
		if 'end time' in row_dict:
			end_time = row_dict['end time'].strip()
			if end_time:
				self.end_time = end_time
				self.check_time('end time', end_time)


Class new:

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
						'assign name',
						'assign tree to block by name',
						'assign sample to block by name',
						'assign sample to block(s) by ID',
						'assign sample to tree(s) by ID',
						'assign sample to sample(s) by ID',
						'specify tissue',
						'harvest date',
						'harvest time',
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
						# ' WITH item, update_variety '
						# '	WHERE of_current_variety IS NULL '
						# '	AND source_sample IS NULL '
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
				if 'assign tree to block by name' in record_data['selected_inputs']:
					update_block_statement = match_item_query[0]
					update_block_parameters = match_item_query[1]
					update_block_parameters['username'] = record_data['username']
					update_block_parameters['assign_tree_to_block_by_name'] = record_data['inputs_dict'][
						'assign tree to block by name']
					update_block_statement += (
						' WITH DISTINCT item, field '
						' MATCH '
						'	(block_update: Block { '
						'		name_lower: toLower(trim($assign_tree_to_block_by_name)) '
						'	})-[:IS_IN]->(:FieldBlocks)-[:IS_IN]->(field) '
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
				if 'assign field sample to block by name' in record_data['selected_inputs']:
					update_block_statement = match_item_query[0]
					update_block_parameters = match_item_query[1]
					update_block_parameters['username'] = record_data['username']
					update_block_parameters[
						'assign_sample_to_block_by_name'
					] = record_data['inputs_dict']['assign field sample to block by name']
					update_block_statement += (
						' WITH DISTINCT item, field '
						' MATCH '
						'	(block: Block { '
						'		name_lower: toLower(trim($assign_sample_to_block_by_name)) '
						'	})-[:IS_IN]->(:FieldBlocks)-[:IS_IN]->(field) '
						'	<-[:FROM]-(:ItemSamples)<-[from:FROM]-(item) '
						' MERGE '
						' 	(is: ItemSamples)-[:FROM]-> '
						' 	(block) '
						' CREATE '
						' 	(item)-[:FROM]->(is) '
						' DELETE from '
					)
					tx.run(update_block_statement, update_block_parameters)
				if 'assign field sample to block(s) by id' in record_data['selected_inputs']:
					update_block_statement = match_item_query[0]
					update_block_parameters = match_item_query[1]
					update_block_parameters['username'] = record_data['username']
					update_block_parameters[
						'assign_sample_to_block_by_id'
					] = record_data['inputs_dict']['assign field sample to block(s) by id']
					update_block_statement += (
						' WITH DISTINCT item, field '
						' MATCH '
						'	(block: Block)-[:IS_IN]->(:FieldBlocks)-[:IS_IN]->(field) '
						'	<-[:FROM]-(:ItemSamples)<-[from:FROM]-(item) '
						' WHERE block.id IN extract(x in split($assign_sample_to_block_by_id, ",") | toInteger(trim(x))) '
						' MERGE '
						' 	(is: ItemSamples)-[:FROM]-> '
						' 	(block) '
						' CREATE '
						' 	(item)-[:FROM]->(is) '
						' DELETE from '
					)
					tx.run(update_block_statement, update_block_parameters)
				if 'assign field sample to tree(s) by id' in record_data['selected_inputs']:
					update_trees_statement = match_item_query[0]
					update_trees_parameters = match_item_query[1]
					update_trees_parameters['username'] = record_data['username']
					update_trees_parameters[
						'assign_sample_to_tree_by_id'
					] = record_data['inputs_dict']['assign field sample to tree(s) by id']
					update_trees_statement += (
						' WITH DISTINCT item, field '
						' MATCH '
						'	(item)'
						'	-[: FROM]->(: ItemSamples) '
						'	-[: FROM]->(field) '
						'	<-[:IS_IN]-(: FieldTrees) '
						'	<-[:IS_IN]-(tree: Tree) '
						' WHERE tree.id IN extract(x in split($assign_sample_to_tree_by_id, ",") | toInteger(trim(x))) '
						' MERGE '
						' 	(item_samples: ItemSamples)'
						'	-[: FROM]->(tree) '
						' CREATE '
						' 	(item)-[:FROM]->(item_samples) '
					)
					tx.run(update_trees_statement, update_trees_parameters)
				if 'assign field sample to sample(s) by id' in record_data['selected_inputs']:
					update_samples_statement = match_item_query[0]
					update_samples_parameters = match_item_query[1]
					update_samples_parameters['username'] = record_data['username']
					update_samples_parameters[
						'assign_sample_to_sample_by_id'
					] = record_data['inputs_dict']['assign field sample to sample(s) by id']
					update_samples_statement += (
						' WITH DISTINCT item, field '
						' MATCH '
						'	(item)'
						'	-[from_field: FROM]->(: ItemSamples) '
						'	-[: FROM]->(field) '
						'	<-[: FROM | IS_IN*]-(sample: Sample) '
						' WHERE sample.id IN extract(x in split($assign_sample_to_sample_by_id, ",") | toInteger(trim(x))) '
						' CREATE '
						' 	(item)-[from_sample:FROM]->(sample) '
					)
					tx.run(update_samples_statement, update_samples_parameters)
				if 'assign name' in record_data['selected_inputs']:
					update_name_statement = match_item_query[0] + ' WITH DISTINCT item '
					update_name_parameters = match_item_query[1]
					update_name_parameters['assign name'] = record_data['inputs_dict']['assign name']
					update_name_statement += (
						' SET item.name = CASE WHEN item.name IS NULL '
						'	THEN $name '
						'	ELSE item.name '
						'	END '
					)
					tx.run(update_name_statement, update_name_parameters)
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
					update_planted_parameters['date'] = record_data['inputs_dict']['planting date']
					update_planted_statement += (
						' SET item.date = CASE WHEN item.date IS NULL '
						'	THEN $date '
						'	ELSE item.date '
						'	END '
						' SET item.time = CASE WHEN item.date IS NOT NULL THEN '
						'	apoc.date.parse(item.date + " 12:00", "ms", "yyyy-MM-dd HH:mm") '
						'	END '
					)
					tx.run(update_planted_statement, update_planted_parameters)
				if 'harvest date' in record_data['selected_inputs']:
					update_harvest_date_statement = match_item_query[0] + ' WITH DISTINCT item '
					update_harvest_date_parameters = match_item_query[1]
					update_harvest_date_parameters['date'] = record_data['inputs_dict']['harvest date']
					update_harvest_date_statement += (
						' MATCH '
						'	(item)-[:FROM]->(:ItemSamples) '
						'	SET item.date = CASE '
						'		WHEN item.date IS NULL '
						'			THEN $date '
						'			ELSE item.date '
						'			END '
						'	SET item.time = CASE '
						'		WHEN item.date IS NOT NULL AND item.time_of_day IS NOT NULL '
						'		THEN '
						'			apoc.date.parse(item.date + " " + item.time, "ms", "yyyy-MM-dd HH:mm") '
						'		WHEN item.date IS NOT NULL AND item.time_of_day IS NULL '
						'		THEN '
						'			apoc.date.parse(item.date + " 12:00", "ms", "yyyy-MM-dd HH:mm") '
						'		WHEN item.date IS NULL '
						'		THEN '
						'			Null '
						'		END '
					)
					tx.run(update_harvest_date_statement, update_harvest_date_parameters)
				if 'harvest time' in record_data['selected_inputs']:
					update_harvest_time_statement = match_item_query[0] + ' WITH DISTINCT item '
					update_harvest_time_parameters = match_item_query[1]
					update_harvest_time_parameters['time'] = record_data['inputs_dict']['harvest time']
					update_harvest_time_statement += (
						' MATCH '
						'	(item)-[:FROM]->(:ItemSamples) '
						'	SET item.time_of_day = CASE '
						'		WHEN item.time_of_day IS NULL '
						'			THEN $time '
						'			ELSE item.time_of_day '
						'			END '
						'	SET item.time = CASE '
						'		WHEN item.date IS NOT NULL AND item.time_of_day IS NOT NULL '
						'		THEN '
						'			apoc.date.parse(item.date + " " + item.time_of_day, "ms", "yyyy-MM-dd HH:mm") '
						'		WHEN item.date IS NOT NULL AND item.time_of_day IS NULL '
						'		THEN '
						'			apoc.date.parse(item.date + " 12:00", "ms", "yyyy-MM-dd HH:mm") '
						'		WHEN item.date IS NULL '
						'		THEN '
						'			Null '
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
			'			time: $time, '
			'			replicate = 0 '
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
			# if this is for another replicate then no conflict we can treat it like another item
			'	r.replicate = replicate '
			# If don't have access or if have access and values don't match then potential conflict 
			# time parsing to allow reduced specificity in the relevant time range is below
			'	AND '
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
			'		replicate: 0, '
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
				submitted_at = datetime.datetime.utcfromtimestamp(int(record['submitted_at']) / 1000).strftime(
					"%Y-%m-%d %H:%M")
			else:
				submitted_at = ""
			row_string = '<tr><td>'
			if record_type == 'condition':
				if record['start']:
					start_time = datetime.datetime.utcfromtimestamp(int(record['start']) / 1000).strftime(
						"%Y-%m-%d %H:%M")
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

