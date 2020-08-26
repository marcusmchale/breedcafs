
class TableRowParser:
	def __init__(self, record_type, input_variables, row_fields, value_fields, row_errors):
		self.record_type = record_type
		self.input_variables = input_variables
		self.required = app.config['REQUIRED_FIELDNAMES'][record_type]
		self.row_fields = row_fields
		self.value_fields = value_fields
		self.row_errors = row_errors
		self.uid_set = set()

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
				self.check_item_level(row_index, field, value)
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

		uid_letters = [k for k in uid if k in app.config['UID_LETTERS'].keys()]
		if len(uid_letters) > 1:
			self.add_error(row_index, field, 'Malformed UID')
			return

		if not uid_letters:  # if no recognised letters then should be a field uid
			ids = [uid]
		else:
			ids = uid.split('_%s ' % uid_letters[0])
		try:
			[int(n) for n in ids]
		except ValueError:
			self.add_error(row_index, field, 'Malformed UID')
			return

		if len(ids) > 2:
			self.add_error(row_index, field, 'Malformed UID')
			return

		self.uid_set.add(uid)

	def check_item_level(self, row_index, field, value):
		if '_' in value:
			item_id = value.split('_')[1]
			item_level = app.config['UID_LETTERS'][item_id[0]]
		else:
			item_level = 'field'
		for input_variable in self.input_variables:
			if item_level not in input_variable['item_levels']:
				self.add_error(
					row_index,
					field,
					'The input variable "%s" cannot be recorded against %s level items' % (
						input_variable['name'],
						item_level
					)
				)

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
