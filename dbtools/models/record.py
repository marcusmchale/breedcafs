from app import app, logging
from app.models.queries import Query
from app.cypher import Cypher
from datetime import datetime, timedelta
from app.models.parsers import Parsers


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
		uid = row_dict['uid'].strip().lower()
		replicate = None
		if '.' in uid:
			uid, replicate = uid.split('.')
			replicate = int(replicate)
		if '_' in uid:
			field_uid = uid.split('_')[0]
		else:
			uid = int(uid)
			field_uid = uid
		self.uid = uid
		self.replicate = replicate
		self.field_uid = field_uid
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


class GenericRecord(Record):
	def __init__(self, row_index, row_dict, row_errors):
		super().__init__(row_index, row_dict, row_errors)
		self.value = None

	def parse_value(self, field, value, input_format, categories=None):
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
		if 'date' in field:
			try:
				datetime.strptime(value, '%Y-%m-%d')
				self.value = value
			except ValueError:
				self.add_error(field, '%s must be in "YYYY-MM-DD" format (ISO 8601), e.g. "2020-07-29"' % field)
		elif 'time' in field:
			try:
				datetime.strptime(value, '%H:%M')
				self.value = value
			except ValueError:
				self.add_error(field, '%s must be in "hh:mm" format (ISO 8601), e.g. "13:01"' % field)
		elif field in {
				'assign tree to block by id', 'assign block to block by id'
			}:
			try:
				self.value = int(value)
			except ValueError:
				self.add_error(field, 'Value for "%s" must be an integer' % field)
		elif field == 'assign sample to block(s) by name':
			self.value = Parsers.parse_name_list(value)
		elif field in {
			'assign sample to sample(s) by id',
			'assign sample to tree(s) by id',
			'assign sample to block(s) by id'
		}:
			try:
				self.value = Parsers.parse_range_list()
			except ValueError:
				self.add_error(
					field,
					'Value for "%s" can be a single integer, a range of integers separated by "-" '
					'or a comma separated list of either of these.' % field
				)
		else:
			self.value = value

	def parse_numeric_value(self, field, value):
		try:
			self.value = float(value)
		except ValueError:
			self.add_error(field, '%s values must be numeric' % field)

	def parse_percent_value(self, field, value):
		try:
			self.value = float(value.replace('%', ''))
		except ValueError:
			self.add_error(field, '%s values must be numeric (optional "%%" character is ignored)' % field)

	def parse_counter_value(self, field, value):
		try:
			value = int(value)
			if value >= 0:
				self.value = value
			else:
				self.add_error(field, '%s values must be positive integers or 0' % field)
		except ValueError:
			self.add_error(field, '%s values must be integers' % field)

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
			self.value = value
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
		try:
			value_index = next(i for i, v in enumerate(categories) if v.lower == value.lower())
			self.value = categories[value_index]
		except StopIteration:
			self.add_error(
				field,
				'%s values must be one of the following categories: \n %s' % (field, '\n'.join(categories))
			)

	def parse_multicat_value(self, field, value, categories):
		value_indices = []
		for value in value.split(','):
			try:
				value_indices.append = next(i for i, v in enumerate(categories) if v.lower == value.lower())
			except StopIteration:
				self.add_error(
					field,
					(
						'%s values must be the following categories (or a comma separated list thereof): \n %s'
						% (field, '\n'.join(categories))
					)
				)
		value_indices.sort()
		self.value = [categories[i] for i in value_indices]


class Property(GenericRecord):
	def __init__(self, row_index, row_dict, row_errors):
		super().__init__(row_index, row_dict, row_errors)
		self.value = None

	def create(self, field, row_dict, input_format, categories = None):
		if field not in row_dict:
			raise TypeError('Field must be in the row dict')

		if self.replicate:
			self.add_error('uid', 'Replicates are not supported for property records')

		value = row_dict[field].strip()
		self.parse_value(field, value, input_format, categories)


class Trait(GenericRecord):
	def __init__(self, row_index, row_dict, row_errors):
		super().__init__(row_index, row_dict, row_errors)
		self.value = None
		self.text_date = None
		self.text_time = None
		self.epoch_ms_time = None

	def create(self, field, row_dict, input_format, categories = None):
		if field not in row_dict:
			raise TypeError('Field must be in the row dict')

		value = row_dict[field].strip()
		self.parse_value(field, value, input_format, categories)
		self.text_date = row_dict['date'].strip()
		if 'time' in row_dict and row_dict['time'].strip():
			self.text_time = row_dict['time'].strip()
			self.epoch_ms_time = int(
				(
						datetime.strptime('%s %s' % (self.text_date, self.text_time), '%Y-%m-%d %H:%M') -
						datetime(1970, 1, 1)
				).total_seconds() * 1000
			)
		else:
			self.epoch_ms_time = int(
				(
						datetime.strptime(self.text_date, '%Y-%m-%d') +
						timedelta(hours=12) -
						datetime(1970, 1, 1)
				).total_seconds() * 1000
			)


class Condition(GenericRecord):
	def __init__(self, row_index, row_dict, row_errors):
		super().__init__(row_index, row_dict, row_errors)
		self.value = None
		self.text_start_date = None
		self.text_start_time = None
		self.text_end_date = None
		self.text_end_time = None
		self.epoch_ms_start = None
		self.epoch_ms_end = None

	def create(self, field, row_dict, input_format, categories = None):
		if field not in row_dict:
			raise TypeError('Field must be in the row dict')
		value = row_dict[field].strip()
		self.parse_value(field, value, input_format, categories)
		if 'start date' in row_dict and row_dict['start date'].strip():
			self.text_start_date = row_dict['start date'].strip()
		if 'start time' in row_dict and row_dict['start time'].strip():
			self.text_start_time = row_dict['start time'].strip()
		if self.text_start_date and self.text_start_time:
			self.epoch_ms_start = int(
				(
						datetime.strptime('%s %s' % (self.text_start_date, self.text_start_time), '%Y-%m-%d %H:%M') -
						datetime(1970, 1, 1)
				).total_seconds() * 1000
			)
		elif self.text_start_date:
			self.epoch_ms_start = int(
				(
						datetime.strptime(self.text_start_date, '%Y-%m-%d') -
						datetime(1970, 1, 1)
				).total_seconds() * 1000
			)
		if 'end date' in row_dict and row_dict['end date'].strip():
			self.text_end_date = row_dict['end date'].strip()
		if 'end time' in row_dict and row_dict['end time'].strip():
			self.text_end_time = row_dict['end time'].strip()
		if self.text_end_date and self.text_end_time:
			self.epoch_ms_end = int(
				(
						datetime.strptime('%s %s' % (self.text_end_date, self.text_end_time), '%Y-%m-%d %H:%M') -
						datetime(1970, 1, 1)
				).total_seconds() * 1000
			)
		elif self.text_end_date:
			self.epoch_ms_end = int(
				(
						datetime.strptime(self.text_end_date, '%Y-%m-%d') +
						timedelta(days=1) -
						datetime(1970, 1, 1)
				).total_seconds() * 1000
			)


class Curve(Record):
	def __init__(self, row_index, row_dict, row_errors):
		super().__init__(row_index, row_dict, row_errors)
		self.x_values = []
		self.y_values = []
		self.text_date = None
		self.text_time = None
		self.epoch_ms_time = None

	def create(self, field, row_dict):
		if field not in row_dict:
			raise TypeError('Field must be in the row dict')

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

		# sort the x and y value lists based on x values
		sorted_x_and_y = sorted(zip(self.x_values, self.y_values), key=lambda pair: pair[0])
		self.x_values = [x for (x, y) in sorted_x_and_y]
		self.y_values = [y for (x, y) in sorted_x_and_y]

		text_date = row_dict['date'].strip()
		if not text_date:
			self.add_error('date', 'Date value required')
		else:
			self.text_date = text_date

		if 'time' in row_dict and row_dict['time'].strip():
			self.text_time = row_dict['time'].strip()
			self.epoch_ms_time = int(
				(
						datetime.strptime('%s %s' % (self.text_date, self.text_time), '%Y-%m-%d %H:%M') -
						datetime(1970, 1, 1)
				).total_seconds() * 1000
			)
		else:
			self.epoch_ms_time = int(
				(
						datetime.strptime(self.text_date, '%Y-%m-%d') +
						timedelta(hours=12) -
						datetime(1970, 1, 1)
				).total_seconds() * 1000
			)

