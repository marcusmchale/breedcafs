


class RowParseResult:
	def __init__(self, row):
		self.row = row
		self.conflicts = {}
		self.errors = {}
		# todo store these on the interpreter side (html_table function) not in the result object
		self.error_comments = {
			"date": {
				"format": "Date format does not match the required input (e.g. 2018-01-01)"
			},
			"start date": {
				"format": "Date format does not match the required input (e.g. 2018-01-01)",
				"order": "Please be sure to start before you finish!"
			},
			"end date": {
				"format": "Date format does not match the required input (e.g. 2018-01-01)",
				"order": "Please be sure to start before you finish!"
			},
			"time": {
				"format": "Time format does not match the required input (e.g. 13:00)",
				"db_format": "Format does not match the required input (e.g. 2018-01-01 13:00)"

			},
			"start time": {
				"format": "Time format does not match the required input (e.g. 13:00)",
				"order": "Please be sure to start before you finish!"

			},
			"end time": {
				"format": "Time format does not match the required input (e.g. 13:00)",
				"order": "Please be sure to start before you finish!"
			},
			"submitted at": {
				"format": "Format does not match the required input (e.g. 2018-01-01 13:00:00)",
			},
			"replicate": {
				"format": "Format does not match the required input, expected an integer value",
			},
			"period": {
				"format": (
					"Format does not match the required input, expected a range of dates with times "
					"(e.g. 2018-01-01 13:00 - 2019-02-02 14:00)"
				),
			},
			"uid": {
				"format": "UID doesn't match BreedCAFS pattern: \n"
				"  - Field UID should be an integers (e.g. '1')\n"
				"  - Block UID should include the Field and Block ID separated by '_B' (e.g. '1_B1')\n"
				"  - Tree UID should include the Field and Tree ID separated by '_T' (e.g. '1_T1')\n"
				"  - Sample UID should include the Field and Sample ID separated by '_S' (e.g. '1_S1')\n",
				"missing": "This UID is not found in the database. "
			},
			"input variable": {
				"missing": (
					"This input variable is not found in the database. "
					"Please check the spelling and "
					"that this input variable is found among those supported by BreedCAFS "
					"for the level of data you are submitting."
				)
			},
			"other": {
				"format": {
					"numeric": "Expected a numeric value",
					"boolean": "Expected a boolean value ('True/False or Yes/No')",
					"percent": "Expected a percent or simple numeric value (e.g. 50% or 50)",
					"location": (
						"Location coordinates are recorded as two numeric values "
						"separated by a semicolon (';') e.g. ('53.2707;-9.0568')"
					),
					"date": "Expected date value format 'YYYY-MM-DD'(e.g. 2018-01-01)",
					"counter": "Expected an integer value (no decimal points or fractions)",
					"multicat": "Expected any of the following categories separated by a colon (':'): \n",
					"categorical": "Expected one of the following categories only: \n",
					"text": "Text field value error. \n"
				},
				"conflict":  "Conflicts were found with existing records: \n"
			}
		}

	def add_error(
			self,
			field,
			error_type,
			# optional arguments only relevant to value errors
			input_name=None,
			input_format=None,
			category_list=None,
			# optional arguments only relevant to conflicts
			conflicts=None
	):
		if field not in self.errors:
			self.errors[field] = []
		self.errors[field].append({
				'error_type': error_type,
				'input_name': input_name,
				'input_format': input_format,
				'category_list': category_list,
				'conflicts': conflicts
		})

	def headers(self):
		return list(self.row.keys())

	def html_row(self, fieldnames):
		fieldnames = [i.lower() for i in fieldnames]
		formatted_cells = {}
		for field in list(self.errors.keys()):
			if field not in fieldnames:  # curve conflicts return this
				# get x_values
				x_values = []
				for x in fieldnames:
					try:
						float(x)
						x_values.append(x)
					except ValueError:
						pass
				if x_values:
					for error in self.errors[field]:
						if error['conflicts']:
							# only show 3 conflicts or "title" attribute is overloaded
							# TODO implement better tooltips to include as a table rather than "title" attribute
							for conflict in itertools.islice(error['conflicts'], 3):
								if isinstance(conflict['existing_value'], list):
									submitted_by_string = ''.join([
										'Submitted by: ',
										conflict['user'],
										'\n'
									])
									submitted_at_string = ''.join([
										'Submitted at: ',
										datetime.datetime.utcfromtimestamp(
											int(conflict['submitted at']) / 1000
										).strftime("%Y-%m-%d %H:%M:%S"),
										'\n'
									])
									for x_y in conflict['existing_value']:
										x_value = x_y[0]
										y_value = x_y[1]
										for x in x_values:
											if float(x) == x_value:
												try:
													if y_value != float(self.row[x]):
														formatted_cells[x] = '<td bgcolor = #FFFF00 title = "'
														formatted_cells[x] += self.error_comments['other'][
															error['error_type']]
														formatted_cells[x] += (
															''.join(['Existing value: ', str(y_value), '\n'])

														)
														formatted_cells[x] += ''.join([
															submitted_by_string,
															submitted_at_string
														])
														formatted_cells[x] += '">' + str(self.row[x]) + '</td>'
												except ValueError:
													formatted_cells[x] = '<td bgcolor = #FFFF00 title = "'
													formatted_cells[x] += self.error_comments['other'][
														error['error_type']]
													formatted_cells[x] += (
														''.join(['Existing value: ', str(y_value), '\n'])

													)
													formatted_cells[x] += ''.join([
														submitted_by_string,
														submitted_at_string
													])
													formatted_cells[x] += '">' + str(self.row[x]) + '</td>'
			else:
				formatted_cells[field] = '<td bgcolor = #FFFF00 title = "'
				for error in self.errors[field]:
					field_error_type = error['error_type']
					field_input_name = error['input_name'].lower() if error['input_name'] else None
					field_input_format = error['input_format']
					field_category_list = error['category_list']
					field_conflicts = error['conflicts']
					# if it is a simple error (time format, UID format or UID/Input not found)
					if field in self.error_comments:
						formatted_cells[field] += self.error_comments[field][field_error_type]
					else:
						if field_error_type == 'format':
							formatted_cells[field] += self.error_comments['other'][field_error_type][field_input_format]
							if field_input_name == 'variety name':
								formatted_cells[field] += 'Expected one of the following variety names: \n'
							elif field_input_name == 'variety code':
								formatted_cells[field] += 'Expected one of the following codes: \n'
							elif field_input_name == 'fertiliser n:p:k ratio':
								formatted_cells[field] += 'Expected N:P:K ratio format, e.g. 1:1:1'
							elif field_input_name in [
								'assign tree to block by name',
								'assign field sample to block by name'
							]:
								formatted_cells[field] += (
									'Expected a block name '
								)
							elif field_input_name in [
								'assign sample to sample(s) by id',
								'assign sample to tree(s) by id',
								'assign sample to block(s) by id'
							]:
								formatted_cells[field] += (
									'Expected a comma separated list of integers corresponding to the ID within the field '
								)
							elif 'time' in field_input_name:
								formatted_cells[field] += 'Expected time format as HH:MM e.g. 13:01'
							if field_category_list:
								formatted_cells[field] += ", ".join([i for i in field_category_list])
						elif field_error_type == 'conflict':
							formatted_cells[field] += self.error_comments['other'][field_error_type]
							# only show 3 conflicts or "title" attribute is overloaded
							# TODO implement better tooltips to include as a table rather than "title" attribute
							for conflict in itertools.islice(field_conflicts, 3):
								existing_value = conflict['existing_value']
								if isinstance(existing_value, list):
									existing_value = ', '.join(existing_value)
								formatted_cells[field] += '\n\n'
								formatted_cells[field] += ''.join(
									['Existing value: ', existing_value, '\n']
								)
								if 'time' in conflict and conflict['time']:
									formatted_cells[field] += (
										'Time: '
										+ datetime.datetime.utcfromtimestamp(
											int(conflict['time']) / 1000
										).strftime(
											"%Y-%m-%d %H:%M"
										)
										+ '\n'
									)
								if 'start' in conflict and conflict['start']:
									formatted_cells[field] += (
										'Start: '
										+ datetime.datetime.utcfromtimestamp(
											int(conflict['start']) / 1000
										).strftime(
											"%Y-%m-%d %H:%M"
										)
										+ '\n'
									)
								if 'end' in conflict and conflict['end']:
									formatted_cells[field] += (
										'End: '
										+ datetime.datetime.utcfromtimestamp(
											int(conflict['end']) / 1000
										).strftime(
											"%Y-%m-%d %H:%M"
										)
										+ '\n'
									)
								formatted_cells[field] += ''.join(['Submitted by: ', conflict['user'], '\n'])
								formatted_cells[field] += ''.join([
									'Submitted at: ',
									datetime.datetime.utcfromtimestamp(
										int(conflict['submitted at']) / 1000
									).strftime("%Y-%m-%d %H:%M:%S"),
									'\n'
								])
					formatted_cells[field] += '\n'
				if isinstance(self.row[field], (int, float)):
					value = str(self.row[field])
				else:
					value = self.row[field]
				formatted_cells[field] += '">'
				formatted_cells[field] += value
				formatted_cells[field] += '</td>'
		row_string = '<tr><td>' + str(self.row['row_index']) + '</td>'
		for field in fieldnames:
			if field in formatted_cells:
				row_string += formatted_cells[field]
			else:
				row_string += '<td>' + self.row[field] + '</td>'
		return row_string


class ParseResult:
	def __init__(self, submission_type, worksheet, fieldnames):
		self.submission_type = submission_type
		self.worksheet = worksheet
		if worksheet in app.config['WORKSHEET_NAMES']:
			if worksheet in app.config['REFERENCE_WORKSHEETS']:
				pass
			else:
				self.record_type = worksheet
		else:
			self.record_type = 'curve'
		self.fieldnames = fieldnames
		self.field_errors = {}
		self.field_found = []
		self.errors = {}
		self.unique_keys = set()
		self.duplicate_keys = {}
		self.contains_data = None

	def add_field_error(self, field, error_type):
		self.field_errors[field] = error_type

	# this is needed to create a list of found fields in case the error is found at one level in a table but not others
	# the list is removed from field_errors at the end of parsing
	def add_field_found(self, field):
		self.field_found.append(field)

	def rem_field_error(self, field):
		if self.field_errors:
			if field in self.field_errors:
				del self.field_errors[field]

	def parse_db_row(self, row):
		# all rows with any data considered when db format
		if any([i for i in row]):
			self.contains_data = True
		if row['submitted at']:
			parsed_submitted_at = Parsers.timestamp_db_submitted_at_format(row['submitted at'])
			if not parsed_submitted_at:
				self.merge_error(
					row,
					"submitted at",
					"format"
				)
		else:
			parsed_submitted_at = None
			self.merge_error(row, "submitted at", "format")
		if 'time' in row and row['time']:
			parsed_time = Parsers.db_time_format(row['time'])
			if not parsed_time:
				self.merge_error(
					row,
					"time",
					"db_format",
				)
		else:
			parsed_time = None
		if 'period' in row and row['period']:
			parsed_period = Parsers.db_period_format(row['period'])
			if not parsed_period:
				self.merge_error(
					row,
					"period",
					"format"
				)
		else:
			parsed_period = None
		if 'replicate' in row and row['replicate']:
			try:
				parsed_replicate = int(row['replicate'])
			except ValueError:
				parsed_replicate = None
				self.merge_error(
					row,
					"replicate",
					"format"
				)
		else:
			parsed_replicate = None
		parsed_uid = Parsers.uid_format(row['uid'])
		unique_key = (
			parsed_uid,
			parsed_submitted_at,
			parsed_time,
			parsed_period,
			parsed_replicate,
			row['input variable']
		)
		if unique_key not in self.unique_keys:
			self.unique_keys.add(unique_key)
		else:
			self.duplicate_keys[row['row_index']] = row

	def parse_table_row(self, row):
		# submission_type = self.submission_type
		# check uid formatting
		# Check time, and for trait duplicates in tables simply check for duplicate fields in header
		parsed_uid = Parsers.uid_format(row['uid'])
		if not parsed_uid:
			self.merge_error(
				row,
				"uid",
				"format"
			)
		# check for date time info.
		if self.record_type == 'property':
			unique_key = parsed_uid
		elif self.record_type in ['trait', 'curve']:
			if 'date' in row:
				parsed_date = Parsers.date_format(row['date'])
				# date required for trait data
				if not parsed_date or parsed_date is True:
					self.merge_error(
						row,
						"date",
						"format"
					)
			else:
				parsed_date = None
			if 'time' in row:
				parsed_time = Parsers.time_format(row['time'])
				if not parsed_time:
					self.merge_error(
						row,
						"time",
						"format"
					)
			else:
				parsed_time = None
			if all([
				parsed_date,
				parsed_time,
				parsed_date is not True,
				parsed_time is not True
			]):
				time = datetime.datetime.strptime(parsed_date + ' ' + parsed_time, '%Y-%m-%d %H:%M')
				unique_key = (parsed_uid, time)
			elif parsed_date and parsed_date is not True and parsed_time is False:
				time = datetime.datetime.strptime(parsed_date + ' ' + '12:00', '%Y-%m-%d %H:%M')
				unique_key = (parsed_uid, time)
			else:
				unique_key = None
		elif self.record_type == 'condition':
			parsed_start_date = None
			parsed_start_time = None
			parsed_end_date = None
			parsed_end_time = None
			if 'start date' in row:
				parsed_start_date = Parsers.date_format(row['start date'])
				parsed_start_time = None
				parsed_end_date = None
				parsed_end_time = None
				if not parsed_start_date:
					self.merge_error(
						row,
						"start date",
						"format"
					)
			if 'start time' in row:
				parsed_start_time = Parsers.time_format(row['start time'])
				if not parsed_start_time:
					self.merge_error(
						row,
						"start time",
						"format"
					)
			if 'end date' in row:
				parsed_end_date = Parsers.date_format(row['end date'])
				if not parsed_end_date:
					self.merge_error(
						row,
						"end date",
						"format"
					)
			if 'end time' in row:
				parsed_end_time = Parsers.time_format(row['end time'])
				if not parsed_end_time:
					self.merge_error(
						row,
						"end time",
						"format"
					)
			if parsed_start_date and parsed_start_date is not True:
				if parsed_start_time and parsed_start_time is not True:
					start = datetime.datetime.strptime(parsed_start_date + ' ' + parsed_start_time, '%Y-%m-%d %H:%M')
				else:
					start = datetime.datetime.strptime(parsed_start_date + ' ' + '00:00', '%Y-%m-%d %H:%M')
			else:
				start = None
			if parsed_end_date and parsed_end_date is not True:
				if parsed_end_time and parsed_end_time is not True:
					end = datetime.datetime.strptime(parsed_end_date + ' ' + parsed_end_time, '%Y-%m-%d %H:%M')
				else:
					end = datetime.datetime.strptime(parsed_end_date + ' ' + '00:00', '%Y-%m-%d %H:%M') + datetime.timedelta(1)
			else:
				end = None
			if start and end:
				if start >= end:
					if parsed_start_date:
						self.merge_error(
							row,
							"start date",
							"order"
						)
					if parsed_start_time and parsed_start_time is not True:
						self.merge_error(
							row,
							"start time",
							"order"
						)
					if parsed_end_date:
						self.merge_error(
							row,
							"end date",
							"order"
						)
					if parsed_end_time and parsed_end_time is not True:
						self.merge_error(
							row,
							"end time",
							"order"
						)
			unique_key = (
				parsed_uid,
				start,
				end
			)
		if unique_key and unique_key not in self.unique_keys:
			self.unique_keys.add(unique_key)
		elif unique_key:
			if not self.duplicate_keys:
				self.duplicate_keys = {}
			self.duplicate_keys[row['row_index']] = row

	def merge_error(
			self,
			row,
			field,
			error_type,
			# optional arguments only relevant to value errors
			input_name=None,
			input_format=None,
			category_list=None,
			# optional arguments only relevant to conflicts
			conflicts=None
	):
		errors = self.errors
		if not int(row['row_index']) in errors:
			errors[int(row['row_index'])] = RowParseResult(row)
		errors[int(row['row_index'])].add_error(field, error_type, input_name, input_format, category_list, conflicts)

	def duplicate_keys_table(self):
		if not self.duplicate_keys:
			return '<p>duplicated keys found</p>'
		else:
			max_length = 25
			response = (
				'<p>The uploaded table contains duplicated unique keys '
				'(the combination of UID, date and time). '
				' The following lines duplicate preceding lines in the file: </p>'
			)
			header_string = '<tr><th><p>Line#</p></th>'
			for field in self.fieldnames:
				header_string += '<th><p>' + field + '</p></th>'
			html_table = header_string + '</tr>'
			# construct each row and append to growing table
			for i, row_num in enumerate(self.duplicate_keys):
				if i >= max_length:
					return response + '<table>' + html_table + '</table>'
				row_string = '<tr><td>' + str(row_num) + '</td>'
				for field in self.fieldnames:
					row_string += '<td>' + self.duplicate_keys[row_num][field] + '</td>'
				row_string += '</tr>'
				html_table += row_string
			return response + '<table>' + html_table + '</table>'

	def html_table(self):
		# create a html table string with tooltip for details
		if self.errors:
			header_string = '<tr><th><p>Line#</p></th>'
			for field in self.fieldnames:
				if self.field_errors:
					if field in self.field_errors:
						header_string += '<th bgcolor = #FFFF00 title = "' + self.field_errors[field] \
							+ '"><p>' + field + '</p></th>'
					else:
						header_string += '<th><p>' + field + '</p></th>'
				else:
					header_string += '<th><p>' + field + '</p></th>'
			html_table = header_string + '</tr>'
			# construct each row and append to growing table
			for i, item in enumerate(sorted(self.errors)):
				if i >= max_length:
					return '<div id="response_table_div"><table>' + html_table + '</table></div>'
				row_string = self.errors[item].html_row(self.fieldnames)
				html_table += row_string
			return '<div id="response_table_div"><table>' + html_table + '</table></div>'
		else:
			return None

	def long_enough(self):
		max_length = 100
		if self.errors:
			if len(self.errors) >= max_length:
				return True
		return False


class SubmissionRecord:
	def __init__(
			self,
			record
	):
		if 'Time' in record and record['Time']:
			record['Time'] = datetime.datetime.utcfromtimestamp(record['Time'] / 1000).strftime(
				"%Y-%m-%d %H:%M")
		if 'Period' in record and record['Period']:
			if record['Period'][0]:
				record['Period'][0] = datetime.datetime.utcfromtimestamp(record['Period'][0] / 1000).strftime("%Y-%m-%d %H:%M")
			else:
				record['Period'][0] = 'Undefined'
			if record['Period'][1]:
				record['Period'][1] = datetime.datetime.utcfromtimestamp(record['Period'][1] / 1000).strftime("%Y-%m-%d %H:%M")
			else:
				record['Period'][1] = 'Undefined'
			record['Period'] = ' - '.join(record['Period'])
		record['Submitted at'] = datetime.datetime.utcfromtimestamp(int(record['Submitted at']) / 1000).strftime("%Y-%m-%d %H:%M:%S")
		self.record = record

	def conflict(self):
		if 'Found' in self.record and not self.record['Found']:
			return False
		if 'conflicts' in self.record and not self.record['conflicts']:
			return False
		if 'Access' in self.record and self.record['Access']:
			if self.record['Value'] == self.record['Uploaded value']:
				return False
			else:
				# Handle where a list submission is subset of existing list, this is not a conflict
				if isinstance(self.record['Value'], list):
					# since curves are lists of lists we first need to flatten these to lists of hashable structures
					# for comparison, use a tuple
					for i, j in enumerate(self.record['Value']):
						if isinstance(j, list):
							self.record['Value'][i] = tuple(j)
					if isinstance(self.record['Uploaded value'], list):
						for i, j in enumerate(self.record['Uploaded value']):
							if isinstance(j, list):
								self.record['Uploaded value'][i] = tuple(j)
						if set(self.record['Uploaded value']) == set(self.record['Value']):
							return False
					elif set([i.lower() for i in self.record['uploaded_value'].split(":")]) == set([y.lower() for y in self.record['value']]):
						return False
				return True
		else:
			return True

	# have to do this after conflict comparison so can compare list subsets
	def lists_to_strings(self):
		for item in self.record:
			if isinstance(self.record[item], list):
				self.record[item] = ', '.join([str(i) if not isinstance(i, list) else ', '.join([str(j) for j in i]) for i in self.record[item]])


class SubmissionResult:
	def __init__(self, username, filename, submission_type, record_type):
		self.username = username
		download_path = os.path.join(app.config['DOWNLOAD_FOLDER'], username)
		if not os.path.isdir(download_path):
			logging.debug('Creating download path for user: %s', username)
			os.mkdir(download_path, mode=app.config['EXPORT_FOLDER_PERMISSIONS'])
		if os.path.splitext(filename)[1] == '.xlsx':
			conflicts_filename = '_'.join([
				os.path.splitext(filename)[0],
				record_type,
				'conflicts.csv'
				])
			submissions_filename = '_'.join([
				os.path.splitext(filename)[0],
				record_type,
				'submission_report.csv'
			])
			new_records_filename = '_'.join([
				os.path.splitext(filename)[0],
				record_type,
				'new_records.csv'
			])
		else:
			conflicts_filename = '_'.join([
				os.path.splitext(filename)[0],
				'conflicts.csv'
			])
			submissions_filename = '_'.join([
				os.path.splitext(filename)[0],
				'submission_report.csv'
			])
			new_records_filename = '_'.join([
				os.path.splitext(filename)[0],
				'new_records.csv'
			])
		fieldnames = [
			"UID",
			"Input variable",
			"Value",
			"Submitted by",
			"Submitted at",
		]
		new_records_fieldnames = [i for i in fieldnames]
		new_records_fieldnames.insert(0, "row_index")
		new_records_fieldnames.insert(1, "Replicate")
		new_records_fieldnames.insert(2, "Time")
		new_records_fieldnames.insert(3, "Period")
		if record_type in ['trait', 'curve']:
			fieldnames.insert(1, "Replicate")
			fieldnames.insert(2, "Time")
		elif record_type == 'condition':
			fieldnames.insert(1, "Period")
		# Add both types of fieldname for new records so can be used by "Correct" procedures
		##
		submissions_fieldnames = [i for i in fieldnames]
		conflicts_fieldnames = [i for i in fieldnames]
		conflicts_fieldnames.insert(-2, "Uploaded value")
		self.conflicts_file_path = os.path.join(download_path, conflicts_filename)
		self.submissions_file_path = os.path.join(download_path, submissions_filename)
		upload_path = os.path.join(app.config['UPLOAD_FOLDER'], self.username)
		self.new_records_file_path = os.path.join(upload_path, new_records_filename)
		self.new_records_filename = new_records_filename
		self.conflicts_file = open(os.open(self.conflicts_file_path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o640), "w")
		self.submissions_file = open(os.open(self.submissions_file_path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o640), "w")
		self.new_records_file = open(os.open(self.new_records_file_path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o640), "w")
		self.conflicts_writer = csv.DictWriter(
					self.conflicts_file,
					fieldnames=conflicts_fieldnames,
					quoting=csv.QUOTE_ALL,
					extrasaction='ignore'
				)
		self.submissions_writer = csv.DictWriter(
			self.submissions_file,
			fieldnames=submissions_fieldnames,
			quoting=csv.QUOTE_ALL,
			extrasaction='ignore'
		)
		self.new_records_writer = csv.DictWriter(
			self.new_records_file,
			fieldnames=new_records_fieldnames,
			quoting=csv.QUOTE_ALL,
			extrasaction='ignore'
		)
		self.conflicts_writer.writeheader()
		self.submissions_writer.writeheader()
		self.new_records_writer.writeheader()
		self.submission_type = submission_type
		self.record_type = record_type
		self.conflicts_found = False
		self.submission_count = 0
		self.resubmission_count = 0

	def close(self):
		self.conflicts_file.close()
		self.submissions_file.close()
		self.new_records_file.close()

	def summary(self):
		return {
			"resubmissions": self.resubmission_count,
			"submitted": self.submission_count
		}

	def parse_record(self, record):
		submission_item = SubmissionRecord(record)
		submission_item.lists_to_strings()
		if submission_item.conflict():
			self.conflicts_found = True
			submission_item.lists_to_strings()
			self.conflicts_writer.writerow(submission_item.record)
		else:
			self.submissions_writer.writerow(submission_item.record)
			if 'Found' in record and record['Found']:
				self.resubmission_count += 1
			else:
				self.submission_count += 1
		# creating this file to feed into correct in case need to reverse submission
		if 'Found' in record and not record['Found']:
			self.new_records_writer.writerow(submission_item.record)

