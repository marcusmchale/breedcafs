from app import app, os, celery, ServiceUnavailable, SecurityError, logging
import grp
from app.cypher import Cypher
from app.emails import send_email
from app.models.parsers import Parsers
from flask import render_template, url_for
from werkzeug import urls
from .user import User
from .neo4j_driver import get_driver, bolt_result
import unicodecsv as csv
import datetime
import itertools
import contextlib
from werkzeug.utils import secure_filename
from openpyxl import load_workbook
from zipfile import BadZipfile


class DictReaderInsensitive(csv.DictReader):
	# overwrites csv.fieldnames property so uses without surrounding whitespace and in lowercase
	@property
	def fieldnames(self):
		return [field.strip().lower() for field in csv.DictReader.fieldnames.fget(self) if field]

	def __next__(self):
		return DictInsensitive(next(self))


class DictInsensitive(dict):
	# This class overrides the __getitem__ method to automatically strip() and lower() the input key
	def __getitem__(self, key):
		if key:
			key = key.strip().lower()
		return dict.__getitem__(self, key)


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
														formatted_cells[x] = '<td bgcolor = #FFFF00 title = "'.encode('utf8')
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
													formatted_cells[x] = '<td bgcolor = #FFFF00 title = "'.encode(
														'utf8')
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
				formatted_cells[field] = '<td bgcolor = #FFFF00 title = "'.encode('utf8')
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
								formatted_cells[field] += '\n\n'
								formatted_cells[field] += ''.join(
									['Existing value: ', conflict['existing_value'], '\n']
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
					value = str(self.row[field].encode('utf8'))
				else:
					value = self.row[field]
				formatted_cells[field] += '">'
				formatted_cells[field] += value.encode('utf8').decode('utf8')
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
			max_length = 50
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
		max_length = 100
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
			header_string += '</tr>'
			html_table = header_string
			# construct each row and append to growing table
			for i, item in enumerate(sorted(self.errors)):
				if i >= max_length:
					return html_table
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
				self.record[item] = str(', '.join(
					[
						str(str(i), 'utf8') if not isinstance(i, list) else ', '.join([str(str(j), 'utf8') for j in i]) for i in self.record[item]
					]
				))


class SubmissionResult:
	def __init__(self, username, filename, submission_type, record_type):
		self.username = username
		download_path = os.path.join(app.config['DOWNLOAD_FOLDER'], username)
		if not os.path.isdir(download_path):
			os.mkdir(download_path, mode=app.config['EXPORT_FOLDER_PERMISSIONS'])
		if os.path.splitext(filename)[1] == '.xlsx':
			conflicts_filename =  '_'.join([
				os.path.splitext(filename)[0],
				record_type,
				'conflicts.csv'
				])
			submissions_filename = '_'.join([
				os.path.splitext(filename)[0],
				record_type,
				'submission_report.csv'
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
		fieldnames = [
			"UID",
			"Input variable",
			"Value",
			"Submitted by",
			"Submitted at",
		]
		if record_type == 'trait':
			fieldnames.insert(1, "Replicate")
			fieldnames.insert(2, "Time")
		if record_type == 'condition':
			fieldnames.insert(1, "Period")
		submissions_fieldnames = fieldnames
		conflicts_fieldnames = fieldnames
		conflicts_fieldnames.insert(-2, "Uploaded value")
		self.conflicts_file_path = os.path.join(download_path, conflicts_filename)
		self.submissions_file_path = os.path.join(download_path, submissions_filename)
		self.conflicts_file = open(self.conflicts_file_path, 'w+')
		self.submissions_file = open(self.submissions_file_path, 'w+')
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
		self.conflicts_writer.writeheader()
		self.submissions_writer.writeheader()
		self.submission_type = submission_type
		self.record_type = record_type
		self.conflicts_found = False
		self.submission_count = 0
		self.resubmission_count = 0

	def close(self):
		self.conflicts_file.close()
		self.submissions_file.close()

	def summary(self):
		return {
			"resubmissions": self.resubmission_count,
			"submitted": self.submission_count
		}

	def parse_record(self, record):
		submission_item = SubmissionRecord(record)
		if submission_item.conflict():
			self.conflicts_found = True
			submission_item.lists_to_strings()
			self.conflicts_writer.writerow(submission_item.record)
		else:
			if self.conflicts_found:
				# don't bother parsing and preparing results from submission with conflicts
				# just continue gathering the list of conflicts
				return
			submission_item.lists_to_strings()
			self.submissions_writer.writerow(submission_item.record)
			if 'Found' in record and record['Found']:
				self.resubmission_count += 1
			else:
				self.submission_count += 1


class PropertyUpdateHandler:
	def __init__(self, tx):
		self.tx = tx
		# We want to aggregate records from submission before processing these for updates
		# rather than doing the update in new tx per update
		self.updates = {}
		# This is also in line with the policy of aggregating errors
		# so the user has a chance to see the type of corrections they may need to make on the entire submission
		# without the server needing to fully process the entire submission only to reject it.
		# Threshold for aggregating records before processing the submission
		# this is per update property function
		self.update_threshold = 100
		# we keep the errors in a dictionary grouped by property name to return them in this context
		self.errors = {}
		self.error_threshold = 10
		self.function_dict = {
			'set sample unit': self.set_unit,
			'set custom name': self.set_name,
			'set row': self.set_row,
			'set column': self.set_column,
			'set elevation': self.set_elevation,
			# TODO the above are basically the same form
			#  write a more generalised function rather than adding any more of this type
			'set harvest date': self.set_time,
			'set harvest time': self.set_time,
			'set planting date': self.set_time,
			'set location': self.set_location,
			# The below modify relationships , call these "assign" updates rather than "set"
			'assign tree to block by name': self.assign_tree_to_block,
			'assign tree to block by id': self.assign_tree_to_block,
			'assign sample to block(s) by name': self.assign_sample_to_sources,
			'assign sample to block(s) by id': self.assign_sample_to_sources,
			'assign sample to tree(s) by id': self.assign_sample_to_sources,
			'assign sample to sample(s) by id': self.assign_sample_to_sources,
			'assign variety name': self.assign_variety,
			'assign variety (el frances code)': self.assign_variety
		}

	def process_record(
			self,
			record
	):
		input_variable = record['Input variable'].lower()
		if input_variable not in self.updates:
			self.updates[input_variable] = []
		self.updates[input_variable].append([record['UID'], record['Value']])
		if len(self.updates[input_variable]) >= self.update_threshold:
			self.update_collection(input_variable)
			self.updates[input_variable] = []
		if input_variable in self.errors and len(self.errors[input_variable]) >= self.error_threshold:
			return True

	def update_all(self):
		for key in self.updates:
			self.update_collection(key)

	def format_error_list(self):
		for property_name, errors in self.errors.items():
			if errors:
				errors.insert(
					0,
					'Errors in assigning "' + property_name + '":<br>'
				)

	def update_collection(
			self,
			input_variable
	):
		if self.updates[input_variable]:
			self.function_dict[input_variable](input_variable)

	def error_check(
			self,
			result,
			property_name='property',
	):
		row_errors = []
		for record in result:
			if not record[0]['item_uid']:
				row_errors.append(
					'Item not found (' + str(record[0]['UID']) + ')'
				)
			else:
				if 'existing' in record[0] and record[0]['existing']:
					if not record[0]['existing'] == record[0]['value']:
						row_errors.append(
							'Item (' +
							str(record[0]['item_uid']) +
							') cannot be assigned this' +
							property_name +
							' (' +
							str(record[0]['value']) +
							') as it already has a different value assigned (' +
							str(record[0]['existing']) +
							')'
						)
				if 'conflicts' in record[0] and record[0]['conflicts']:
					row_errors.append(
						'Item (' +
						str(record[0]['item_uid']) +
						') cannot be assigned this' +
						property_name +
						' (' +
						str(record[0]['value']) +
						') as directly linked items already have a different value assigned: ' +
						','.join([
							'(uid: ' + str(i[0]) + ', ' + property_name + ':' + str(i[1]) + ')' for i in record[0]['conflicts']
						])
					)
		if row_errors:
			if property_name not in self.errors:
				self.errors[property_name] = []
			self.errors[property_name] += row_errors

	def variety_error_check(
			self,
			result,
			property_name='variety',
	):
		row_errors = []
		for record in result:
			if not record[0]['item_uid']: # this shouldn't happen as uids are already checked
				logging.debug(
					'A variety assignment was attempted but item was not found: ' + str(record[0]['UID'])
				)
				row_errors.append(
					'Item not found (' + str(record[0]['UID']) + ')'
				)
			elif not record[0]['assigned_variety']:  # this shouldn't happen as values are already checked
				logging.debug(
					'A variety assignment was attempted but variety was not found: ' + str(record[0]['value'])
				)
				row_errors.append(
					'Variety not found (' + str(record[0]['value']) + ')'
				)
			else:
				if all([
					record[0]['existing_variety'],
					record[0]['existing_variety'] != record[0]['assigned_variety']
				]):
					row_errors.append(
						'Item (' +
						str(record[0]['item_uid']) +
						') cannot be assigned this variety '
						' (' +
						str(record[0]['assigned_variety']) +
						') as it is already assigned a different variety (' +
						str(record[0]['existing_variety']) +
						')'
					)
				elif record[0]['kin_conflicts']:
					row_errors.append(
						'Item (' +
						str(record[0]['item_uid']) +
						') cannot be assigned this variety '
						' (' +
						str(record[0]['assigned_variety']) +
						') as directly linked items already have a different variety assigned (' +
						','.join(
							[
								'(UID:' +
								str(i[0]) +
								', variety:' +
								str(i[1]) +
								')' for i in record[0]['kin_conflicts']
							]
						) + ')'
					)
				elif record[0]['tree_varieties_error']:
					row_errors.append(
						'Sample (' +
						str(record[0]['item_uid']) +
						') cannot be assigned this variety '
						' (' +
						str(record[0]['assigned_variety']) +
						') as another sample from the same tree already has a different variety assigned (' +
						','.join([
							'(uid: ' + str(j[0]) + ', variety: ' + str(j[1]) + ')'
							for j in i[1][0:5]  # silently only reporting the first 5 items
						]) for i in record[0]['tree_varieties_error'][0:2]
					)
		if row_errors:
			if property_name not in self.errors:
				self.errors[property_name] = []
			self.errors[property_name] += row_errors

	def item_source_error_check(
			self,
			result,
			property_name='source',
	):
		errors = []
		for record in result:
			# If we don't find the item
			if not record[0]['item_uid']:
				errors.append(
					'Item source assignment failed. The item (' + str(record[0]['UID']) + ')' + ') was not found.'
				)
			# If we don't find the source
			elif len(record[0]['new_source_details']) == 0:
				errors.append(
					'Item (' +
					'UID: ' + record[0]['item_uid']
				)
				if record[0]['item_name']:
					errors[-1] += ', name: ' + record[0]['item_name']
				if len(record[0]['value']) >= 1:
					errors[-1] += (
							') source assignment failed. The sources were not found: '
					)
				else:
					errors[-1] += (
						') source assignment failed. The source was not found: '
					)
				errors[-1] += (
					', '.join([
						str(i) for i in record[0]['value']
					])
				)
			elif record[0]['unmatched_sources']:
				errors.append(
					'Item (' +
					'UID: ' + record[0]['item_uid']
				)
				if record[0]['item_name']:
					errors[-1] += ', name: ' + record[0]['item_name']
				if len(record[0]['unmatched_sources']) >= 1:
					errors[-1] += (
						') source assignment failed. Some sources were not found: '
					)
				else:
					errors[-1] += (
						') source assignment failed. A source was not found: '
					)
				errors[-1] += (
					', '.join([
						str(i) for i in record[0]['unmatched_sources']
					])
				)
			elif record[0]['invalid_sources']:
				errors.append(
					'Item (' +
					'UID: ' + record[0]['item_uid']
				)
				if record[0]['item_name']:
					errors[-1] += ', name: ' + record[0]['item_name']
				if len(record[0]['invalid_sources']) >= 1:
					errors[-1] += ') source assignment failed. Proposed sources ['
				else:
					errors[-1] += ') source assignment failed. Proposed source '
				errors[-1] += ', '.join([
					'(uid: ' + str(i[0]) + ', name: ' + str(i[1]) + ')' if i[1]
					else '(uid: ' + str(i[0]) + ')'
					for i in record[0]['invalid_sources']
				])
				if len(record[0]['invalid_sources']) >= 1:
					errors[-1] += (
						'] are not themselves sourced (either directly or indirectly) from '
					)
				else:
					errors[-1] += (
						' is not itself sourced (either directly or indirectly) from '
					)
				if len(record[0]['prior_source_details']) >= 1:
					errors[-1] += (
						'any of the existing assigned sources: '
					)
				else:
					errors[-1] += (
						'the existing assigned source: '
					)
				errors[-1] += ', '.join([
					'(uid: ' + str(i[0]) + ', name: ' + str(i[1]) + ')' if i[1]
					else '(uid: ' + str(i[0]) + ')'
					for i in record[0]['prior_source_details']
				])
			elif record[0]['unrepresented_sources']:
				# this occurs when not all prior sources are represented by the new sources
				# this occurs in the case of attempting to re-assign to new block(s)/tree(s) without deleting an existing record
				# and also in re-assigning pooled samples with greater detail
				errors.append(
					'Item (' +
					'UID: ' + record[0]['item_uid']
				)
				if record[0]['item_name']:
					errors[-1] += ', name: ' + record[0]['item_name']
				if len(record[0]['unrepresented_sources']) >= 1:
					errors[-1] += ') source assignment failed. Existing sources ['
				else:
					errors[-1] += ') source assignment failed. Existing source '
				errors[-1] += ', '.join([
					'(uid: ' + str(i[0]) + ', name: ' + str(i[1]) + ')' if i[1]
					else '(uid: ' + str(i[0]) + ')'
					for i in record[0]['unrepresented_sources']
				])
				if len(record[0]['unrepresented_sources']) >= 1:
					errors[-1] += (
						'] would not be represented (either directly or indirectly) by '
					)
				else:
					errors[-1] += (
						' would not be represented (either directly or indirectly) by '
					)
				if len(record[0]['new_source_details']) >= 1:
					errors[-1] += (
						'the proposed sources: '
					)
				else:
					errors[-1] += (
						'the proposed source: '
					)
				errors[-1] += ', '.join([
					'(uid: ' + str(i[0]) + ', name: ' + str(i[1]) + ')' if i[1]
					else '(uid: ' + str(i[0]) + ')'
					for i in record[0]['new_source_details']
				])
			elif record[0]['variety_conflicts']:
				errors.append(
					'Item (' +
					'UID: ' + record[0]['item_uid']
				)
				if record[0]['item_name']:
					errors[-1] += ', name: ' + record[0]['item_name']
				errors[-1] += (
					') source assignment failed. '
				)
				errors[-1] += (
					'The variety assigned to '
					'proposed source(s) conflicts with the variety that is specified for the item '
					'or its samples: '
				)
				if len(record[0]['variety_conflicts']) >= 2:
					errors[-1] += (
						'The assignment would create many such conflicts so '
						'only the first two are being reported. '
					)
				errors[-1] += ', '.join([
					'(source uid: ' + str(i['ancestor']) + ', source variety:' + str(i['ancestor_variety']) + ', ' +
					'descendant item: ' + str(i['descendant']) + ', ' +
					'descendant variety: ' + str(i['descendant_variety']) + ')'
					for i in record[0]['variety_conflicts'][0:2]
				])
			elif record[0]['tree_varieties_error']:
				errors.append(
					'Item (' +
					'UID: ' + record[0]['item_uid']
				)
				if record[0]['item_name']:
					errors[-1] += ', name: ' + record[0]['item_name']
				errors[-1] += (
					') source assignment failed. '
				)
				if len(record[0]['tree_varieties_error']) >= 2:
					errors[-1] += (
						'The proposed source assignment would create ambiguous definitions for many trees, '
						'only the first two are being reported. '
					)
				errors[-1] += '. '.join([
					'The proposed source assignment would create an ambiguous definition '
					'for the variety of a tree (' + str(i[0]) + '). ' +
					'The conflicts are between varieties assigned to the following items: ' +
					','.join([
						'(uid: ' + str(j[0]) + ', variety: ' + str(j[1]) + ')'
						for j in i[1][0:5]  # silently only reporting the first 5 items
					]) for i in record[0]['tree_varieties_error'][0:2]
				])
		if errors:
			if property_name not in self.errors:
				self.errors[property_name] = []
			self.errors[property_name] += errors

	def set_name(self, input_variable):
		statement = (
			' UNWIND $uid_value_list AS uid_value '
			'	OPTIONAL MATCH '
			'		(item: Item {uid: uid_value[0]}) '
			'	WITH uid_value, item, item.name as existing '
			'	SET item.name = CASE '
			'		WHEN item.name IS NULL '
			'		THEN uid_value[1] '
			'		ELSE item.name '
			'		END '
			'	RETURN { '
			'		UID: uid_value[0], '
			'		value: uid_value[1], '
			'		item_uid: item.uid, '
			'		existing: existing '
			'	} '
		)
		result = self.tx.run(
			statement,
			uid_value_list=self.updates[input_variable]
		)
		self.error_check(
			result,
			'name'
		)

	def set_row(self, input_variable):
		statement = (
			' UNWIND $uid_value_list AS uid_value '
			'	OPTIONAL MATCH '
			'		(item: Tree {uid: uid_value[0]}) '
			'	WITH uid_value, item, item.row as existing '
			'	SET item.row = CASE '
			'		WHEN item.row IS NULL '
			'		THEN uid_value[1] '
			'		ELSE item.row '
			'		END '
			'	RETURN { '
			'		UID: uid_value[0], '
			'		value: uid_value[1], '
			'		item_uid: item.uid, '
			'		existing: existing '
			'	} '
		)
		result = self.tx.run(
			statement,
			uid_value_list=self.updates[input_variable]
		)
		self.error_check(
			result,
			'row'
		)

	def set_column(self, input_variable):
		statement = (
			' UNWIND $uid_value_list AS uid_value '
			'	OPTIONAL MATCH '
			'		(item: Tree {uid: uid_value[0]}) '
			'	WITH uid_value, item, item.column as existing '
			'	SET item.column = CASE '
			'		WHEN item.column IS NULL '
			'		THEN uid_value[1] '
			'		ELSE item.column '
			'		END '
			'	RETURN { '
			'		UID: uid_value[0], '
			'		value: uid_value[1], '
			'		item_uid: item.uid, '
			'		existing: existing '
			'	} '
		)
		result = self.tx.run(
			statement,
			uid_value_list=self.updates[input_variable]
		)
		self.error_check(
			result,
			'column'
		)

	def set_location(self, input_variable):
		statement = (
			' UNWIND $uid_value_list AS uid_value '
			'	OPTIONAL MATCH '
			'		(item: Item {uid: uid_value[0]}) '
			'	WITH uid_value, item, item.location as existing '
			'	SET item.location = CASE '
			'		WHEN item.location IS NULL '
			'		THEN uid_value[1] '
			'		ELSE item.location '
			'		END '
			'	RETURN { '
			'		UID: uid_value[0], '
			'		value: uid_value[1], '
			'		item_uid: item.uid, '
			'		existing: existing '
			'	} '
		)
		result = self.tx.run(
			statement,
			uid_value_list=self.updates[input_variable]
		)
		self.error_check(
			result,
			'location'
		)

	def set_unit(self, input_variable):
		statement = (
			' UNWIND $uid_value_list AS uid_value '
			'	OPTIONAL MATCH '
			'		(item: Sample {uid: uid_value[0]}) '
			'	WITH uid_value, item, item.unit as existing'
			'	SET item.unit = CASE '
			'		WHEN item.unit IS NULL '
			'		THEN uid_value[1] '
			'		ELSE item.unit '
			'		END '
			'	RETURN  { '
			'		UID: uid_value[0], '
			'		value: uid_value[1], '	
			'		item_uid: item.uid, '
			'		existing: existing '
			'	} '
		)
		result = self.tx.run(
			statement,
			uid_value_list=self.updates[input_variable]
		)
		self.error_check(
			result,
			'sample unit'
		)

	def set_elevation(self, input_variable):
		statement = (
			' UNWIND $uid_value_list AS uid_value '
			'	OPTIONAL MATCH '
			'		(item: Field {uid: toInteger(uid_value[0])}) '
			'	WITH uid_value, item, item.elevation as existing'
			'	SET item.elevation = CASE '
			'		WHEN item.elevation IS NULL '
			'		THEN toInteger(uid_value[1]) '
			'		ELSE item.elevation '
			'		END '
			'	RETURN  { '
			'		UID: uid_value[0], '
			'		value: uid_value[1], '	
			'		item_uid: item.uid, '
			'		existing: existing '
			'	} '
		)
		result = self.tx.run(
			statement,
			uid_value_list=self.updates[input_variable]
		)
		self.error_check(
			result,
			'elevation'
		)

# Notes for assign source functions
	# In all assign to source functions we need to consider inheritance updates.
	# Ensure match for variety property between source (and its ancestors) and item (and its descendants)
	# Need to update varieties property for all members of new item lineage (including item)
	#  don't have to update prior lineage:
	#  - item prior ancestors are still included in new item ancestors
	#    - We enforce this by only allowing reassignments to descendants of a prior source
	#  - item prior descendants are included in new lineage
	#  - source prior ancestors are included in new lineage
	#  - source prior descendants are unaffected as they were not in the prior lineage
	# Ensure any tree in new lineage has size(varieties) <= 1

	def assign_tree_to_block(self, input_variable):
		statement = (
			' UNWIND $uid_value_list AS uid_value '
			'	WITH '
			'		uid_value[0] as uid,'
			'		uid_value[1] as value '
			'	OPTIONAL MATCH '
			'		(tree: Tree { '
			'			uid: uid '
			'		}) '
			'	OPTIONAL MATCH '
			'		(field: Field {uid: '
			'			CASE '
			'			WHEN toInteger(uid) IS NOT NULL '
			'				THEN uid '
			'			ELSE '
			'				toInteger(split(uid, "_")[0]) '
			'			END '
			'		}) '
			'	OPTIONAL MATCH '
			'		(tree) '
			'		-[:IS_IN]->(:BlockTrees) '
			'		-[:IS_IN]->(prior_block: Block) '
			'	OPTIONAL MATCH '
			'		(new_block: Block) '
			'		-[:IS_IN]->(:FieldBlocks) '
			'		-[:IS_IN]->(field) '
		)
		if 'name' in input_variable:
			statement += (
				' WHERE new_block.name_lower = toLower(trim(value)) '
			)
		else:
			statement += (
				' WHERE new_block.id = toInteger(value) '
			)
		statement += (
			# check for variety conflicts
			'	OPTIONAL MATCH '
			'		(tree)<-[: FROM*]-(sample: Sample) '
			'	WITH '
			'		uid, value, '
			'		tree, field, '
			'		prior_block, '
			'		new_block, '
			' 		CASE WHEN tree.variety IS NOT NULL AND new_block.variety IS NOT NULL AND tree.variety <> new_block.variety '
			'			THEN [{ '
			'				ancestor: new_block.uid,'
			'				ancestor_variety: new_block.variety, '
			'				descendant: tree.uid, '
			'				descendant_variety: tree.variety '
			'			}] '
			'			ELSE [] END + ' 
			'		[ '
			'			x in collect({ '
			'				ancestor: new_block.uid,'
			'				ancestor_variety: new_block.variety, '
			'				descendant: sample.uid, '
			'				descendant_variety: sample.variety '
			'			}) WHERE '
			'				x["descendant_variety"] IS NOT NULL '
			'				AND x["ancestor_variety"] IS NOT NULL '
			'				AND  x["descendant_variety"] <> x["ancestor_variety"] '
			'		] as variety_conflicts '
			'	FOREACH (n IN CASE WHEN '
			'			prior_block IS NULL AND '
			'			new_block IS NOT NULL AND '
			'			size(variety_conflicts) = 0 '
			'		THEN [1] ELSE [] END | '
			'		MERGE '
			'			(bt:BlockTrees) '
			'			-[:IS_IN]->(new_block) '
			'		MERGE '
			'			(bt) '
			'			<-[:FOR]-(c:Counter) '
			'			ON CREATE SET '
			'				c.count = 0, '
			'				c.name = "tree", '
			'				c.uid = (new_block.uid + "_tree") '
			'		SET c._LOCK_ = True '
			'		MERGE (tree)-[:IS_IN]->(bt) '
			'		SET c.count = c.count + 1 '
			'		REMOVE c._LOCK_ '
			'	) '
			'	WITH '
			'		uid, value, tree, field, prior_block, new_block, variety_conflicts '
			'		OPTIONAL MATCH '
			'			(tree)-[:IS_IN*]->(ancestor: Item) '
			'	WITH '
			'		uid, value, tree, field, prior_block, new_block, variety_conflicts, '
			'		collect(ancestor) as ancestors '
			'		OPTIONAL MATCH '
			'			(tree)<-[:FROM*]-(descendant: Item) '
			'	WITH '
			'		uid, value, tree, field, prior_block, new_block, variety_conflicts, '
			'		ancestors + collect(descendant) as lineage '
			'		UNWIND lineage as kin '
			'			OPTIONAL MATCH '
			'				(kin)-[:IS_IN | FROM *]->(kin_ancestor: Item) '
			'			WITH '
			'				uid, value, tree, field, prior_block, new_block, variety_conflicts, '
			'				kin, collect(kin_ancestor) as kin_ancestors '
			'			OPTIONAL MATCH '
			'				(kin)<-[:IS_IN | FROM *]-(kin_descendant: Item) '
			'			WITH '
			'				uid, value, tree, field, prior_block, new_block, variety_conflicts, '
			'				kin, kin_ancestors + collect(kin_descendant) as kin_lineage '
			'			UNWIND kin_lineage as kin_of_kin '
			'			WITH '
			'				uid, value, tree, field, prior_block, new_block, variety_conflicts, '
			'				kin, collect(distinct kin_of_kin.variety) as kin_varieties '
			'			SET kin.varieties = CASE WHEN kin.variety IS NOT NULL THEN [kin.variety] ELSE kin_varieties END '
			'	WITH '
			'		uid, value, tree, field, prior_block, new_block, variety_conflicts, '
			'		collect(distinct kin.variety) as varieties, '
			'		collect(distinct [kin.uid, kin.variety]) as variety_sources '
			'	SET tree.varieties = CASE WHEN tree.variety IS NOT NULL THEN [tree.variety] ELSE varieties END '
			'	RETURN  { '
			'		UID: uid, '
			'		value: value, '
			'		item_name: tree.name, '
			'		item_uid: tree.uid, '
			'		prior_source_details: [[prior_block.uid, prior_block.name]], '
			'		new_source_details: [[new_block.uid, new_block.name]], '
			'		unmatched_sources:	CASE WHEN new_block IS NULL THEN [value] END, '
			'		invalid_sources: CASE '
			'			WHEN prior_block IS NOT NULL AND prior_block <> new_block '
			'			THEN [[new_block.uid, new_block.name]] '
			'			END, '
			'		unrepresented_sources: CASE '
			'			WHEN prior_block IS NOT NULL AND prior_block <> new_block '
			'			THEN [[prior_block.uid, prior_block.name]] '
			'			END, '
			'		variety_conflicts: variety_conflicts, '
			'		tree_varieties_error: CASE '
			'			WHEN size(tree.varieties) > 1 '
			'			THEN '
			'				collect([tree.uid,variety_sources]) '
			'			END '
			'	} '
		)
		result = self.tx.run(
			statement,
			uid_value_list=self.updates[input_variable]
		)
		self.item_source_error_check(
			result,
			'block'
		)

	def assign_sample_to_sources(self, input_variable):
		if 'to block' in input_variable:
			source_level = "Block"
		elif 'to tree' in input_variable:
			source_level = "Tree"
		elif 'to sample' in input_variable:
			source_level = "Sample"
		statement = (
			' UNWIND $uid_value_list AS uid_value '
			'	WITH '
			'		uid_value[0] as uid,'
			'		uid_value[1] as value '
			'	OPTIONAL MATCH '
			'		(sample: Sample { '
			'			uid: uid '
			'		}) '
			'	OPTIONAL MATCH '
			'		(field: Field {uid: '
			'			CASE '
			'			WHEN toInteger(uid) IS NOT NULL '
			'				THEN uid '
			'			ELSE '
			'				toInteger(split(uid, "_")[0]) '
			'			END '
			'		}) '
			'	WITH '
			'		uid, value, sample, field '
			'	OPTIONAL MATCH '
			'		(sample)-[prior_primary_sample_from: FROM]->(:ItemSamples)-[: FROM]->(prior_primary_source: Item) '
			'	OPTIONAL MATCH '
			'		(sample)-[prior_secondary_sample_from: FROM]->(prior_secondary_source: Sample) '
			'	WITH '
			'		uid, value, sample, field, '
			'		collect(coalesce(prior_primary_source, prior_secondary_source)) as prior_sources, '
			'		collect(coalesce(prior_primary_sample_from, prior_secondary_sample_from)) as prior_source_rels '
			'	UNWIND value as source_identifier '
			'		OPTIONAL MATCH '
			'			(new_source: Item)-[: IS_IN | FROM*]->(field) '
			'		WHERE $source_level in labels(new_source) AND '
		)
		if 'name' in input_variable:
			statement += (
				'		new_source.name_lower = source_identifier '
			)
		else:
			statement += (
				'		new_source.id = toInteger(source_identifier) '
			)
		statement += (
			'		UNWIND prior_sources as prior_source '
			'			OPTIONAL MATCH '
			'				lineage_respected = (new_source)-[: IS_IN | FROM*]->(prior_source) '
			'	WITH  '
			'		uid, value, sample, '
			'		prior_source_rels, '
			'		collect(distinct new_source) as new_sources, '
			'		collect(distinct prior_source) as prior_sources, '
			'		[ '
			'			x in collect(distinct [source_identifier, new_source]) '
			'			WHERE x[1] IS NULL | x[0] '
			'		] as unmatched_sources, '  # where the new source was not found by name/id in this field
			'		[ '
			'			x in collect(distinct [new_source, lineage_respected]) '
			'			WHERE x[1] IS NOT NULL | x[0] '
			'		] as valid_sources, '  # where the new source is a direct descendant of a prior source
			'		[ '
			'			x in collect(distinct [prior_source, lineage_respected])'
			'			WHERE x[1] IS NOT NULL | x[0] '
			'		] as represented_sources '  # where the prior source has a direct descendant among the new sources
			'	WITH '
			'		uid, value, sample, '
			'		prior_source_rels, '
			'		new_sources, '
			'		prior_sources, '
			'		unmatched_sources, '
			'		[ '
			'			x in new_sources '
			'			WHERE NOT x IN valid_sources '
			'			AND NOT x IN prior_sources '
			'			| [x.uid, x.name] '
			'		]  as invalid_sources, '
			'		[ '
			'			x in prior_sources '
			'			WHERE NOT x IN represented_sources '
			'			AND NOT x IN new_sources '
			'			| [x.uid, x.name] '
			'		]  as unrepresented_sources '
			# Need to check for variety conflicts that would be created in new lineage 
			# we also want to update varieties property in all members of new lineage
			' 	UNWIND new_sources as new_source '
			'		OPTIONAL MATCH '
			'			(new_source)-[:IS_IN | FROM *]->(ancestor: Item) '
			'		OPTIONAL MATCH '
			'			(sample)<-[:IS_IN | FROM *]-(descendant: Item) '
			# The above matches make a cartesian product 
			# this may be reasonable as we want to check for any conflict among these products
			'	WITH '
			'		uid, value, sample, '
			'		prior_source_rels, '
			'		collect(distinct new_source) as new_sources, '
			'		prior_sources, '
			'		unmatched_sources, invalid_sources, unrepresented_sources, '
			'		collect(distinct ancestor) + collect(distinct descendant) + collect(distinct new_source) as lineage, '	
			'		CASE '
			'			WHEN '
			'				sample.variety IS NOT NULL AND '
			'				new_source.variety IS NOT NULL AND '
			'				sample.variety <> new_source.variety '
			'			THEN collect(distinct { '
			'				ancestor: new_source.uid,'
			'				ancestor_variety: new_source.variety, '
			'				descendant: sample.uid, '
			'				descendant_variety: sample.variety '
			'			}) '
			'			ELSE []'
			'		END + ' 
			'		[ '	
			'			x in collect(distinct { '
			'				ancestor: ancestor.uid,'
			'				ancestor_variety: ancestor.variety, '
			'				descendant: sample.uid, '
			'				descendant_variety: sample.variety'
			'			}) WHERE '
			'				x["descendant_variety"] IS NOT NULL AND '
			'				x["ancestor_variety"] IS NOT NULL AND '
			'				x["descendant_variety"] <> x["ancestor_variety"] '
			'		] + '
			'		[ '	
			'			x in collect(distinct { '
			'				ancestor: new_source.uid,'
			'				ancestor_variety: new_source.variety, '
			'				descendant: descendant.uid, '
			'				descendant_variety: descendant.variety'
			'			}) WHERE '
			'				x["descendant_variety"] IS NOT NULL AND '
			'				x["ancestor_variety"] IS NOT NULL AND '
			'				x["descendant_variety"] <> x["ancestor_variety"] '
			'		] + '
			'		[ '	
			'			x in collect(distinct { '
			'				ancestor: ancestor.uid,'
			'				ancestor_variety: ancestor.variety, '
			'				descendant: descendant.uid, '
			'				descendant_variety: descendant.variety '
			'			}) WHERE '
			'				x["descendant_variety"] IS NOT NULL AND '
			'				x["ancestor_variety"] IS NOT NULL AND '
			'				x["descendant_variety"] <> x["ancestor_variety"] '
			'		] '
			'			as variety_conflicts  '
			'	UNWIND '
			'		new_sources as new_source '
			'		FOREACH (n IN CASE WHEN '
			'			new_source IS NOT NULL AND '
			'			NOT new_source IN prior_sources AND '
			'			sample IS NOT NULL AND '
			'			size(unmatched_sources) = 0 AND '
			'			size(invalid_sources) = 0 AND '
			'			size(unrepresented_sources) = 0 AND '
			'			size(variety_conflicts) = 0 '
			'		THEN [1] ELSE [] END | '
			'			FOREACH (n in prior_source_rels | delete n) '
		)
		if source_level == "sample":
			statement += (
				'		MERGE (sample)-[:FROM]->(new_source) '
			)
		else:
			statement += (
				'		MERGE (is:ItemSamples)-[:FROM]->(new_source) '
				'		MERGE (sample)-[:FROM]->(is) '
			)
		statement += (
			'		) '
			'	WITH '
			'		uid, value, sample, '
			'		prior_source_rels, '
			'		prior_sources, collect(new_source) as new_sources, '
			'		unmatched_sources, invalid_sources, unrepresented_sources, variety_conflicts, '
			'		lineage '
			'	WITH '
			'		uid, value, sample, '
			'		[x in prior_sources | [x.uid, x.name]] as prior_source_details, '
			'		[x in new_sources | [x.uid, x.name]] as new_source_details, '
			'		unmatched_sources, invalid_sources, unrepresented_sources, variety_conflicts, '
			'		lineage '
			'	UNWIND lineage as kin '
			'		OPTIONAL MATCH '
			'			(kin)-[:IS_IN | FROM *]->(kin_ancestor: Item) '
			'		WITH '
			'			uid, value, sample, '
			'			prior_source_details, new_source_details, '
			'			unmatched_sources, invalid_sources, unrepresented_sources, variety_conflicts, '
			'			kin, collect(distinct kin_ancestor) as kin_ancestors '
			'		OPTIONAL MATCH '
			'			(kin)<-[:IS_IN | FROM *]-(kin_descendant: Item) '
			'		WITH '
			'			uid, value, sample, '
			'			prior_source_details, new_source_details, '
			'			unmatched_sources, invalid_sources, unrepresented_sources, variety_conflicts, '
			'			kin, kin_ancestors + collect(distinct kin_descendant) as kin_lineage '
			'		UNWIND kin_lineage as kin_of_kin '
			'		WITH '
			'			uid, value, sample, '
			'			prior_source_details, new_source_details, '
			'			unmatched_sources, invalid_sources, unrepresented_sources, variety_conflicts, '
			'			kin, collect(distinct kin_of_kin.variety) as kin_of_kin_varieties, '
			'			[ '
			'				x in collect(distinct [kin_of_kin.uid, kin_of_kin.variety]) WHERE x[1] IS NOT NULL'
			'			] as kin_variety_sources '
			'		SET kin.varieties = CASE WHEN kin.variety IS NOT NULL THEN [kin.variety] ELSE kin_of_kin_varieties END '
			'	WITH '
			'		uid, value, sample, '
			'		prior_source_details, new_source_details, '
			'		unmatched_sources, invalid_sources, unrepresented_sources, variety_conflicts, '
			'		collect(distinct kin.variety) as kin_varieties, '
			'		[x in collect(distinct [[kin.uid, kin_variety_sources], labels(kin), kin.varieties]) '
			'			WHERE "Tree" IN x[1] AND size(x[2]) > 1 '
			'			| [x[0]] '
			'		] as tree_varieties_error '
			'	SET sample.varieties = CASE WHEN sample.variety IS NOT NULL THEN [sample.variety] ELSE kin_varieties END '
			'	RETURN  { '
			'		UID: uid, '
			'		value: value, '
			'		item_name: sample.name, '
			'		item_uid: sample.uid, '
			'		prior_source_details: prior_source_details,'
			'		new_source_details: new_source_details, '
			'		unmatched_sources: unmatched_sources, '
			'		invalid_sources: invalid_sources, '
			'		unrepresented_sources: unrepresented_sources, '
			'		variety_conflicts: variety_conflicts, '
			'		tree_varieties_error: tree_varieties_error '
			'	} '
		)
		for item in self.updates[input_variable]:
			if 'name' in input_variable:
				item[1] = Parsers.parse_name_list(item[1])
			else:
				item[1] = Parsers.parse_range_list(item[1])
		result = self.tx.run(
			statement,
			uid_value_list=self.updates[input_variable],
			source_level=source_level
		)
		self.item_source_error_check(
			result,
			source_level
		)

	def assign_variety(self, input_variable):
		#  Variety assignment can come from multiple properties (variety name/variety code)
		#    so we need to handle cases where variety is already set
		#
		# Properties affected:
		#  variety: a single value matching a known variety that is set from a record submission
		#  varieties: a collection (and reflected in relationships to the FieldVariety node)
		#
		# Relationships affected:
		#  OF_VARIETY: a definite specification of a single variety for an item
		#  CONTAINS_VARIETY:
		#    a relationship between a Field item and a given FieldVariety container node
		#    exists when any item with direct (single direction) path of IS_IN and/or FROM relationships to field
		#      has relationship OF_VARIETY to the relevant Variety
		#
		# Inheritance:
		#  when assigning new variety we update varieties for affected kin (see below):
		#
		# Terms:
		#    item: the primary subject of the query to which the property is being assigned
		#    ancestor:
		#      = direct lineal source of item
		#      = (item)-[:FROM | IS_IN*]->(ancestor)
		#    ancestors:
		#      = all ancestors of item
		#      = collect(ancestor)
		#    descendant:
		#      = direct lineal product of item
		#      = (descendant)-[:FROM | IS_IN*]->(item)
		#    descendants:
		#      = all descendants of item
		#      = collect(descendant)
		#    lineage: (NB: here this term excludes item)
		#      = ancestors + descendants
		#      = items connected to item by any path consisting of IS_IN or FROM relationships with single direction
		#    kin:
		#      = lineal kinsman
		#      = member of lineage
		#    kin_lineage:
		#      = lineage of kin
		#      i.e.:
		#        kin_ancestor = (kin)-[:FROM | IS_IN*]->(ancestor)
		#        kin_descendant = (descendant)-[:FROM | IS_IN*]->(kin)
		#        kin_lineage = collect(kin_ancestor) + collect(kin_descendant)
		#    kin_of_kin:
		#      = member of kin_lineage
		#
		# Updates:
		#  kin.varieties = collect(distinct kin_of_kin.variety)
		#  item.varieties = collect(distinct kin.variety)
		#
		# Errors to be raised:
		#  - kin.variety IS NOT NULL and (kin.varieties <> [kin.variety])
		#  - size(kin.varieties) > 1 WHERE "Tree" in labels(kin)
		statement = (
			' UNWIND $uid_value_list AS uid_value '
			'	WITH '
			'		CASE '
			'			WHEN size(split(toString(uid_value[0]), "_")) = 1 '
			'			THEN toInteger(uid_value[0]) '
			'			ELSE uid_value[0] '
			'			END as uid, '
			'		uid_value[1] as value '
			'	OPTIONAL MATCH '
			'		(item: Item {uid: uid}) '
			'	OPTIONAL MATCH '
			'		(field: Field {uid: '
			'			CASE '
			'			WHEN toInteger(uid) IS NOT NULL '
			'				THEN uid '
			'			ELSE '
			'				toInteger(split(uid, "_")[0]) '
			'			END '
			'		}) '
			'	OPTIONAL MATCH '
			'		(variety: Variety) '
		)
		if 'name' in input_variable:
			statement += (
				' WHERE variety.name_lower = toLower(trim(value)) '
			)
		else:  # input_variable contains 'code'
			statement += (
				' WHERE variety.code = toLower(trim(value)) '
			)
		statement += (
			'	WITH '
			'		uid, value, item, field, variety, '
			'		item.variety as existing_variety '
			'	FOREACH (n IN CASE '
			'		WHEN item.variety IS NULL '
			'		THEN [1] ELSE [] END | '
			'		MERGE '
			'			(field) '
			'			-[: CONTAINS_VARIETY]->(fv:FieldVariety) '
			'			-[: OF_VARIETY]->(variety) '
			'		MERGE '
			'			(item) '
			'			-[: OF_VARIETY]->(fv) '
			'		SET '
			'			item.variety = variety.name,  '
			'			item.varieties = [variety.name] '
			'	) '
			'	WITH '
			'		uid, value, item, existing_variety, variety'
			'	OPTIONAL MATCH '
			'		(item)-[:IS_IN | FROM *]->(ancestor: Item) '
			'	WITH '
			'		uid, value, item, existing_variety, variety, '
			'		collect(distinct ancestor) as ancestors '
			'	OPTIONAL MATCH '
			'		(item)<-[:IS_IN | FROM *]-(descendant: Item) '
			'	WITH  '
			'		uid, value, item, existing_variety, variety, '
			'		ancestors + collect(distinct descendant) as lineage '
			'	UNWIND lineage AS kin '
			'		OPTIONAL MATCH '
			'			(kin)-[:IS_IN | FROM *]->(kin_ancestor: Item) '
			'		WITH '
			'			uid, value, item, existing_variety, variety, '
			'			kin, '
			'			collect(distinct kin_ancestor) as kin_ancestors '
			'		OPTIONAL MATCH '
			'			(kin)<-[:IS_IN | FROM *]-(kin_descendant: Item) '
			'		WITH '
			'			uid, value, item, existing_variety, variety, '
			'			kin, '			
			'			kin_ancestors + collect(distinct kin_descendant) as kin_lineage '
			'		UNWIND '
			'			kin_lineage as kin_of_kin '
			'		WITH '
			'			uid, value, item, existing_variety, variety, '
			'			kin, '
			'			collect(distinct kin_of_kin.variety) as kin_varieties, '
			# If kin is a Tree we need to record kin_of_kin UID and variety if it differs from variety.name
			# as we could have a conflict where two samples from same tree have different variety assigned
			# For fields/blocks/samples we accept cases of multiple varieties 
			# Among this list will also be direct kin conflicts
			#   so only include these errors in response when no direct kin conflicts
			'			[ '
			'				x in collect(distinct [kin_of_kin.uid, kin_of_kin.variety]) WHERE x[1] IS NOT NULL'
			'			] as kin_variety_sources '
			'		SET kin.varieties = CASE WHEN kin.variety IS NOT NULL THEN [kin.variety] ELSE kin_varieties END '
			'	WITH '
			'		uid, value, item, existing_variety, variety, '
			'		[ '
			'			x in collect( '
			'				distinct [kin.uid, kin.variety] '
			'			) WHERE x[1] IS NOT NULL AND x[1] <> variety.name '
			'		] as kin_conflicts, '
			'		[x in collect(distinct [[kin.uid, kin_variety_sources], labels(kin), kin.varieties]) '
			'			WHERE "Tree" IN x[1] AND size(x[2]) > 1 '
			'			| x[0] '
			'		] as tree_varieties_error '
			'	RETURN { '
			'		UID: uid, '
			'		value: value, '	
			'		item_uid: item.uid, '
			'		existing_variety: existing_variety, '
			'		assigned_variety: variety.name, '
			'		item_variety: item.variety, '
			'		item_varieties: item.varieties, '
			'		tree_varieties_error: tree_varieties_error, '
			'		kin_conflicts: kin_conflicts '
			'	} '
		)
		result = self.tx.run(
			statement,
			uid_value_list=self.updates[input_variable]
		)
		self.variety_error_check(
			result,
			input_variable
		)

	def set_time(self, input_variable):
		statement = (
			' UNWIND $uid_value_list AS uid_value '
			'	OPTIONAL MATCH '
			'		(item: Item {uid: uid_value[0]}) '
			'	WITH '
			'		uid_value, item, '
		)
		if 'date' in input_variable:
			statement += (
				' item.date as existing '
				' SET item.date = CASE WHEN existing IS NULL THEN uid_value[1] ELSE existing END '
			)
		else:  # 'time' in input variable
			statement += (
				' item.time_of_day as existing '
				' SET item.time = CASE WHEN existing IS NULL THEN uid_value[1] ELSE existing END '
			)
		statement += (
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
			'	RETURN  { '
			'		UID: uid_value[0], '
			'		value: uid_value[1], '
			'		item_uid: item.uid, '
			'		existing: existing '
			'	} '
		)
		result = self.tx.run(
			statement,
			uid_value_list=self.updates[input_variable]
		)
		self.error_check(
			result,
			input_variable
		)


class Upload:
	def __init__(self, username, submission_type, raw_filename):
		self.username = username
		self.raw_filename = raw_filename
		self.filename = secure_filename(raw_filename)
		self.submission_type = submission_type
		self.file_path = os.path.join(app.config['UPLOAD_FOLDER'], username, self.filename)
		self.record_types = []
		self.trimmed_file_paths = dict()
		self.parse_results = dict()
		self.submission_results = dict()
		self.fieldnames = dict()
		self.file_extension = self.filename.rsplit('.', 1)[1].lower()
		self.contains_data = None
		self.input_variables = dict()
		self.row_count = dict()
		self.error_messages = []
		self.property_updater = None
		# 'db' submissions ("Correct" page) have mixed record types
		# required to identify a given record are:
		# 	uid (and sometimes replicate),
		# 	input variable (and for traits or properties the time or period respectively)
		# but to further confirm we only delete the intended record in the context of updates etc.
		# 	e.g. if a record is later resubmitted
		# 	we include the check for submitted at
		self.required_sets = {
			'mixed': {
				'uid',
				'input variable',
				'time',
				'period',
				'submitted at'
			},
			'property': {'uid'},
			'trait': {'uid', 'date'},
			'curve': {'uid', 'date'},
			'condition': {'uid', 'start date'}
		}
		self.optional_sets = {
			'mixed': {
				'country',
				'region',
				'farm',
				'field',
				'field uid',
				'block',
				'block id',
				'source trees',
				'source samples',
				'name',
				'time',
				'period',
				'replicate',
				'value',
				'recorded by',
				'submitted at',
				'submitted by',
				'partner'
			},
			'property': {'name', 'row', 'column', 'person'},
			'trait': {'name', 'row', 'column', 'person', 'time'},
			'curve': {'name', 'row', 'column', 'person', 'time'},
			'condition': {'name', 'row', 'column', 'person', 'start time', 'end date', 'end time'}
		}

	def file_save(self, file_data):
		# create user upload path if not found
		upload_path = os.path.join(app.config['UPLOAD_FOLDER'], self.username)
		if not os.path.isdir(upload_path):
			os.mkdir(upload_path, mode=app.config['IMPORT_FOLDER_PERMISSIONS'])
			logging.debug('Created upload path for user: %s', self.username)
		file_data.save(self.file_path)

	def file_format_errors(self):
		if self.file_extension.lower() == 'csv':
			with open(self.file_path) as uploaded_file:
				# TODO implement CSV kit checks - in particular csvstat to check field length (avoid stray quotes)
				# now get the dialect and check it conforms to expectations
				dialect = csv.Sniffer().sniff(uploaded_file.read())
				if not all((dialect.delimiter == ',', dialect.quotechar == '"')):
					return 'Please upload comma (,) separated file with quoted (") fields'
		elif self.file_extension.lower() == 'xlsx':
			try:
				wb = load_workbook(self.file_path, read_only=True, data_only=True)
			except BadZipfile:
				logging.info(
					'Bad zip file submitted: \n'
					+ 'username: ' + self.username
					+ 'filename: ' + self.file_path
				)
				return 'This file does not appear to be a valid xlsx file'
			if not set(app.config['WORKSHEET_NAMES'].values()) & set(wb.sheetnames):
				# Need to check for curve input worksheets
				statement = (
					' MATCH '
					'	(input: Input)-[:OF_TYPE]->(:RecordType {name_lower:"curve"}) '
					' WHERE input.name_lower IN $names '
					' RETURN input.name '
				)
				parameters = {
					'names': [i.lower for i in wb.sheetnames]
				}
				with get_driver().session() as neo4j_session:
					result = neo4j_session.read_transaction(
						bolt_result,
						statement,
						parameters
					)
				if not result.single():
					return (
						'This workbook does not appear to contain any of the following accepted worksheets: <br> - '
						+ '<br>  - '.join([str(i) for i in list(app.config['WORKSHEET_NAMES'].values())])
						+ ' nor does it appear to contain a "curve" input variable. '
					)
		else:
			return None

	def check_duplicate_fieldnames(self, worksheet):
		fieldnames_set = set([i for i in self.fieldnames[worksheet]])
		if len(self.fieldnames[worksheet]) > len(fieldnames_set):
			if self.file_extension == 'xlsx':
				if worksheet in app.config['WORKSHEET_NAMES']:
					error_message = '<p>' + app.config['WORKSHEET_NAMES'][worksheet] + ' worksheet '
				else:
					error_message = '<p>' + worksheet + ' worksheet '
			else:
				error_message = '<p>This file '
			self.error_messages.append(error_message + 'contains duplicate column labels. This is not supported.</p>')

	def check_required_fieldnames(self, worksheet):
		record_type = self.get_record_type_from_worksheet(worksheet)
		if record_type:
			if not self.required_sets[record_type].issubset(self.fieldnames[worksheet]):
				self.error_messages.append(
					worksheet +
					' does not appear to contain a full set of required fieldnames for any record type: ' +
					' <br> Property records require: ' + ', '.join(self.required_sets['property']) +
					' <br> Trait and Curve records require: ' + ', '.join(self.required_sets['trait']) +
					' <br> Condition records require:' + ', '.join(self.required_sets['condition'])
				)

	def check_input_variables(self, worksheet):
		# get fieldnames that aren't in the required list or optional list
		# these should be the input variables which we then confirm are found in the db.
		# except for curves, where they should all be numbers
		record_type = self.get_record_type_from_worksheet(worksheet)
		reference_fieldnames = self.required_sets[record_type].union(self.optional_sets[record_type])
		self.input_variables[worksheet] = [
			i for i in self.fieldnames[worksheet] if i.lower() not in reference_fieldnames
		]
		statement = (
			' UNWIND $input_variables AS input_variable '
			'	OPTIONAL MATCH '
			'		(input: Input {name_lower: toLower(trim(toString(input_variable)))}) '
			'	OPTIONAL MATCH '
			'		(input)-[:OF_TYPE]->(record_type: RecordType) '
			'	WITH '
			'		input_variable, record_type '
			'	WHERE '
			'		input IS NULL '
			'		OR '
			'		record_type.name_lower <> $record_type '
			'	RETURN '
			'		input_variable, record_type.name_lower '
		)
		parameters = {
			'record_type': record_type,
			'input_variables': self.input_variables[worksheet]
		}
		with get_driver().session() as neo4j_session:
			unrecognised_input_variables = neo4j_session.read_transaction(
				bolt_result,
				statement,
				parameters
			)
			if unrecognised_input_variables.peek():
				if self.file_extension == 'xlsx':
					error_message = '<p>' + worksheet + ' worksheet '
				else:
					error_message = '<p>This file '
				error_message += (
					'contains column headers that are not recognised as input variables or required details: </p>'
				)
				for input_variable in unrecognised_input_variables :
					error_message += '<dt>' + input_variable[0] + ':</dt> '
					input_record_type = input_variable[1]
					if input_record_type:
						if record_type == 'mixed':
							error_message += (
								' <dd> Required fields missing: '
							)
						else:
							error_message += (
									' <dd> Required fields present for ' +
									record_type +
									' records but this input variable is a ' +
									input_record_type +
									'.'
							)
						if input_record_type == 'condition':
							error_message += (
								'. Condition records require "start date", "start time", "end date" and "end time" '
								' in addition to the "UID" and "Person" fields.'
							)
						elif input_record_type == 'trait':
							error_message += (
								'. Trait records require "date" and "time" fields'
								' in addition to the "UID" and "Person" fields.'
							)
						elif input_record_type == 'property':
							error_message += '. Property records require the "UID" and "Person" fields.'
						error_message += (
							'</dd>\n'
						)
					else:
						error_message += '<dd>Unrecognised input variable. Please check your spelling.</dd>\n'
				self.error_messages.append(error_message)

	def check_fieldnames(self):
		if not self.fieldnames:
			self.error_messages.append(
				'This file does not contain recognised input'
			)
		else:
			for worksheet in list(self.fieldnames.keys()):
				self.check_duplicate_fieldnames(worksheet)
				self.check_required_fieldnames(worksheet)

	def set_record_types_and_fieldnames_from_csv(self):
		record_type = None
		with open(self.file_path) as uploaded_file:
			file_dict = DictReaderInsensitive(uploaded_file)
			if self.submission_type == 'db':  # this type is submitted through the Correct page
				record_type = 'mixed'
				if not self.required_sets[record_type].issubset(set(file_dict.fieldnames)):
					self.error_messages.append(
						'This file does not appear to be a database exported csv file. ' +
						'It should contain the following fieldnames: ' +
						', '.join([str(i) for i in self.required_sets[record_type]])
					)
			elif self.submission_type == 'table':
				# Consider key fieldnames to set the expected type of input
				if 'start_date' in file_dict.fieldnames:
					record_type = 'condition'
				elif 'date' in file_dict.fieldnames:
					record_type = 'trait'  # or curve
					# check if all other fieldnames are valid numbers,
					# if so it should be a table for a curve input variable
					other_fields = set(file_dict.fieldnames) - self.required_sets[record_type] - self.optional_sets[record_type]
					all_numbers = True
					for field in other_fields:
						try:
							float(field)
						except ValueError:
							all_numbers = False
							break
					if all_numbers:
						record_type = 'curve'
				else:
					record_type = 'property'
			else:
				self.error_messages.append('Submission type not recognised')
		if record_type:
			self.fieldnames = {record_type: file_dict.fieldnames}
			self.record_types = [record_type]
		return record_type

	def load_xlsx(self):
		try:
			wb = load_workbook(self.file_path, read_only=True, data_only=True)
			return wb
		except BadZipfile:
			logging.info(
				'Bad zip file submitted: \n'
				+ 'username: ' + self.username
				+ 'filename: ' + self.file_path
			)
			self.error_messages.append(
				'This file does not appear to be a valid xlsx file'
			)
			return None

	def worksheet_to_curve_input(self, worksheet):
		statement = (
			' MATCH '
			'	(input: Input {name_lower: $name})-[:OF_TYPE]->(:RecordType {name_lower:"curve"}) '
			' RETURN input.name '
		)
		parameters = {
			'name': worksheet.lower()
		}
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				bolt_result,
				statement,
				parameters
			)
		if result.peek():
			return result.single()[0]
		else:
			self.error_messages.append(
				' This workbook contains an unsupported worksheet:<br> "' + worksheet + '"<br><br>'
				+ 'Other than worksheets named after curve input variables, only the following are accepted: '
				+ '<ul><li>'
				+ '</li><li>'.join(
					[
						str(i) for i in list(app.config['WORKSHEET_NAMES'].values()) if
						i not in app.config['REFERENCE_WORKSHEETS']
					]
				)
				+ '</li>'
			)
			return None

	def check_curve_table(self, worksheet):
		record_type = 'curve'
		reference_fieldnames = self.required_sets[record_type].union(self.optional_sets[record_type])
		other_fields = set(self.fieldnames[worksheet]) - reference_fieldnames
		all_numbers = True
		for field in other_fields:
			try:
				float(field)
			except ValueError:
				all_numbers = False
				break
		if not all_numbers:
			if self.file_extension == 'xlsx':
				error_message = '<p>' + worksheet + ' worksheet'
			else:
				error_message = self.raw_filename
			error_message += (
				' contains unexpected column labels. '
				' For curve data only float values are accepted in addition to the following labels: '
			)
			# we are calling trait required fieldnames here because they are the same for traits and curves
			# and when uploading a csv we check if this is curve data by all other headers not in required list being numbers
			# so the required fieldnames will have been set as trait
			for i in reference_fieldnames:
				error_message += '<dd>' + str(i) + '</dd>'
			self.error_messages.append(error_message)

	def get_record_type_from_worksheet(self, worksheet):
		record_type = None
		if self.submission_type == 'db':
			record_type = 'mixed'
		elif worksheet.lower() in app.config['WORKSHEET_TYPES']:
			worksheet_type = app.config['WORKSHEET_TYPES'][worksheet.lower()]
			record_type = None if worksheet_type in app.config['REFERENCE_WORKSHEETS'] else worksheet_type
		else:
			input_variable = self.worksheet_to_curve_input(worksheet)
			if input_variable:
				record_type = 'curve'
				self.input_variables[worksheet] = input_variable
		if record_type and record_type not in self.record_types: # consider using a set for record types, is order important?
			self.record_types.append(record_type)
		return record_type

	def set_record_types_and_fieldnames_from_xlsx(self):
		wb = self.load_xlsx()
		if wb:
			for worksheet in wb.sheetnames:
				record_type = self.get_record_type_from_worksheet(worksheet)
				if record_type:
					ws = wb[worksheet]
					rows = ws.iter_rows(min_row=1, max_row=1)
					first_row = next(rows)
					self.fieldnames[worksheet] = [
						c.value.strip().lower()
						if isinstance(c.value, str)
						else str(c.value)
						for c in first_row
						if c.value
					]
					if record_type == 'curve':
						# check the fieldnames that aren't required/optional for curved inputs are all numbers:
						self.check_curve_table(worksheet)
					else:
						self.check_input_variables(worksheet)

	def set_fieldnames(self):
		if self.file_extension == 'csv':
			self.set_record_types_and_fieldnames_from_csv()
		elif self.file_extension == 'xlsx':
			self.set_record_types_and_fieldnames_from_xlsx()
		else:
			self.error_messages.append('Only csv and xlsx file formats are supported for data submissions')
		self.check_fieldnames()

	# clean up the csv by passing through dict reader and rewriting
	def trim_file(self, worksheet):
		trimmed_file_path = '_'.join([
			os.path.splitext(self.file_path)[0],
			worksheet,
			'trimmed.csv'
		])
		self.trimmed_file_paths[worksheet] = trimmed_file_path
		if self.file_extension == 'csv':
			with open(self.file_path, 'r') as uploaded_file, open(trimmed_file_path, "w") as trimmed_file:
				# this dict reader lowers case and trims whitespace on all headers
				file_dict = DictReaderInsensitive(
					uploaded_file,
					skipinitialspace=True
				)
				file_writer = csv.DictWriter(
					trimmed_file,
					fieldnames=['row_index'] + file_dict.fieldnames,
					quoting=csv.QUOTE_ALL
				)
				file_writer.writeheader()
				for i, row in enumerate(file_dict):
					self.row_count[worksheet] = i + 1
					# remove rows without entries
					if any(field.strip() for field in row):
						for field in file_dict.fieldnames:
							if row[field]:
								row[field] = row[field].strip()
						row['row_index'] = self.row_count[worksheet]
						file_writer.writerow(row)
		elif all([
			self.file_extension == 'xlsx',
			self.submission_type in ['db', 'table']
		]):
			wb = load_workbook(self.file_path, read_only=True, data_only=True)
			if self.submission_type == 'db':
				ws = wb['Records']
			elif self.submission_type == 'table':
				if worksheet in app.config['WORKSHEET_NAMES']:
					ws = wb[app.config['WORKSHEET_NAMES'][worksheet]]
				else:
					ws = wb[worksheet]
			with open(trimmed_file_path, "wt") as trimmed_file:
				file_writer = csv.writer(
					trimmed_file,
					quoting=csv.QUOTE_ALL
				)
				rows = ws.iter_rows()
				first_row = next(rows)
				file_writer.writerow(
					[
						'row_index'] + [
						str(cell.value.encode('utf8')).lower()
						if isinstance(cell.value, str)
						else str(cell.value)
						for cell in first_row
						if cell.value
					]
				)
				# handle deleted columns in the middle of the worksheet
				empty_headers = []
				time_column_index = None
				date_column_index = None
				for i, cell in enumerate(first_row):
					if not cell.value:
						empty_headers.append(i)
					if isinstance(cell.value, str):
						if cell.value.strip().lower() == 'time':
							time_column_index = i
				for j, row in enumerate(rows):
					# j+2 to store the "Line number" as index
					# this is 0 based, and accounts for header
					self.row_count[worksheet] = j + 2
					cell_values = [cell.value for cell in row]
					# remove columns with empty header
					for i in sorted(empty_headers, reverse=True):
						del cell_values[i]
					# remove empty rows
					for i, value in enumerate(cell_values):
						if isinstance(value, datetime.datetime):
							# when importing from excel if the time isn't between 00:00 and 24:00
							# then it is imported as a datetime object relative to 1900,1,1,0,0
							# so we catch this here by checking if we are looking at the time column
							if i == time_column_index:
								cell_values[i] = value.strftime("%H:%M")
							else:
								try:
									cell_values[i] = value.strftime("%Y-%m-%d")
								except ValueError:
									cell_values[i] = 'Dates before 1900 are not supported'
						elif isinstance(value, datetime.time):
							cell_values[i] = value.strftime("%H:%M")
						else:
							if isinstance(value, str):
								cell_values[i] = value.strip()
					if any(value for value in cell_values):
						cell_values = [self.row_count[worksheet]] + cell_values
						file_writer.writerow(cell_values)

	def parse_rows(self, worksheet):
		trimmed_file_path = self.trimmed_file_paths[worksheet]
		submission_type = self.submission_type
		parse_result = ParseResult(submission_type, worksheet, self.fieldnames[worksheet])
		self.parse_results[worksheet] = parse_result
		if submission_type == 'table':
			record_type = self.get_record_type_from_worksheet(worksheet)
		elif submission_type == 'db':
			record_type = 'mixed'
		if record_type == 'curve':
			x_values = set(self.fieldnames[worksheet]) - self.required_sets[record_type]
		with open(trimmed_file_path, 'r') as trimmed_file:
			trimmed_dict = DictReaderInsensitive(trimmed_file)
			for row in trimmed_dict:
				if submission_type == 'table':
					if record_type != 'curve':
						# first check for input data, if none then just skip this row
						if worksheet in self.input_variables:
							if [row[input_variable] for input_variable in self.input_variables[worksheet] if row[input_variable]]:
								parse_result.contains_data = True
								parse_result.parse_table_row(row)
					else:
						if [row[x] for x in x_values if row[x]]:
							parse_result.contains_data = True
							parse_result.parse_table_row(row)
				elif submission_type == 'db':
					parse_result.parse_db_row(row)
		if parse_result.contains_data:
			self.contains_data = True
		if parse_result.duplicate_keys:
			self.error_messages.append(
				'<p>' + worksheet
				+ 'worksheet contains duplicate keys:</p>'
				+ parse_result.duplicate_keys_table()
			)

	def db_check(
		self,
		tx,
		worksheet
	):
		# todo it seems like the field errors here should not occur
		# todo we are handling this check before we parse the rows
		# todo so we could remove this check for input variables from  here to simplify
		username = self.username
		if worksheet.lower() in app.config['WORKSHEET_TYPES']:
			record_type = app.config['WORKSHEET_TYPES'][worksheet.lower()]
		else:
			record_type = 'curve'
		trimmed_file_path = self.trimmed_file_paths[worksheet]
		trimmed_filename = os.path.basename(trimmed_file_path)
		submission_type = self.submission_type
		parse_result = self.parse_results[worksheet]
		with open(trimmed_file_path, 'r') as trimmed_file:
			if submission_type == 'db':
				trimmed_dict_reader = DictReaderInsensitive(trimmed_file)
				inputs_set = set()
				# todo move this to the parse procedure where we iterate through the file already
				for row in trimmed_dict_reader:
					inputs_set.add(row['input variable'].lower())
				record_types = tx.run(
					' UNWIND $inputs as input_name'
					'	MATCH '
					'	(f:Input { '
					'		name_lower: input_name '
					'	})-[:OF_TYPE]->(record_type: RecordType) '
					' RETURN distinct(record_type.name_lower) ',
					inputs=list(inputs_set)
				).value()
				if not set(self.required_sets[worksheet]).issubset(set(self.fieldnames[worksheet])):
					missing_fieldnames = set(self.required_sets[worksheet]) - set(self.fieldnames[worksheet])
					if self.file_extension == 'xlsx':
						error_message = '<p>' + app.config['WORKSHEET_NAMES'][worksheet] + ' worksheet '
					else:
						error_message = '<p>This file '
					error_message += (
							' is missing the following required fields: '
							+ ', '.join([i for i in missing_fieldnames])
							+ '</p>'
					)
					self.error_messages.append(error_message)
			elif submission_type == 'table' and worksheet in self.input_variables:
				if record_type == 'property':
					statement = Cypher.upload_table_property_check
					parameters = {
						'username': username,
						'filename': urls.url_fix('file:///' + username + '/' + trimmed_filename ),
						'inputs': self.input_variables[worksheet],
						'record_type': record_type
					}
				elif record_type == 'trait':
					statement = Cypher.upload_table_trait_check
					parameters = {
						'username': username,
						'filename': urls.url_fix('file:///' + username + '/' + trimmed_filename),
						'inputs': self.input_variables[worksheet],
						'record_type': record_type
					}
				elif record_type == 'condition':
					statement = Cypher.upload_table_condition_check
					parameters = {
						'username': username,
						'filename': urls.url_fix('file:///' + username + '/' + trimmed_filename),
						'inputs': self.input_variables[worksheet],
						'record_type': record_type
					}
				elif record_type == 'curve':
					statement = Cypher.upload_table_curve_check
					reference_fields = self.required_sets[record_type].union(self.optional_sets[record_type])
					parameters = {
						'username': username,
						'filename': urls.url_fix('file:///' + username + '/' + trimmed_filename),
						'input_name': self.input_variables[worksheet],
						'x_values': sorted(
							[float(i) for i in self.fieldnames[worksheet] if i.lower() not in reference_fields]
						),
						'record_type': record_type
					}
				else:
					logging.warn('record type not recognised')
					return
				check_result = tx.run(
					statement,
					parameters
				)
				trimmed_dict_reader = DictReaderInsensitive(trimmed_file)
				row = next(trimmed_dict_reader)
				# need to check_result sorted by file/dictreader row_index
				for item in check_result:
					record = item[0]
					while record['row_index'] != int(row['row_index']):
						row = next(trimmed_dict_reader)
					if not record['UID']:
						parse_result.merge_error(
							row,
							"uid",
							"missing"
						)
					if not record['Input variable']:
						parse_result.add_field_error(
							record['Supplied input name'],
							(
								"This input variable is not found. Please check your spelling. "
								"This may also be because the input variable is not available at the level of these items"
							)
						)
					# we add found fields to a list to handle mixed items in input
					# i.e. if found at level of one item but not another
					else:
						parse_result.add_field_found(record['Input variable'])
					if all([
						record['UID'],
						record['Input variable'],
						not record['Value']
					]):
						parse_result.merge_error(
							row,
							record['Supplied input name'],
							"format",
							input_name=record['Input variable'],
							input_format=record['Format'],
							category_list=record['Category list']
						)
					# need to check an element of the list as all results
					if record['Conflicts'][0]['existing_value']:
						parse_result.merge_error(
							row,
							record['Supplied input name'],
							"conflict",
							conflicts=record['Conflicts']
						)
				if parse_result.field_found:
					for field in parse_result.field_found:
						parse_result.rem_field_error(field)
		if parse_result.field_errors:
			if self.file_extension != 'xlsx':
				self.error_messages.append(
					'<p> This file contains unrecognised column labels: </p><ul><li>'
					+ '</li><li>'.join(parse_result.field_errors)
					+ '</li></ul>'
				)
			else:
				if worksheet in app.config['WORKSHEET_NAMES']:
					self.error_messages.append(
						'<p>' + app.config['WORKSHEET_NAMES'][worksheet]
						+ ' worksheet contains unrecognised column labels: </p><ul><li>'
						+ '</li><li>'.join(parse_result.field_errors)
						+ '</li></ul>'
					)
				else:
					self.error_messages.append(
						'<p>' + worksheet
						+ ' worksheet contains unrecognised column labels: </p><ul><li>'
						+ '</li><li>'.join(parse_result.field_errors)
						+ '</li></ul>'
					)
		if parse_result.errors:
			if self.file_extension != 'xlsx':
				self.error_messages.append(
					'<p> This file contains errors: </p>'
					+ parse_result.html_table()
				)
			else:
				if worksheet in app.config['WORKSHEET_NAMES']:
					self.error_messages.append(
						'<p>' + app.config['WORKSHEET_NAMES'][worksheet]
						+ ' worksheet contains errors: '
						+ parse_result.html_table()
					)
				else:
					self.error_messages.append(
						'<p>' + worksheet
						+ ' worksheet contains errors: '
						+ parse_result.html_table()
					)

	def submit(
			self,
			tx,
			worksheet
	):
		username = self.username
		if worksheet.lower() in app.config['WORKSHEET_TYPES']:
			record_type = app.config['WORKSHEET_TYPES'][worksheet.lower()]
		else:
			record_type = 'curve'
		trimmed_file_path = self.trimmed_file_paths[worksheet]
		trimmed_filename = os.path.basename(trimmed_file_path)
		submission_type = self.submission_type
		filename = self.filename
		inputs = self.input_variables[worksheet] if worksheet in self.input_variables else None
		with contextlib.closing(SubmissionResult(username, filename, submission_type, worksheet)) as submission_result:
			self.submission_results[worksheet] = submission_result
			if record_type == 'property':
				statement = Cypher.upload_table_property
				result = tx.run(
					statement,
					username=username,
					filename=urls.url_fix("file:///" + username + '/' + trimmed_filename),
					inputs=inputs,
					record_type=record_type
				)
			elif record_type == 'trait':
				statement = Cypher.upload_table_trait
				result = tx.run(
					statement,
					username=username,
					filename=urls.url_fix("file:///" + username + '/' + trimmed_filename),
					inputs=inputs,
					record_type=record_type
				)
			elif record_type == 'condition':
				statement = Cypher.upload_table_condition
				result = tx.run(
					statement,
					username=username,
					filename=urls.url_fix("file:///" + username + '/' + trimmed_filename),
					inputs=inputs,
					record_type=record_type
				)
			elif record_type == 'curve':
				statement = Cypher.upload_table_curve
				reference_fields = self.required_sets[record_type].union(self.optional_sets[record_type])
				result = tx.run(
					statement,
					username=username,
					filename=urls.url_fix("file:///" + username + '/' + trimmed_filename),
					input_name=self.input_variables[worksheet],
					x_values=sorted(
						[float(i) for i in self.fieldnames[worksheet] if i not in reference_fields]
					),
					record_type=record_type
				)
			else:
				logging.warn('Record type not recognised')
			# create a submission result and update properties from result
			if record_type == 'property':
				self.property_updater = PropertyUpdateHandler(tx)
			for record in result:
				submission_result.parse_record(record[0])
				if record_type == 'property':
					if self.property_updater.process_record(record[0]):
						break
			# As we are collecting property updates we need to run the updater at the end
			if not result.peek():
				if record_type == 'property':
					self.property_updater.update_all()

	@celery.task(bind=True)
	def async_submit(self, username, upload_object):
		try:
			upload_object.set_fieldnames()
			if upload_object.error_messages:
				with app.app_context():
					html = render_template(
						'emails/upload_report.html',
						response='<br>'.join(upload_object.error_messages)
					)
					subject = "BreedCAFS upload rejected"
					recipients = [User(username).find('')['email']]
					response = "Submission rejected due to invalid file:\n " + '<br>'.join(upload_object.error_messages)
					body = response
					send_email(subject, app.config['ADMINS'][0], recipients, body, html)
				return {
					'status': 'ERRORS',
					'result': (
							'Submission report for file: ' + upload_object.raw_filename + '<br><br>' +
							'<br>'.join(upload_object.error_messages)
					)
				}
			with get_driver().session() as neo4j_session:
				if upload_object.file_extension in ['csv', 'xlsx']:
					with neo4j_session.begin_transaction() as tx:
						for worksheet in list(upload_object.fieldnames.keys()):
							# todo Stop trimming before parsing, this should be done in one pass of the file
							# clean up the file removing empty lines and whitespace, lower case headers for matching in db
							upload_object.trim_file(worksheet)
							# parse the trimmed file/worksheet for errors
							# also adds parse_result to upload_object.parse_result dict (by record_type)
							upload_object.parse_rows(worksheet)
							# with string parsing performed, now we check against the database for UID, input variable, value
							upload_object.db_check(tx, worksheet)
						if not upload_object.contains_data:
							upload_object.error_messages.append(
								upload_object.raw_filename
								+ ' appears to contain no input values. '
							)
						if upload_object.error_messages:
							error_messages = '<br>'.join(upload_object.error_messages)
							with app.app_context():
								html = render_template(
									'emails/upload_report.html',
									response=(
										'Submission report for file: ' + upload_object.raw_filename + '<br><br>' +
										error_messages
									)
								)
								subject = "BreedCAFS upload rejected"
								recipients = [User(username).find('')['email']]
								body = (
										'Submission report for file: ' + upload_object.raw_filename + '<br><br>'
										+ error_messages
								)
								send_email(subject, app.config['ADMINS'][0], recipients, body, html)
							return {
								'status': 'ERRORS',
								'result': (
									'Submission report for file: ' + upload_object.raw_filename + '<br><br>' +
									error_messages
								)
							}
						conflict_files = []
						submissions = []
						for worksheet in list(upload_object.fieldnames.keys()):
							# submit data
							upload_object.submit(tx, worksheet)
							submission_result = upload_object.submission_results[worksheet]
							if submission_result.conflicts_found:
								conflict_files.append(submission_result.conflicts_file_path)
								os.unlink(submission_result.submissions_file_path)
							else:
								os.unlink(submission_result.conflicts_file_path)
								if submission_result.submissions_file_path:
									submissions.append((
										worksheet,
										submission_result.summary(),
										submission_result.submissions_file_path
									))
						if conflict_files:
							# These should only be generated due to concurrent conflicting submissions
							# or internal conflicts in the submitted file
							tx.rollback()
							with app.app_context():
								response = (
									'Submission report for file: ' + upload_object.raw_filename + '<br><br>' +
									'<p>Conflicting records were either submitted concurrently '
									'or are found within in the submitted file. '
									'Your submission has been rejected. Please address the conflicts listed '
									'in the following files before resubmitting. '
									'\n'
								)
								for conflict_file in conflict_files:
									response += ' - '
									response += (
										url_for(
											'download_file',
											username=username,
											filename=os.path.basename(conflict_file),
											_external=True
										)
									)
									response += '\n'
								response += '</p>'
								html = render_template(
									'emails/upload_report.html',
									response=(
										'Submission report for file: ' + upload_object.raw_filename + '<br><br>' +
										response
									)
								)
								# send result of merger in an email
								subject = "BreedCAFS upload rejected"
								recipients = [User(username).find('')['email']]
								body = (
									'Submission report for file: ' + upload_object.raw_filename + '<br><br>' +
									response
								)
								send_email(subject, app.config['ADMINS'][0], recipients, body, html)
								return {
									'status': 'ERRORS',
									'result': (
											'Submission report for file: ' + upload_object.raw_filename + '<br><br>' +
											response
									)
								}
						if 'property' in upload_object.record_types:
							property_updater = upload_object.property_updater
							if property_updater.errors:
								property_updater.format_error_list()
								error_messages = [
									item for errors in list(property_updater.errors.values()) for item in errors
								]
								error_messages = '<br>'.join(error_messages)
								tx.rollback()
								with app.app_context():
									response = (
											'Submission report for file: ' + upload_object.raw_filename + '<br><br>' +
											error_messages
									)
									html = render_template(
										'emails/upload_report.html',
										response=response
									)
									subject = "BreedCAFS upload rejected"
									recipients = [User(username).find('')['email']]
									body = response
									send_email(subject, app.config['ADMINS'][0], recipients, body, html)
									return {
										'status': 'ERRORS',
										'result': response
									}
						tx.commit()
						# now need app context for the following (this is running asynchronously)
						with app.app_context():
							if not submissions:
								response = 'No data submitted, please check that you uploaded a completed file'
								return {
									'status': 'ERRORS',
									'result': (
										'Submission report for file: ' + upload_object.raw_filename + '<br><br>' +
										response
									)
								}
							else:
								response = "<p>"
								for submission in submissions:
									submission_url = url_for(
										'download_file',
										username=username,
										filename=os.path.basename(submission[2]),
										_external=True,
										# adding a parameter here to stop browser from accessing cached version of file
										date=datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S-%f')
									)
									if submission[0] in app.config['WORKSHEET_NAMES']:
										response += (
											' <br> - <a href= ' + submission_url + '>'
											+ str(app.config['WORKSHEET_NAMES'][submission[0]])
											+ '</a> contained ' + str(submission[1]['submitted']) + ' new '
											+ ' and ' + str(submission[1]['resubmissions']) + ' existing records.'
										)
									else:
										response += (
												' <br> - <a href= ' + submission_url + '>'
												+ str(submission[0])
												+ '</a> contained ' + str(submission[1]['submitted']) + ' new '
												+ ' and ' + str(submission[1]['resubmissions']) + ' existing records.'
										)
								response += '</p>'
								html = render_template(
									'emails/upload_report.html',
									response=(
										'Submission report for file: ' + upload_object.raw_filename + '<br><br>' +
										response
									)
								)
								# send result of merger in an email
								subject = "BreedCAFS upload summary"
								recipients = [User(username).find('')['email']]
								body = (
									'Submission report for file: ' + upload_object.raw_filename + '<br><br>' +
									response
								)
								send_email(subject, app.config['ADMINS'][0], recipients, body, html)
								return {
									'status': 'SUCCESS',
									'result': (
										'Submission report for file: ' + upload_object.raw_filename + '<br><br>' +
										response
									)
								}
				else:
					return {
						'status': 'SUCCESS',
						'result': "Nothing done with the file"
					}
		except (ServiceUnavailable, SecurityError) as exc:
			raise self.retry(exc=exc)

	def correct(
			self,
			tx,
			access,
			record_type
	):
		if access == 'global_admin':
			statement = (
				' MATCH '
				'	(partner:Partner) '
				'	<-[:AFFILIATED {data_shared:True})-(user:User) '
				' MATCH '
				'	(current_user: User { '
				'		username_lower: toLower($username) '
				'	}) '
				' WHERE "global_admin" IN current_user.access '
			)
		elif access == 'partner_admin':
			statement = (
				' MATCH '
				'	(current_user: User {'
				'		username_lower: toLower($username)'
				'	}) '
				'	-[: AFFILIATED {admin: True}]->(partner: Partner) '
				' WHERE "partner_admin" IN current_user.access '
				' MATCH '
				'	(partner) '
				'	<-[:AFFILIATED {data_shared:True}]-(user:User) '
			)
		else:
			statement = (
				' MATCH '
				'	(partner: Partner) '
				'	<-[:AFFILIATED {data_shared:True}]-(user: User {'
				'		username_lower: toLower($username)'
				'	}) '
				' WITH '
				'	partner, '
				'	user as user, '
				'	user as current_user '
			)
		statement += (
			' LOAD CSV WITH HEADERS FROM $filename as csvLine '
			'	WITH '
			'	current_user, '
			'	user, partner, '
			'	toInteger(csvLine.row_index) as row_index, '
			'	apoc.date.parse('
			'		csvLine.`submitted at`,'
			'		"ms", '
			'		 "yyyy-MM-dd HH:mm:ss"'
			'	) as `submitted at`, '
			'	toLower(csvLine.`input variable`) as input_name, '
			'	CASE '
			'		WHEN toInteger(csvLine.uid) IS NOT NULL '
			'		THEN toInteger(csvLine.uid) '
			'		ELSE toUpper(csvLine.uid)'
			'	END as uid, ' 
			'	toInteger(csvLine.replicate) as replicate, '
			'	CASE '
			'		WHEN csvLine.time <> "" '
			'		THEN apoc.date.parse( '
			'			csvLine.time, '
			'			"ms", '
			'			 "yyyy-MM-dd HH:mm" '
			'			) '
			'		ELSE Null '
			'	END as time, '
			'	CASE '
			'		WHEN size(split(csvLine.period, " - ")) > 1 '
			'		THEN CASE '
			'			WHEN toLower(split(csvLine.period, " - ")[0]) = "undefined" '
			'			THEN False '
			'			ELSE apoc.date.parse('
			'				split(csvLine.period, " - ")[0],'
			'				"ms", '
			'				 "yyyy-MM-dd HH:mm" '
			'			) '
			'		END '
			'		ELSE Null '
			'	END as start, '
			'	CASE '
			'		WHEN size(split(csvLine.period, " - ")) > 1 '
			'		THEN CASE '
			'			WHEN toLower(split(csvLine.period, " - ")[1]) = "undefined" '
			'			THEN False '
			'			ELSE apoc.date.parse('
			'				split(csvLine.period, " - ")[1],'
			'				"ms", '
			'				 "yyyy-MM-dd HH:mm"'
			'			) '
			'		END '
			'		ELSE Null '
			'	END as end, '
			'	CASE '
			# match at least one valid entry
			'		WHEN csvLine.value =~ "\\\\[.+,.+\\\\].*" '
			'		THEN '		
			'			[x in split(csvLine.value, "],") | [i in split(x, ",") | toFloat(replace(replace(i, "[", ""), "]", ""))][0]] '
			'		ELSE Null '
			'	END as x_values, '
			'	CASE '
			# match at least one valid entry
			'		WHEN csvLine.value =~ "\\\\[.+,.+\\\\].*" '
			'		THEN '		
			'			[x in split(csvLine.value, "],") | [i in split(x, ",") | toFloat(replace(replace(i, "[", ""), "]", ""))][1]] '
			'		ELSE Null '
			'	END as y_values '		
			' MATCH '
			'	(user)'
			'	-[:SUBMITTED]->(:Submissions)  '
			'	-[:SUBMITTED]->(:Records) '
			'	-[:SUBMITTED]->(uff:UserFieldInput) '
			'	-[submitted:SUBMITTED]->(record :Record) '
			'	-[record_for:RECORD_FOR]->(if:ItemInput) '
			'	-[:FOR_ITEM]-(item:Item {'
			'		uid: uid'
			'	}), '
			'	(if)'
			'	-[FOR_INPUT*..2]->(input:Input {'
			'		name_lower:input_name'
			'	})-[:OF_TYPE]->(record_type:RecordType) '
			' WHERE '
			# account for rounding to nearest second for submitted at in output files
			'	round(submitted.time / 1000) * 1000 = `submitted at` '
			'	AND '
			'	CASE '
			'		WHEN record.time IS NULL '
			'		THEN True '
			'		ELSE record.time = time '
			'	END '
			'	AND '
			'	CASE '
			'		WHEN record.start IS NULL '
			'		THEN True '
			'		ELSE record.start = start '
			'	END'
			'	AND '
			'	CASE '
			'		WHEN record.end IS NULL '
			'		THEN True '
			'		ELSE record.end = end '
			'	END '
			'	AND '
			'	CASE '
			'		WHEN record.replicate IS NULL '
			'		THEN True '
			'		ELSE record.replicate = replicate '
			'	END '
			'	AND '
			'	CASE '
			'		WHEN record_type.name_lower = "curve" '
			'		AND record.x_values = x_values '
			'		AND record.y_values = y_values '
			'		THEN True '
			'		ELSE record_type.name_lower IN ["property", "trait", "condition"] '
			'	END '
			' MERGE '
			'	(current_user)'
			'	-[:DELETED]->(del:Deletions) '
			' CREATE '
			'	(del)-[:DELETED { '
			'		`deleted at`: datetime.transaction().epochMillis '
			'	}]->(record) '
			' CREATE '
			'	(record)-[:DELETED_FROM]->(if) '
			' DELETE '
			'	record_for '
			' RETURN { '
			'	time: time, '
			'	record_time: record.time, '
			'	uid: uid, '
			'	`input variable`: input.name_lower, '
			'	row_index: row_index '
			' } '
			' ORDER BY row_index '
		)
		trimmed_file_path = self.trimmed_file_paths[record_type]
		trimmed_filename = os.path.basename(trimmed_file_path)
		result = tx.run(
				statement,
				username=self.username,
				access=access,
				filename=("file:///" + self.username + '/' + trimmed_filename)
			)
		return result

	@staticmethod
	def remove_properties(tx, property_uid):
		inputs_properties = app.config['INPUTS_PROPERTIES']
		properties_inputs = app.config['PROPERTIES_INPUTS']
		for input_variable, item_uids in property_uid.items():
			if property_uid[input_variable]:
				if inputs_properties[input_variable] == 'name':
					tx.run(
						' UNWIND $uid_list as uid'
						' MATCH '
						'	(item: Item {uid: uid}) '
						' REMOVE item.name ',
						uid_list=property_uid[input_variable]
					)
				elif inputs_properties[input_variable] == 'row':
					tx.run(
						' UNWIND $uid_list as uid'
						' MATCH '
						'	(item: Item {uid: uid}) '
						' REMOVE item.row ',
						uid_list=property_uid[input_variable]
					)
				elif inputs_properties[input_variable] == 'column':
					tx.run(
						' UNWIND $uid_list as uid'
						' MATCH '
						'	(item: Item {uid: uid}) '
						' REMOVE item.column ',
						uid_list=property_uid[input_variable]
					)
				elif inputs_properties[input_variable] == 'location':
					tx.run(
						' UNWIND $uid_list as uid'
						' MATCH '
						'	(item: Item {uid: uid}) '
						' REMOVE item.location ',
						uid_list=property_uid[input_variable]
					)
				elif inputs_properties[input_variable] == 'variety':
					tx.run(
						' UNWIND $uid_list as uid '
						' MATCH '
						'	(item: Item { '
						'		uid: uid '
						'	}) '
						' OPTIONAL MATCH '
						'	(item) '
						'	<-[:FOR_ITEM]-(ii: ItemInput) '
						'	-[:FOR_INPUT*2]->(i: Input), '
						'	(ii)<-[:RECORD_FOR]-(:Record) '
						' WHERE i.name_lower IN $property_inputs '
						' WITH '
						'	item '
						' WHERE ii IS NULL '
						' MATCH '
						'	(item)-[of_variety:OF_VARIETY]->(fv:FieldVariety) '
						'	<-[contains_variety:CONTAINS_VARIETY]-(field:Field) '
						' DELETE of_variety '
						' REMOVE item.variety '
						' WITH '
						'	item, fv, contains_variety '
						'	OPTIONAL MATCH '
						'		(item)-[:IS_IN | FROM *]->(ancestor: Item) '
						' WITH '
						'	item, fv, contains_variety, '
						'	collect(distinct ancestor) as ancestors '
						' OPTIONAL MATCH '
						'	(item)<-[:IS_IN | FROM *]-(descendant: Item) '
						' WITH  '
						'	item, fv, contains_variety, '
						'	ancestors + collect(distinct descendant) as lineage '
						' UNWIND lineage AS kin '
						'	OPTIONAL MATCH '
						'		(kin)-[:IS_IN | FROM *]->(kin_ancestor: Item) '
						'	WITH '
						'		item, fv, contains_variety, '
						'		kin, '
						'		collect(distinct kin_ancestor) as kin_ancestors '
						'	OPTIONAL MATCH '
						'		(kin)<-[:IS_IN | FROM *]-(kin_descendant: Item) '
						'	WITH '
						'		item, fv, contains_variety, '
						'		kin, '			
						'		kin_ancestors + collect(distinct kin_descendant) as kin_lineage '
						'	UNWIND '
						'		kin_lineage as kin_of_kin '
						'	WITH '
						'		item, fv, contains_variety, '
						'		kin, '
						'		collect(distinct kin_of_kin.variety) as kin_varieties '
						'	SET kin.varieties = CASE WHEN kin.variety IS NOT NULL THEN [kin.variety] ELSE kin_varieties END '
						' WITH '
						'	item, fv, contains_variety, '
						'	kin, '
						'	collect(kin.variety) as varieties '
						' SET item.varieties = CASE WHEN item.variety IS NOT NULL THEN [item.variety] ELSE varieties END '
						' WITH DISTINCT '
						'	fv, contains_variety '
						' OPTIONAL MATCH '
						'	(:Item)-[of_variety:OF_VARIETY]->(fv) '
						' FOREACH (n IN CASE WHEN of_variety IS NULL THEN [1] ELSE [] END | '
						'	DELETE contains_variety '
						' ) ',
						uid_list=property_uid[input_variable],
						property_inputs=properties_inputs[inputs_properties[input_variable]]
					)
				elif inputs_properties[input_variable] == 'unit':
					# There are multiple inputs for unit
					# we only need to update the property if there is no other record
					tx.run(
						' UNWIND $uid_list as uid'
						' MATCH '
						'	(item: Item {uid: uid}) '
						' OPTIONAL MATCH '
						'	(item) '
						'	<-[:FOR_ITEM]-(ii: ItemInput) '
						'	-[:FOR_INPUT*2]->(i: Input), '
						'	(ii)<-[:RECORD_FOR]-(:Record) '
						' WHERE i.name_lower IN $property_inputs '
						' WITH item WHERE ii IS NULL '
						' REMOVE item.unit ',
						uid_list=property_uid[input_variable],
						property_inputs=properties_inputs[inputs_properties[input_variable]]
					)
				elif inputs_properties[input_variable] == 'source':
					# There may be multiple inputs for tree/sample item sources (by id, by name)
					#  and multiple contributions with variable specificity for samples
					#  but conflicts are prevented between assignments at same level and
					#  only assigment to greater specificity is allowed so
					#  we only need to update if the deleted record is the latest "source" submission for this item
					#  we could check this by comparing the latest record 'value' against the current assignments
					#  but it might be sensible to just always update to assignment from latest record submission
					# we do also need to update the varieties for new item lineage and prior ancestors
					statement = (
						' UNWIND $uid_list as uid '
						' MATCH '
						'	(item: Item {uid: uid}) '
						'	-[:FROM | IS_IN]->(ancestor:Item) '
						' WITH item, collect(ancestor) as prior_ancestors '
						' OPTIONAL MATCH '
						'	(item) '
						'	<-[:FOR_ITEM]-(ii: ItemInput) '
						'	-[:FOR_INPUT*2]->(input: Input), '
						'	(ii)<-[:RECORD_FOR]-(record:Record) '
						'	<-[s:SUBMITTED]-() '
						' WHERE input.name_lower IN $property_inputs '
					)
					if 'to block' in input_variable:
						# these will revert to field level if no records
						# but remain unchanged if existing source assignment records
						statement += (
							' AND ii IS NULL '
						)
						if 'assign tree' in input_variable:
							# these are trees so can just be deleted and the counter decremented
							# if no other records specifying and assignment
							# The IS_IN field relationship is retained when assigning tree to block
							statement += (
								' WITH item, prior_ancestors '
								' MATCH (item)-[is_in:IS_IN]->(:BlockTrees)<-[:FOR]-(c:Counter) '
								' SET c._LOCK_ = True '
								' DELETE is_in '
								' SET c.count = c.count - 1 '
								' REMOVE c._LOCK_ '
								' WITH '
								'	item, prior_ancestors '
							)
						else:  # assign sample
							# these must be reattached to field ItemSamples container
							statement += (
								' WITH item, prior_ancestors '
								' MATCH (item)-[from:FROM]->(:ItemSamples) '
								' MATCH (item)-[:FROM | IS_IN]->(field: Field) '
								' DELETE from '
								' MERGE (is:ItemSamples)-[:FROM]->(field) '
								' CREATE (item)-[:FROM]->(is) '
								' WITH DISTINCT '
								'	item, prior_ancestors '
							)
					else:  # to tree or to sample (these are all samples)
						# for these we need to find the most recent record and update accordingly,
						# if no record then we re-attach to the Field ItemSamples
						# we only need to assess one record since if they are submitted concurrently they must agree
						# we also don't need to check for variety conflicts as the update will be to
						# either the same items, a subset thereof or an ancestor
						statement += (
							' WITH '
							'	item, prior_ancestors, '
							'	collect(input.name_lower, record.value, s.time) as records '
							'	max(s.time) as latest '
							' WITH '
							'	item, prior_ancestors, '
							'	[x IN records WHERE x[2] = latest | [x[0], x[1]][0][0] as value, '
							'	[x IN records WHERE x[2] = latest | [x[0], x[1]][0][1] as input_name_lower '
							' MATCH '
							'	(item)-[:FROM | IS_IN*]->(field: Field) '
							' WITH item, prior_ancestors, field, value, input_name_lower '
							' OPTIONAL MATCH '
							'	(new_source: Item)-[:FROM | IS_IN*]->(field) '
							'	WHERE '
							'		CASE '
							'			WHEN "by name" IN input_name_lower ' 
							'			THEN new_source.name_lower IN [x in split(value, ",") | toLower(x) '
							'			ELSE new_source.id IN [x in split(value, ",") | toInteger(x)] '
							'		END '
							'	AND CASE ' 
							'		WHEN "to block" IN input_name_lower '
							'		THEN "Block" in labels(new_source) '
							'		WHEN "to tree" IN input_name_lower '
							'		THEN "Tree" in labels(new_source) '
							'		ELSE "Sample" in labels(new_source) '
							' WITH '
							'	item, prior_ancestors, '
							'	coalesce(new_source, field) as new_source '
							' MERGE '
							'	(is:ItemSamples)-[:FROM]->(new_source) '
							' MERGE '
							'	(item)-[:FROM]->(is) '
							' ) '
							' WITH DISTINCT item, prior_ancestors '
						)
					statement += (
						' OPTIONAL MATCH '
						'	(item)-[:FROM | IS_IN*]->(ancestor: Item) '
						' WITH '
						'	item, '
						'	prior_ancestors, '
						'	collect(ancestor) as ancestors '
						' WITH '
						'	item, '
						'	ancestors,'
						'	[x IN prior_ancestors WHERE NOT x IN ancestors | x ] as removed_ancestors'
						' OPTIONAL MATCH '
						'	(item)<-[:FROM | IS_IN *]-(descendant: Item) '
						' WITH '
						'	item, '
						'	removed_ancestors, '
						'	ancestors + collect(descendant) as lineage '
						' UNWIND lineage AS kin '
						'	OPTIONAL MATCH '
						'		(kin)-[:FROM | IS_IN*]->(kin_ancestor: Item) '
						'	WITH '
						'		item, removed_ancestors,'
						'		kin,'
						'		collect(kin_ancestor) as kin_ancestors '
						'	OPTIONAL MATCH '
						'		(kin)<-[:FROM | IS_IN*]-(kin_descendant: Item) '
						'	WITH '
						'		item, removed_ancestors,'
						'		kin, '
						'		kin_ancestors + collect(kin_descendant) as kin_lineage '
						'	UNWIND kin_lineage AS kin_of_kin '
						'	WITH '
						'		item, removed_ancestors, '
						'		kin, collect(distinct kin_of_kin.variety) as kin_varieties '
						'	SET kin.varieties = CASE WHEN kin.variety IS NOT NULL THEN [kin.variety] ELSE kin_varieties END '
						' WITH '
						'	item, removed_ancestors, '
						'	collect(distinct kin.variety) as varieties '
						' SET item.varieties = CASE WHEN item.variety IS NOT NULL THEN [item.variety] ELSE varieties END '
						' WITH item, removed_ancestors '
						' UNWIND removed_ancestors AS removed_ancestor '
						'	OPTIONAL MATCH '
						'		(removed_ancestor)-[: FROM | IS_IN]->(descendant: Item) '
						'	WITH '
						'		item, removed_ancestor, '
						'		collect(descendant) as removed_ancestor_descendants '
						'	OPTIONAL MATCH '
						'		(removed_ancestor)<-[:FROM | IS_IN]-(ancestor: Item) '
						'	WITH '
						'		item, removed_ancestor, '
						'		removed_ancestor_descendants + collect(ancestor) as removed_ancestor_lineage '
						'	UNWIND removed_ancestor_lineage AS removed_ancestor_kin '
						'	WITH '
						'		item, removed_ancestor, '
						'		collect(removed_ancestor_kin.variety) as removed_ancestor_varieties '
						'	SET removed_ancestor.varieties = '
						'		CASE '
						'			WHEN removed_ancestor.variety IS NOT NULL THEN [removed_ancestor.variety] '
						'			ELSE removed_ancestor_varieties '
						'		END '
					)
					tx.run(
						statement,
						uid_list=property_uid[input_variable],
						property_inputs=properties_inputs[inputs_properties[input_variable]]
					)
				elif inputs_properties[input_variable] == 'time':
					# There is only one input for date and possibly one for time for each level of item.
					# so we always update on delete and can just check the set values
					statement = (
						' UNWIND $uid_list as uid'
						' MATCH '
						'	(item: Item {uid: uid}) '
					)
					if 'date' in input_variable:
						statement += (
							' REMOVE item.date, item.time '
						)
					else:
						statement += (
							' REMOVE item.time_of_day'
							'	 SET item.time = CASE '
							'		WHEN item.date IS NOT NULL '
							'		THEN '
							'			apoc.date.parse(item.date + " 12:00", "ms", "yyyy-MM-dd HH:mm") '
							'		END '
						)
					tx.run(
						statement,
						uid_list=property_uid[input_variable],
					)
				elif inputs_properties[input_variable] == 'elevation':
					tx.run(
						' UNWIND $uid_list as uid'
						' MATCH '
						'	(item: Item {uid: uid}) '
						' REMOVE item.elevation ',
						uid_list=property_uid[input_variable]
					)

	@celery.task(bind=True)
	def async_correct(self, username, access, upload_object):
		try:
			upload_object.set_fieldnames()
			if upload_object.error_messages:
				with app.app_context():
					error_messages = '<br>'.join(upload_object.error_messages)
					html = render_template(
						'emails/upload_report.html',
						response=(
							'Submission report for file: ' + upload_object.raw_filename + '\n<br>' +
							error_messages
						)
					)
					subject = "BreedCAFS upload rejected"
					recipients = [User(username).find('')['email']]
					response = (
							'Submission report for file: ' + upload_object.raw_filename + '\n<br>' +
							"Submission rejected due to invalid file:\n " + error_messages
					)
					body = response
					send_email(subject, app.config['ADMINS'][0], recipients, body, html)
				return {
					'status': 'ERRORS',
					'result': (
							'Submission report for file: ' + upload_object.raw_filename + '\n<br>' +
							error_messages

					)
				}
			with get_driver().session() as neo4j_session:
				with neo4j_session.begin_transaction() as tx:
					# there should only be one record type here, "mixed", so no need to iterate
					record_type = upload_object.record_types[0]
					# todo Stop trimming before parsing, this should be done in one pass of the file
					if upload_object.file_extension == 'xlsx':
						wb = load_workbook(upload_object.file_path, read_only=True, data_only=True)
						if 'Records' not in wb.sheetnames:
							return {
								'status': 'ERRORS',
								'result': (
									'Submission report for file: ' + upload_object.raw_filename + '\n<br>' +
									'This workbook does not contain the expected "Records" worksheet'
								)
							}
					upload_object.trim_file(record_type)
					if not upload_object.row_count[record_type]:
						upload_object.error_messages.append('No records found to delete')
					upload_object.parse_rows(record_type)
					upload_object.db_check(tx, record_type)
					if upload_object.error_messages:
						error_messages = '<br>'.join(upload_object.error_messages)
						with app.app_context():
							response = (
									'Submission report for file: ' + upload_object.raw_filename + '\n<br>' +
									error_messages
							)
							html = render_template(
								'emails/upload_report.html',
								response=response
							)
							subject = "BreedCAFS correction rejected"
							recipients = [User(username).find('')['email']]
							body = response
							send_email(subject, app.config['ADMINS'][0], recipients, body, html)
						return {
							'status': 'ERRORS',
							'result': response
						}
					deletion_result = upload_object.correct(tx, access, record_type)
					# update properties where needed
					property_uid = {
						'set custom name': [],
						'set row': [],
						'set column': [],
						'set sample unit': [],
						'assign tree to block by name': [],
						'assign tree to block by id': [],
						'assign sample to block(s) by name': [],
						'assign sample to block(s) by id': [],
						'assign sample to tree(s) by id': [],
						'assign sample to sample(s) by id': [],
						'set harvest date': [],
						'set planting date': [],
						'set harvest time': [],
						'assign variety name': [],
						'assign variety (el frances code)': []
					}
					# just grab the UID where records are deleted and do the update from any remaining records
					record_tally = {}
					missing_row_indexes = []
					expected_row_index = 1
					if not deletion_result.peek():
						missing_row_indexes += list(range(2, upload_object.row_count[record_type] + 2))
					else:
						for record in deletion_result:
							while (
									expected_row_index != record[0]['row_index']
									and expected_row_index <= upload_object.row_count[record_type] + 2
							):
								expected_row_index += 1
								if expected_row_index != record[0]['row_index']:
									missing_row_indexes.append(expected_row_index)
							if not record[0]['input variable'] in record_tally:
								record_tally[record[0]['input variable']] = 0
							record_tally[record[0]['input variable']] += 1
							if record[0]['input variable'] in property_uid:
								property_uid[record[0]['input variable']].append(record[0]['uid'])
						upload_object.remove_properties(tx, property_uid)
						if not expected_row_index == upload_object.row_count[record_type]:
							missing_row_indexes += list(range(expected_row_index, upload_object.row_count[record_type] + 2))
					if missing_row_indexes:
						missing_row_ranges = (
							list(x) for _, x in itertools.groupby(enumerate(missing_row_indexes), lambda i_x: i_x[0]-i_x[1])
						)
						missing_row_str = str(
							",".join("-".join(map(str, (i[0][1], i[-1][1])[:len(i)])) for i in missing_row_ranges)
						)
						tx.rollback()
						with app.app_context():
							# send result of merger in an email
							subject = 'BreedCAFS correction rejected'
							recipients = [User(username).find('')['email']]
							response = (
								'Submission report for file: ' + upload_object.raw_filename + '\n<br>' +
								'Correction rejected:\n '
							)
							if missing_row_str:
								response += (
									'<p>Records from the following rows of the uploaded file were not found: '
									+ missing_row_str
									+ '\n</p>'
								)
							body = response
							html = render_template(
								'emails/upload_report.html',
								response=response
							)
							send_email(subject, app.config['ADMINS'][0], recipients, body, html)
						return {
							'status': 'ERRORS',
							'result': response
						}
					tx.commit()
					with app.app_context():
						# send result of merger in an email
						subject = 'BreedCAFS correction summary'
						recipients = [User(username).find('')['email']]
						response = (
							'Submission report for file: ' + upload_object.raw_filename + '\n<br>' +
							'Correction report:\n '
						)
						if record_tally:
							response += '<p>The following records were deleted: \n</p>'
							for key in record_tally:
								response += '<p>  -' + str(record_tally[key]) + ' ' + str(key) + ' records deleted\n </p>'
						body = response
						html = render_template(
							'emails/upload_report.html',
							response=response
						)
						send_email(subject, app.config['ADMINS'][0], recipients, body, html)
					return {
						'status': 'SUCCESS',
						'result': response
						}
		except (ServiceUnavailable, SecurityError) as exc:
			raise self.retry(exc=exc)


class Resumable:
	def __init__(self, username, raw_filename, resumable_id):
		self.username = username
		self.resumable_id = resumable_id
		self.chunk_paths = None
		user_upload_dir = os.path.join(
			app.config['UPLOAD_FOLDER'],
			self.username
		)
		if not os.path.isdir(user_upload_dir):
			logging.debug('Creating path for user: %s', username)
			os.mkdir(user_upload_dir, mode=app.config['IMPORT_FOLDER_PERMISSIONS'])
			logging.debug('Created upload path for user: %s', username)
		self.temp_dir = os.path.join(
			app.config['UPLOAD_FOLDER'],
			self.username,
			self.resumable_id
		)
		if not os.path.isdir(self.temp_dir):
			os.mkdir(self.temp_dir, mode=app.config['IMPORT_FOLDER_PERMISSIONS'])
			logging.debug('Created upload path for user: %s', username)
		self.filename = secure_filename(raw_filename)
		self.file_path = os.path.join(
			app.config['UPLOAD_FOLDER'],
			self.username,
			self.filename
		)

	@staticmethod
	def allowed_file(raw_filename, submission_type=None):
		if '.' in raw_filename:
			file_extension = raw_filename.rsplit('.', 1)[1].lower()
			if file_extension in  app.config['ALLOWED_EXTENSIONS']:
				if submission_type:
					if submission_type in ['db', 'table']:
						if file_extension not in ['csv', 'xlsx']:
							return False
						else:
							return True
					elif submission_type == 'seq':
						if file_extension not in ['fastq', 'gz', 'zip']:
							return False
						else:
							return True
					else:
						return False
				else:
					return True
		else:
			return False

	def get_chunk_name(self, chunk_number):
		return self.filename + "_part%03d" % chunk_number

	def check_for_chunk(self, resumable_id, chunk_number):
		temp_dir = os.path.join(
			app.instance_path,
			app.config['UPLOAD_FOLDER'],
			self.username,
			resumable_id
		)
		chunk_file = os.path.join(
			temp_dir,
			self.get_chunk_name(chunk_number)
		)
		logging.debug('Getting chunk: %s', chunk_file)
		if os.path.isfile(chunk_file):
			return True
		else:
			return False

	def save_chunk(self, chunk_data, chunk_number):
		chunk_name = self.get_chunk_name(chunk_number)
		chunk_file = os.path.join(self.temp_dir, chunk_name)
		chunk_data.save(chunk_file)
		logging.debug('Saved chunk: %s', chunk_file)

	def complete(self, total_chunks):
		self.chunk_paths = [
			os.path.join(self.temp_dir, self.get_chunk_name(x))
			for x in range(1, total_chunks + 1)
		]
		return all([
			os.path.exists(p) for p in self.chunk_paths
		])

	def assemble(self, size):
		# replace file if same name already found
		if os.path.isfile(self.file_path):
			os.unlink(self.file_path)
		with open(self.file_path, "ab") as target_file:
			for p in self.chunk_paths:
				stored_chunk_filename = p
				stored_chunk_file = open(stored_chunk_filename, 'rb')
				target_file.write(stored_chunk_file.read())
				stored_chunk_file.close()
				os.unlink(stored_chunk_filename)
			target_file.close()
			os.rmdir(self.temp_dir)
			logging.debug('File saved to: %s', self.file_path)
		return os.path.getsize(self.file_path) == size
