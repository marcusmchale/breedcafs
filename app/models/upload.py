from app import app, os, celery, ServiceUnavailable, SecurityError, logging
import grp
from app.cypher import Cypher
from app.emails import send_email
from app.models.parsers import Parsers
from flask import render_template, url_for
from werkzeug import urls
from user import User
from neo4j_driver import get_driver, bolt_result
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

	def next(self):
		return DictInsensitive(csv.DictReader.next(self))


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
			"timestamp": {
				"format": "Timestamp doesn't match Field Book generated pattern (e.g. 2018-01-01 13:00:00+0100)"
			},
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
			"feature": {
				"missing": (
					"This feature is not found in the database. "
					"Please check the spelling and "
					"that this feature is found among those supported by BreedCAFS "
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
			feature_name=None,
			feature_format=None,
			category_list=None,
			# optional arguments only relevant to conflicts
			conflicts=None
	):
		if field not in self.errors:
			self.errors[field] = []
		self.errors[field].append({
				'error_type': error_type,
				'feature_name': feature_name,
				'feature_format': feature_format,
				'category_list': category_list,
				'conflicts': conflicts
		})

	def headers(self):
		return self.row.keys()

	def html_row(self, fieldnames):
		formatted_cells = {}
		for field in self.errors.keys():
			if field not in fieldnames: # curve conflicts return this
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
							for conflict in error['conflicts']:
								if isinstance(conflict['existing_value'], list):
									for x_y in conflict['existing_value']:
										x_value = x_y[0]
										y_value = x_y[1]
										for x in x_values:
											if float(x) == x_value:
												try:
													if y_value != float(self.row[x]):
														formatted_cells[x] = (
															'<td bgcolor = #FFFF00 title = "'
															+ ''.join(['Existing value: ', str(y_value), '\n'])
															+ '">' + str(self.row[x]) + '</td>'
														)
												except ValueError:
													if y_value != self.row[x]:
														formatted_cells[x] = (
																'<td bgcolor = #FFFF00 title = "'
																+ ''.join(['Existing value: ', str(y_value), '\n'])
																+ '">' + str(self.row[x]) + '</td>'
															)
			else:
				formatted_cells[field] = '<td bgcolor = #FFFF00 title = "'
				for error in self.errors[field]:
					field_error_type = error['error_type']
					field_feature_name = error['feature_name'].lower() if error['feature_name'] else None
					field_feature_format = error['feature_format']
					field_category_list = error['category_list']
					field_conflicts = error['conflicts']
					# if it is a simple error (time format, UID format or UID/Feature not found)
					if field in self.error_comments:
						formatted_cells[field] += self.error_comments[field][field_error_type]
					else:
						if field_error_type == 'format':
							formatted_cells[field] += self.error_comments['other'][field_error_type][field_feature_format]
							if field_feature_name == 'variety name':
								formatted_cells[field] += 'Expected one of the following variety names: \n'
							elif field_feature_name == 'variety code':
								formatted_cells[field] += 'Expected one of the following codes: \n'
							elif field_feature_name == 'fertiliser n:p:k ratio':
								formatted_cells[field] += 'Expected N:P:K ratio format, e.g. 1:1:1'
							elif field_feature_name == 'assign to block':
								formatted_cells[field] += (
									'Expected a block name '
								)
							elif field_feature_name == 'assign to trees':
								formatted_cells[field] += (
									'Expected a comma separated list of integers corresponding to the ID within the field '
								)
							elif 'time' in field_feature_name:
								formatted_cells[field] += 'Expected time format as HH:MM e.g. 13:01'
							if field_category_list:
								formatted_cells[field] += ", ".join([i for i in field_category_list])
						elif field_error_type == 'conflict':
							formatted_cells[field] += self.error_comments['other'][field_error_type]
							# only show 3 conflicts or "title" attribute is overloaded
							# TODO implement better tooltips to include as a table rather than "title" attribute
							for conflict in itertools.islice(field_conflicts, 3):
								formatted_cells[field] += '\n\n'
								formatted_cells[field] += ''.join(['Existing value: ', conflict['existing_value'], '\n'])
								if 'time' in conflict and conflict['time']:
									formatted_cells[field] += (
											'Time: '
											+ datetime.datetime.utcfromtimestamp(int(conflict['time']) / 1000).strftime(
										"%Y-%m-%d %H:%M")
											+ '\n'
									)
								if 'start' in conflict and conflict['start']:
									formatted_cells[field] += (
											'Start: '
											+ datetime.datetime.utcfromtimestamp(
										int(conflict['start']) / 1000).strftime(
										"%Y-%m-%d %H:%M")
											+ '\n'
									)
								if 'end' in conflict and conflict['end']:
									formatted_cells[field] += (
											'End: '
											+ datetime.datetime.utcfromtimestamp(int(conflict['end']) / 1000).strftime(
										"%Y-%m-%d %H:%M")
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
				formatted_cells[field] += '">' + str(self.row[field]) + '</td>'
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
		unique_key = (parsed_uid, parsed_submitted_at, parsed_time, parsed_period, parsed_replicate, row['feature'])
		if unique_key not in self.unique_keys:
			self.unique_keys.add(unique_key)
		else:
			self.duplicate_keys[row['row_index']] = row

	def parse_fb_row(self, row):
		# check time formatting and for FB use trait in unique key.
		if row['trait']:
			self.contains_data = True
		parsed_timestamp = Parsers.timestamp_fb_format(row['timestamp'])
		parsed_uid = Parsers.uid_format(row['uid'])
		if not parsed_timestamp:
			self.merge_error(
				row,
				"timestamp",
				"format"
			)
		unique_key = (parsed_uid, parsed_timestamp, row['trait'])
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
			elif parsed_date and parsed_date is not True:
				time = datetime.datetime.strptime(parsed_date + ' ' + '12:00', '%Y-%m-%d %H:%M')
			else:
				time = None
			unique_key = (parsed_uid, time)
		elif self.record_type == 'condition':
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
		if unique_key not in self.unique_keys:
			self.unique_keys.add(unique_key)
		else:
			if not self.duplicate_keys:
				self.duplicate_keys = {}
			self.duplicate_keys[row['row_index']] = row

	def merge_error(
			self,
			row,
			field,
			error_type,
			# optional arguments only relevant to value errors
			feature_name=None,
			feature_format=None,
			category_list=None,
			# optional arguments only relevant to conflicts
			conflicts=None
	):
		errors = self.errors
		if not int(row['row_index']) in errors:
			errors[int(row['row_index'])] = RowParseResult(row)
		errors[int(row['row_index'])].add_error(field, error_type, feature_name, feature_format, category_list, conflicts)

	def duplicate_keys_table(self):
		if not self.duplicate_keys:
			return '<p>duplicated keys found</p>'
		else:
			max_length = 50
			if self.submission_type == "FB":
				response = (
					'<p>The uploaded file contains duplicated unique keys '
					' (the combination of UID, timestamp and trait).'
					' The following lines duplicate preceding lines in the file: </p>'
				)
			else:
				response = (
					'<p>The uploaded table contains duplicated unique keys '
					'(the combination of UID, date and time). '
					' The following lines duplicate preceding lines in the file: </p>'
				)
			header_string = '<tr><th><p>Line#</p></th>'
			for field in self.fieldnames:
				header_string += '<th><p>' + str(field) + '</p></th>'
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
				self.record[item] = str(', '.join([str(i).encode() for i in self.record[item]]))


class SubmissionResult:
	def __init__(self, username, filename, submission_type, record_type):
		download_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], username)
		if not os.path.isdir(download_path):
			os.mkdir(download_path)
			gid = grp.getgrnam(app.config['CELERYGRPNAME']).gr_gid
			os.chown(download_path, -1, gid)
			os.chmod(download_path, app.config['IMPORT_FOLDER_PERMISSIONS'])
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
			"Feature",
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
		# todo rewrite property updates to happen during result consumption rather than storing in memory
		self.property_updates = {
			'custom_id': [],
			'assign_to_block': [],
			'assign_to_trees': [],
			'assign_to_samples': [],
			'tissue': [],
			'variety': {},
			'harvest_time': {}
		}

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
			if submission_item.record['Found']:
				self.resubmission_count += 1
				# Since sometimes a record for assign to will be submitted before the item is registered
				# To handle this, we collect all assign to submissions and set them whether found or not
				# TODO it might be worth considering scanning for records assigning to the item on item creation
				# but this would be very inefficient with trees and samples.
				if self.record_type == 'property':
					if submission_item.record['Feature'].lower() == 'assign to block':
						self.property_updates['assign_to_block'].append(
							[record['UID'], record['Value']]
						)
					if submission_item.record['Feature'].lower() == 'assign to trees':
						self.property_updates['assign_to_trees'].append(
							[record['UID'], record['Value']]
						)
					if submission_item.record['Feature'].lower() == 'assign to samples':
						self.property_updates['assign_to_samples'].append(
							[record['UID'], record['Value']]
						)
			else:
				self.submission_count += 1
				if self.record_type == 'property':
					if submission_item.record['Feature'].lower() == 'custom id':
						self.property_updates['custom_id'].append(
							[record['UID'], record['Value']]
						)
					if submission_item.record['Feature'].lower() == 'tissue':
						self.property_updates['tissue'].append(
							[record['UID'], record['Value']]
						)
					if submission_item.record['Feature'].lower() == 'assign to block':
						self.property_updates['assign_to_block'].append(
							[record['UID'], record['Value']]
						)
					if submission_item.record['Feature'].lower() == 'assign to trees':
						self.property_updates['assign_to_trees'].append(
							[record['UID'], record['Value']]
						)
					if submission_item.record['Feature'].lower() == 'assign to samples':
						self.property_updates['assign_to_samples'].append(
							[record['UID'], record['Value']]
						)
					if submission_item.record['Feature'].lower() == 'variety name':
						if not record['UID'] in self.property_updates['variety']:
							self.property_updates['variety'][record['UID']] = {}
						self.property_updates['variety'][record['UID']]['name'] = record['Value']
					if submission_item.record['Feature'].lower() == 'variety code':
						if not record['UID'] in self.property_updates['variety']:
							self.property_updates['variety'][record['UID']] = {}
						self.property_updates['variety'][record['UID']]['code'] = record['Value']
					if submission_item.record['Feature'].lower() == 'harvest date':
						if not record['UID'] in self.property_updates['harvest_time']:
							self.property_updates['harvest_time'][record['UID']] = {}
						self.property_updates['harvest_time'][record['UID']]['date'] = record['Value']
					if submission_item.record['Feature'].lower() == 'harvest time':
						if not record['UID'] in self.property_updates['harvest_time']:
							self.property_updates['harvest_time'][record['UID']] = {}
						self.property_updates['harvest_time'][record['UID']]['time'] = record['Value']

	def update_properties(self, tx):
		if self.property_updates['custom_id']:
			tx.run(
				' UNWIND $custom_ids AS uid_value '
				'	MATCH '
				'		(item: Item {uid: uid_value[0]}) '
				'	WHERE item.custom_id IS NULL '
				'	SET item.custom_id = uid_value[1] ',
				custom_ids=self.property_updates['custom_id']
			)
		if self.property_updates['tissue']:
			tx.run(
				' UNWIND $tissue AS uid_value '
				'	MATCH '
				'		(item: Sample {uid: uid_value[0]}) '
				'	WHERE item.tissue IS NULL '
				'	SET item.custom_id = uid_value[1] ',
				tissue=self.property_updates['tissue']
			)
		if self.property_updates['assign_to_block']:
			statement = (
				' UNWIND $assign_to_block AS uid_value '
				'	MATCH '
				'		(tree: Tree {uid: uid_value[0]}) '
				'		-[:IS_IN]->(: FieldTrees) '
				'		-[:IS_IN]->(: Field) '
				'		<-[:IS_IN]-(: FieldBlocks) '
				'		<-[:IS_IN]-(block: Block) '
				'	WHERE trim(block.name) = uid_value[1] '
				'	OPTIONAL MATCH (tree)-[:IS_IN]->(existing:BlockTrees) '
				'	WITH block, tree WHERE existing IS NULL '
				'	MERGE '
				'		(bt:BlockTrees)'
				'		-[:IS_IN]->(block) '
				'	MERGE '
				'		(bt) '
				'		<-[:FOR]-(c:Counter) '
				'		ON CREATE SET '
				'			c.count = 0, '
				'			c.name = "tree", '
				'			c.uid = (block.uid + "_tree") '
				'	SET c._LOCK_ = True '
				'	MERGE (tree)-[:IS_IN]->(bt) '
				'	SET c.count = c.count + 1 '
				'	REMOVE c._LOCK_ '
			)
			tx.run(statement, assign_to_block=self.property_updates['assign_to_block'])
		if self.property_updates['assign_to_trees']:
			tx.run(
				' UNWIND $assign_to_trees AS uid_value '
				'	MATCH '
				'		(sample: Sample {uid: uid_value[0]}) '
				'		-[from:FROM]->(: ItemSamples)'
				'		-[:FROM]->(: Field) '
				'		<-[:IS_IN]-(: FieldTrees) '
				'		<-[:IS_IN]-(tree: Tree) '
				'	WHERE tree.id IN extract(x in split(uid_value[1], ",") | toInteger(trim(x))) '
				'	MERGE '
				'		(tree)<-[:FROM]-(is:ItemSamples) '
				'	CREATE '
				'		(sample)-[:FROM]->(is) '
				'	DELETE from ',
				assign_to_trees=self.property_updates['assign_to_trees']
			)
		if self.property_updates['assign_to_samples']:
			tx.run(
				' UNWIND $assign_to_samples AS uid_value '
				'	MATCH '
				'		(sample: Sample {uid: uid_value[0]}) '
				'		-[from_field:FROM]->(: ItemSamples) '
				'		-[:FROM]->(field: Field) '
				'	MATCH '
				'		(field)'
				'		<-[: FROM | IS_IN *]-(source_sample: Sample) '
				'	WHERE source_sample.id IN extract(x in split(uid_value[1], ",") | toInteger(trim(x))) '
				# prevent self targeting
				'	AND sample.id <> source_sample.id '
				'	CREATE '
				'		(sample)-[from_sample: FROM]->(source_sample) '
				'	WITH sample, source_sample, from_field, from_sample '
				# prevent cycles 
				'	OPTIONAL MATCH cycle = (sample)-[: FROM *]->(sample) '
				'	FOREACH (n IN CASE WHEN cycle IS NULL THEN [1] ELSE [] END | '
				'		DELETE from_field '				
				'	) '
				'	FOREACH (n IN CASE WHEN cycle IS NOT NULL THEN [1] ELSE [] END | '
				'		DELETE from_sample '				
				'	) ',
				assign_to_samples=self.property_updates['assign_to_samples']
			)
		if self.property_updates['variety']:
			variety = [
				[
					str(key),
					value['name'] if 'name' in value else None,
					value['code'] if 'code' in value else None
				] for key, value in self.property_updates['variety'].iteritems()
			]
			statement = (
				' UNWIND $variety AS uid_name_code '
				'	WITH '
				'		CASE '
				'			WHEN size(split(uid_name_code[0], "_")) = 1 '
				'			THEN toInteger(uid_name_code[0]) '
				'			ELSE uid_name_code[0] '
				'			END as uid, '
				'		uid_name_code[1] as name, '
				'		uid_name_code[2] as code '
				'	MATCH '
				'		(item: Item {uid: uid}), '
				'		(field: Field {uid: '
				'			CASE '
				'			WHEN toInteger(uid) IS NOT NULL '
				'				THEN uid '
				'			ELSE '
				'				toInteger(split(uid, "_")[0]) '
				'			END '
				'		}), '
				'		(variety: Variety) '
				'		WHERE '
				'			variety.name_lower = toLower(trim(name)) '
				'			OR '
				'			variety.code = toLower(trim(code)) '
				'	OPTIONAL MATCH (item)-[:OF_VARIETY]->(existing:FieldVariety) '
				'	WITH '
				'		item, '
				'		field, '
				'		collect(variety)[0] as variety, '
				'		existing '
				'	WHERE existing IS NULL '
				'	MERGE '
				'		(field) '
				'		-[: CONTAINS_VARIETY]->(fv:FieldVariety) '
				'		-[: OF_VARIETY]->(variety) '
				'	CREATE '
				'		(item) '
				'		-[: OF_VARIETY]->(fv) '
			)
			tx.run(statement, variety=variety)
		if self.property_updates['harvest_time']:
			harvest_time = [
				[
					str(key),
					value['date'] if 'date' in value else None,
					value['time'] if 'time' in value else None
				] for key, value in self.property_updates['harvest_time'].iteritems()
			]
			statement = (
				' UNWIND $harvest_time AS uid_date_time '
				'	WITH '
				'		uid_date_time[0] as uid, '
				'		uid_date_time[1] as date, '
				'		uid_date_time[2] as time '
				'	MATCH '
				'		(item: Sample { '
				'			uid: uid '
				'		})-[:FROM]->(:ItemSamples) '
				# this match only finds primary samples, others inherit the harvest time from this node
				'	WHERE '
				'		item.time IS NULL  '
				'		AND '
				'		date IS NOT NULL '
				'	SET item.time = CASE '
				'		WHEN time IS NOT NULL '
				'		THEN apoc.date.parse(date + " " + time, "ms", "yyyy-MM-dd HH:mm") '
				'		ELSE apoc.date.parse(date + " 12:00", "ms", "yyyy-MM-dd HH:mm") '
				'		END '
			)
			tx.run(statement, harvest_time=harvest_time)


class Upload:
	def __init__(self, username, submission_type, raw_filename):
		self.username = username
		self.raw_filename = raw_filename
		self.filename = secure_filename(raw_filename)
		self.submission_type = submission_type
		self.file_path = os.path.join(app.instance_path, app.config['UPLOAD_FOLDER'], username, self.filename)
		self.record_types = []
		self.required_fieldnames = dict()
		self.trimmed_file_paths = dict()
		self.parse_results = dict()
		self.submission_results = dict()
		self.fieldnames = dict()
		self.file_extension = None
		self.contains_data = None
		self.features = dict()
		self.row_count = dict()
		self.error_messages = []

	def allowed_file(self):
		if '.' in self.raw_filename:
			self.file_extension = self.raw_filename.rsplit('.', 1)[1].lower()
			if self.file_extension in app.config['ALLOWED_EXTENSIONS']:
				return True
		else:
			return False

	def file_save(self, file_data):
		# create user upload path if not found
		upload_path = os.path.join(app.instance_path, app.config['UPLOAD_FOLDER'], self.username)
		if not os.path.isdir(upload_path):
			os.mkdir(upload_path)
			gid = grp.getgrnam(app.config['CELERYGRPNAME']).gr_gid
			os.chown(upload_path, -1, gid)
			os.chmod(upload_path, app.config['IMPORT_FOLDER_PERMISSIONS'])
		file_data.save(self.file_path)

	def file_format_errors(self):
		if self.file_extension == 'csv':
			with open(self.file_path) as uploaded_file:
				# TODO implement CSV kit checks - in particular csvstat to check field length (avoid stray quotes)
				# now get the dialect and check it conforms to expectations
				dialect = csv.Sniffer().sniff(uploaded_file.read())
				if not all((dialect.delimiter == ',', dialect.quotechar == '"')):
					return 'Please upload comma (,) separated file with quoted (") fields'
		elif self.file_extension == 'xlsx':
			try:
				wb = load_workbook(self.file_path, read_only=True)
			except BadZipfile:
				logging.info(
					'Bad zip file submitted: \n'
					+ 'username: ' + self.username
					+ 'filename: ' + self.file_path
				)
				return 'This file does not appear to be a valid xlsx file'
			if not set(app.config['WORKSHEET_NAMES'].values()) & set(wb.sheetnames):
				# Need to check for curve feature worksheets
				statement = (
					' MATCH '
					'	(feature: Feature)-[:OF_TYPE]->(:RecordType {name_lower:"curve"}) '
					' WHERE feature.name_lower IN $names '
					' RETURN feature.name '
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
						+ '<br>  - '.join([str(i) for i in app.config['WORKSHEET_NAMES'].values()])
						+ ' nor does it appear to contain a "curve" feature. '
					)
		else:
			return None

	def set_fieldnames(self):
		record_type_sets = {
			'mixed': {'uid'},
			'property': {'uid', 'person'},
			'trait': {'uid', 'person', 'date', 'time'},
			'condition': {'uid', 'person', 'start date', 'start time', 'end date', 'end time'},
			'curve': {'uid', 'person', 'date', 'time'},
		}
		if self.file_extension == 'csv':
			with open(self.file_path) as uploaded_file:
				file_dict = DictReaderInsensitive(uploaded_file)
				if self.submission_type == 'db':
					# 'Correct' submissions have mixed record types
					# these are uploads for correct
					# uid, replicate, feature (and for traits or properties the time (and replicate) or period respectively)
					# are required to identify the unique record
					# but to further confirm we only delete the intended record, e.g. if a record is later resubmitted
					# we include the check for submission time.
					# todo We need to check later for the types of records in the file
					# todo and then ensure the corresponding fields are present,
					# todo this can be done during a db_check as we iterate through the file
					# todo just y collecting the "Feature" field entries as a set
					# todo then finally check the types from this set.
					self.record_types = ['mixed']
					self.required_fieldnames = {'mixed': ['uid', 'feature', 'submitted at']}
					self.fieldnames = {'mixed': file_dict.fieldnames}
				elif self.submission_type == 'fb':
					# Field Book csv exports
					self.record_types = ['trait']
					self.required_fieldnames = {'trait': ['uid', 'trait', 'value', 'timestamp', 'person', 'location']}
					self.fieldnames = {'trait': file_dict.fieldnames}
				elif self.submission_type == 'table':
					# All record types will match the requirements for property
					# as such we check the length of the required list before updating,
					# and only update if longer than existing
					self.required_fieldnames = set()
					for record_type, required_set in record_type_sets:
						if record_type_sets[record_type].issubset(set(file_dict.fieldnames)):
							if len(required_set) > len(self.required_fieldnames):
								self.required_fieldnames = {record_type: required_set}
								self.record_types = [record_type]
								self.fieldnames = {record_type: file_dict.fieldnames}
							# The above will not disambiguate curves and traits
							# to separate we can check if all others are numbers
							if record_type in ['trait', 'curve']:
								other_fields = set(file_dict.fieldnames) - record_type_sets[record_type]
								all_numbers = True
								for field in other_fields:
									try:
										float(field)
									except ValueError:
										all_numbers = False
										break
								if all_numbers:
									self.required_fieldnames = {'curve': record_type_sets['curve']}
									self.record_types = ['curve']
									self.fieldnames = {'curve': file_dict.fieldnames}
								else:
									self.required_fieldnames = {'trait': record_type_sets['trait']}
									self.record_types = ['trait']
									self.fieldnames = {'trait': file_dict.fieldnames}
					if not self.fieldnames:
						return (
								'This table does not appear to contain a full set of required fieldnames: '
								+ ' <br> Property records require: ' + ', '.join([str(i) for i in record_type_sets['property']])
								+ ' <br> Trait records require: ' + ', '.join([str(i) for i in record_type_sets['trait']])
								+ ' <br> Condition records require: ' + ', '.join([str(i) for i in record_type_sets['condition']])
								+ ' <br> Trait records require: ' + ', '.join([str(i) for i in record_type_sets['trait']]) + ' and other column labels must be numbers.'
						)
				else:
					return 'Submission type not recognised'
		elif self.file_extension == 'xlsx':
			try:
				wb = load_workbook(self.file_path, read_only=True)
			except BadZipfile:
				logging.info(
					'Bad zip file submitted: \n'
					+ 'username: ' + self.username
					+ 'filename: ' + self.file_path
				)
				return 'This file does not appear to be a valid xlsx file'
			self.record_types = []
			sheetname_to_record_type = {v.lower(): k for k, v in app.config['WORKSHEET_NAMES'].iteritems()}
			for sheetname in wb.sheetnames:
				if sheetname.lower() in sheetname_to_record_type.keys():
					if sheetname_to_record_type[sheetname.lower()] in ['item_details', 'feature_details', 'hidden']:
						pass
					else:
						record_type = sheetname_to_record_type[sheetname.lower()]
						self.record_types.append(record_type)
						ws = wb[sheetname]
						rows = ws.iter_rows(min_row=1, max_row=1)
						first_row = next(rows)
						self.fieldnames[record_type] = [str(c.value).encode().strip().lower() for c in first_row if c.value]
						self.required_fieldnames[record_type] = record_type_sets[record_type]
				else:
					statement = (
						' MATCH '
						'	(feature: Feature {name_lower: $name})-[:OF_TYPE]->(:RecordType {name_lower:"curve"}) '
						' RETURN feature.name '
					)
					parameters = {
						'name': sheetname.lower()
					}
					with get_driver().session() as neo4j_session:
						result = neo4j_session.read_transaction(
							bolt_result,
							statement,
							parameters
						)
					if result.peek():
						self.features[sheetname] = result.single()[0]
						self.record_types.append('curve')
						ws = wb[sheetname]
						rows = ws.iter_rows(min_row=1, max_row=1)
						first_row = next(rows)
						self.fieldnames[sheetname] = [
							str(c.value).encode().strip().lower() for c in first_row if	c.value
						]
						self.required_fieldnames[sheetname] = record_type_sets['curve']
					else:
						return (
							'This workbook does not appear to contain any of the following accepted worksheets: <br> - '
							+ '<br>  - '.join(
								[
									str(i) for i in app.config['WORKSHEET_NAMES'].values() if i not in app.config['REFERENCE_WORKSHEETS']
								]
							) + '<br> Nor does it appear to contain a "curve" feature. '
						)
			if not self.fieldnames:
				return (
						'This workbook does not appear to contain any of the following accepted worksheets: '
						+ '<br>  - '.join([str(i) for i in app.config['WORKSHEET_NAMES'].values()])
					)
		else:
			return 'Only csv and xlsx file formats are supported for data submissions'

	def check_headers(self, tx):
		errors = []
		for worksheet in self.fieldnames.keys():
			fieldnames_set = set(self.fieldnames[worksheet])
			if len(self.fieldnames[worksheet]) > len(fieldnames_set):
				if self.file_extension == 'xlsx':
					if worksheet in app.config['WORKSHEET_NAMES']:
						error_message = '<p>' + app.config['WORKSHEET_NAMES'][worksheet] + ' worksheet '
					else:
						error_message = '<p>' + worksheet + ' worksheet '
				else:
					error_message = '<p>This file '
				errors.append(error_message + 'contains duplicate column labels. This is not supported.</p>')
			if not set(self.required_fieldnames[worksheet]).issubset(fieldnames_set):
				missing_fieldnames = set(self.required_fieldnames[worksheet]) - fieldnames_set
				if self.file_extension == 'xlsx':
					if worksheet in app.config['WORKSHEET_NAMES']:
						error_message = '<p>' + app.config['WORKSHEET_NAMES'][worksheet] + ' worksheet '
					else:
						error_message = '<p>' + worksheet + ' worksheet '
				else:
					error_message = '<p>This file '
				error_message += (
						' is missing the following required fields: '
						+ ', '.join([i for i in missing_fieldnames])
						+ '</p>'
				)
				errors.append(error_message)
			if self.submission_type == 'table':
				# now we strip back the fieldnames to keep only those that aren't in the required list
				# these should now just be the features which we confirm are found in the db.
				# except for curves, where they should all be numbers
				if worksheet in app.config['WORKSHEET_NAMES']:
					self.features[worksheet] = [
						i for i in self.fieldnames[worksheet] if i not in self.required_fieldnames[worksheet]
					]
					record_type = worksheet
					statement = (
						' UNWIND $fields AS field '
						'	OPTIONAL MATCH '
						'		(feature:Feature {name_lower: toLower(trim(toString(field)))}) '
						'	OPTIONAL MATCH '
						'		(feature)-[:OF_TYPE]->(record_type: RecordType) '
						'	WITH '
						'		field, record_type '
						'	WHERE '
						'		feature IS NULL '
						'		OR '
						'		record_type.name_lower <> $record_type '
						'	RETURN '
						'		field, record_type.name_lower '
					)
					field_errors = tx.run(
						statement,
						record_type=record_type,
						fields=self.features[worksheet]
					)
					if field_errors.peek():
						if self.file_extension == 'xlsx':
							error_message = '<p>' + app.config['WORKSHEET_NAMES'][record_type] + ' worksheet '
						else:
							error_message = '<p>This file '
						error_message += (
							'contains column headers that are not recognised as features or required details: </p>'
						)
						for field in field_errors:
							error_message += '<dt>' + field[0] + ':</dt> '
							feature_record_type = field[1]
							if feature_record_type:
								error_message += (
									' <dd> Required fields present for ' + record_type + ' records but'							
									' this feature is a ' + feature_record_type + '.'
								)
								if feature_record_type == 'condition':
									error_message += (
										'. Condition records require "start date", "start time", "end date" and "end time" '
										' in addition to the "UID" and "Person" fields.'
									)
								elif feature_record_type == 'trait':
									error_message += (
										'. Trait records require "date" and "time" fields'
										' in addition to the "UID" and "Person" fields.'
									)
								elif feature_record_type == 'property':
									error_message += '. Property records require the "UID" and "Person" fields.'
								error_message += (
										'</dd>\n'
								)
							else:
								error_message += '<dd>Unrecognised feature. Please check your spelling.</dd>\n'
						errors.append(error_message)
				else:
					other_fields = fieldnames_set - self.required_fieldnames[worksheet]
					all_numbers = True
					for field in other_fields:
						try:
							float(field)
						except ValueError:
							all_numbers = False
							break
					if not all_numbers:
						if self.file_extension == 'xlsx':
							error_message = '<p>' + worksheet + ' worksheet '
						else:
							error_message = '<p>This file '
						error_message += (
							'contains unexpected column labels.'
							' For curve data only float values are accepted in addition to required labels: '
						)
						for i in self.required_fieldnames['curve']:
							error_message += '<dd>' + str(i) + '</dd>'
						errors.append(error_message)
		header_report = '<br>'.join(errors)
		return header_report

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
			wb = load_workbook(self.file_path, read_only=True)
			if self.submission_type == 'db':
				ws = wb['Records']
			elif self.submission_type == 'table':
				if worksheet in app.config['WORKSHEET_NAMES']:
					ws = wb[app.config['WORKSHEET_NAMES'][worksheet]]
				else:
					ws = wb[worksheet]
			with open(trimmed_file_path, "wb") as trimmed_file:
				file_writer = csv.writer(
					trimmed_file,
					quoting=csv.QUOTE_ALL
				)
				rows = ws.iter_rows()
				first_row = next(rows)
				file_writer.writerow(['row_index'] + [str(cell.value).encode().lower() for cell in first_row if cell.value])
				# handle deleted columns in the middle of the worksheet
				empty_headers = []
				for i, cell in enumerate(first_row):
					if not cell.value:
						empty_headers.append(i)
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
							# when importing 00:00 time from excel, which uses 1900 date system
							# we get a datetime.datetime object of 30th December 1899 for the time field
							# we have to catch this here. If the date/time is an exact match, we just render it as a time
							# it will be picked up in the subsequent scan of the trimmed file
							# although won't make a lot of sense to the user...
							if cell_values[i] == datetime.datetime(1899, 12, 30, 0, 0):
								cell_values[i] = "00:00"
							else:
								try:
									cell_values[i] = value.strftime("%Y-%m-%d")
								except ValueError:
									cell_values[i] = 'Dates before 1900 not supported'
						elif isinstance(value, datetime.time):
							cell_values[i] = value.strftime("%H:%M")
						else:
							if isinstance(value, basestring):
								cell_values[i] = value.strip()
					if any(value for value in cell_values):
						cell_values = [self.row_count[worksheet]] + cell_values
						file_writer.writerow(cell_values)

	def parse_rows(self, worksheet):
		trimmed_file_path = self.trimmed_file_paths[worksheet]
		submission_type = self.submission_type
		parse_result = ParseResult(submission_type, worksheet, self.fieldnames[worksheet])
		self.parse_results[worksheet] = parse_result
		if worksheet in app.config['WORKSHEET_NAMES']:
			record_type = worksheet
		else:
			record_type = 'curve'
			x_values = set(self.fieldnames[worksheet]) - self.required_fieldnames[worksheet]
		with open(trimmed_file_path, 'r') as trimmed_file:
			trimmed_dict = DictReaderInsensitive(trimmed_file)
			for row in trimmed_dict:
				if submission_type == 'table':
					if record_type != 'curve':
						# first check for feature data, if none then just skip this row
						if [row[feature] for feature in self.features[worksheet] if row[feature]]:
							parse_result.contains_data = True
							parse_result.parse_table_row(row)
					else:
						if [row[x] for x in x_values if row[x]]:
							parse_result.contains_data = True
							parse_result.parse_table_row(row)
				elif submission_type == 'db':
					parse_result.parse_db_row(row)
				elif submission_type == "fb":
					parse_result.parse_fb_row(row)
		if parse_result.contains_data:
			self.contains_data = True
		if parse_result.duplicate_keys:
			if self.file_extension != 'xlsx':
				self.error_messages.append(
					'<p> This file contains duplicate keys: </p>' +
					parse_result.duplicate_keys_table()
				)
			else:
				if worksheet in app.config['WORKSHEET_NAMES']:
					self.error_messages.append(
						'<p>' + app.config['WORKSHEET_NAMES'][worksheet]
						+ 'worksheet contains duplicate keys:</p>'
						+ parse_result.duplicate_keys_table()
					)
				else:
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
		# todo so we could remove this check for features from  here to simplify
		username = self.username
		if worksheet in app.config['WORKSHEET_NAMES']:
			record_type = worksheet
		else:
			record_type = 'curve'
		trimmed_file_path = self.trimmed_file_paths[worksheet]
		trimmed_filename = os.path.basename(trimmed_file_path)
		submission_type = self.submission_type
		parse_result = self.parse_results[worksheet]
		with open(trimmed_file_path, 'r') as trimmed_file:
			if submission_type == 'db':
				trimmed_dict_reader = DictReaderInsensitive(trimmed_file)
				features_set = set()
				# todo move this to the parse procedure where we iterate through the file already
				for row in trimmed_dict_reader:
					features_set.add(row['feature'].lower())
				record_types = tx.run(
					' UNWIND $features as feature_name'
					'	MATCH '
					'	(f:Feature { '
					'		name_lower: feature_name'
					'	})-[:OF_TYPE]->(record_type: RecordType) '
					' RETURN distinct(record_type.name_lower) ',
					features=list(features_set)
				).value()
				# traits require additional time and replicate
				if {'trait', 'curve'}.intersection(record_types):
					self.required_fieldnames['mixed'].update([
						'time',
						'replicate'
					])
				# conditions require period
				if 'condition' in record_types:
					self.required_fieldnames['mixed'].add(
						'period'
					)
				if not set(self.required_fieldnames[worksheet]).issubset(set(self.fieldnames[worksheet])):
					missing_fieldnames = set(self.required_fieldnames[worksheet]) - set(self.fieldnames[worksheet])
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
			elif submission_type == "fb":
				check_result = tx.run(
					Cypher.upload_fb_check,
					username=username,
					filename=("file:///" + username + '/' + trimmed_filename),
					submission_type=submission_type
				)
				trimmed_dict_reader = DictReaderInsensitive(trimmed_file)
				row = trimmed_dict_reader.next()
				for item in check_result:
					record = item[0]
					while record['row_index'] != int(row['row_index']):
						row = trimmed_dict_reader.next()
					if not record['UID']:
						parse_result.merge_error(
							row,
							"uid",
							"missing"
						)
					if not record['Feature']:
						parse_result.merge_error(
							row,
							"feature",
							"missing"
						)
					if all([
						record['UID'],
						record['Feature'],
						not record['Value']
					]):
						parse_result.merge_error(
							row,
							"value",
							"format",
							feature_name=record['Feature'],
							feature_format=record['Format'],
							category_list=record['Category list']
						)
					# need to check an element of the list as all results
					if record['Conflicts'][0]['existing_value']:
						parse_result.merge_error(
							row,
							"value",
							"conflict",
							conflicts=record['conflicts']
						)
			elif submission_type == 'table':
				if record_type == 'property':
					statement = Cypher.upload_table_property_check
					parameters = {
						'username': username,
						'filename': urls.url_fix('file:///' + username + '/' + trimmed_filename ),
						'features': self.features[worksheet],
						'record_type': record_type
					}
				elif record_type == 'trait':
					statement = Cypher.upload_table_trait_check
					parameters = {
						'username': username,
						'filename': urls.url_fix('file:///' + username + '/' + trimmed_filename),
						'features': self.features[worksheet],
						'record_type': record_type
					}
				elif record_type == 'condition':
					statement = Cypher.upload_table_condition_check
					parameters = {
						'username': username,
						'filename': urls.url_fix('file:///' + username + '/' + trimmed_filename),
						'features': self.features[worksheet],
						'record_type': record_type
					}
				elif record_type == 'curve':
					statement = Cypher.upload_table_curve_check
					parameters = {
						'username': username,
						'filename': urls.url_fix('file:///' + username + '/' + trimmed_filename),
						'feature_name': self.features[worksheet],
						'x_values': sorted(
							[float(i) for i in self.fieldnames[worksheet] if i not in self.required_fieldnames[worksheet]]
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
				row = trimmed_dict_reader.next()
				# need to check_result sorted by file/dictreader row_index
				for item in check_result:
					record = item[0]
					while record['row_index'] != int(row['row_index']):
						row = trimmed_dict_reader.next()
					if not record['UID']:
						parse_result.merge_error(
							row,
							"uid",
							"missing"
						)
					if not record['Feature']:
						parse_result.add_field_error(
							record['Input feature'],
							(
								"This feature is not found. Please check your spelling. "
								"This may also be because the feature is not available at the level of these items"
							)
						)
					# we add found fields to a list to handle mixed items in input
					# i.e. if found at level of one item but not another
					else:
						parse_result.add_field_found(record['Feature'])
					if all([
						record['UID'],
						record['Feature'],
						not record['Value']
					]):
						parse_result.merge_error(
							row,
							record['Input feature'],
							"format",
							feature_name=record['Feature'],
							feature_format=record['Format'],
							category_list=record['Category list']
						)
					# need to check an element of the list as all results
					if record['Conflicts'][0]['existing_value']:
						parse_result.merge_error(
							row,
							record['Input feature'],
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
		if worksheet in app.config['WORKSHEET_NAMES']:
			record_type = worksheet
		else:
			record_type = 'curve'
		trimmed_file_path = self.trimmed_file_paths[worksheet]
		trimmed_filename = os.path.basename(trimmed_file_path)
		submission_type = self.submission_type
		filename = self.filename
		features = self.features[worksheet]
		with contextlib.closing(SubmissionResult(username, filename, submission_type, worksheet)) as submission_result:
			self.submission_results[worksheet] = submission_result
			if submission_type == 'fb':
				statement = Cypher.upload_fb
				result = tx.run(
					statement,
					username=username,
					filename=("file:///" + username + '/' + trimmed_filename)
				)
			else:  # submission_type == 'table':
				if record_type == 'property':
					statement = Cypher.upload_table_property
					result = tx.run(
						statement,
						username=username,
						filename=urls.url_fix("file:///" + username + '/' + trimmed_filename),
						features=features,
						record_type=record_type
					)
				elif record_type == 'trait':
					statement = Cypher.upload_table_trait
					result = tx.run(
						statement,
						username=username,
						filename=urls.url_fix("file:///" + username + '/' + trimmed_filename),
						features=features,
						record_type=record_type
					)
				elif record_type == 'condition':
					statement = Cypher.upload_table_condition
					result = tx.run(
						statement,
						username=username,
						filename=urls.url_fix("file:///" + username + '/' + trimmed_filename),
						features=features,
						record_type=record_type
					)
				elif record_type == 'curve':
					statement = Cypher.upload_table_curve
					result = tx.run(
						statement,
						username=username,
						filename=urls.url_fix("file:///" + username + '/' + trimmed_filename),
						feature_name=self.features[worksheet],
						x_values=sorted(
							[float(i) for i in self.fieldnames[worksheet] if i not in self.required_fieldnames[worksheet]]
						),
						record_type=record_type
					)
				else:
					logging.warn('Record type not recognised')
			# create a submission result and update properties from result
			for record in result:
				submission_result.parse_record(record[0])

	@celery.task(bind=True)
	def async_submit(self, username, upload_object):
		try:
			fieldname_errors = upload_object.set_fieldnames()
			if fieldname_errors:
				with app.app_context():
					html = render_template(
						'emails/upload_report.html',
						response=fieldname_errors
					)
					subject = "BreedCAFS upload rejected"
					recipients = [User(username).find('')['email']]
					response = "Submission rejected due to invalid file:\n " + fieldname_errors
					body = response
					send_email(subject, app.config['ADMINS'][0], recipients, body, html)
				return {
					'status': 'ERRORS',
					'result': fieldname_errors
				}
			with get_driver().session() as neo4j_session:
				header_report = neo4j_session.read_transaction(
					upload_object.check_headers
				)
				if header_report:
					with app.app_context():
						html = render_template(
							'emails/upload_report.html',
							response=header_report
						)
						subject = "BreedCAFS upload rejected"
						recipients = [User(username).find('')['email']]
						response = "Submission rejected due to unrecognised or missing fields:\n " + header_report
						body = response
						send_email(subject, app.config['ADMINS'][0], recipients, body, html)
					return {
						'status': 'ERRORS',
						'result': header_report
					}
				if upload_object.file_extension in ['csv', 'xlsx']:
					with neo4j_session.begin_transaction() as tx:
						for worksheet in upload_object.fieldnames.keys():
							# todo Stop trimming before parsing, this should be done in one pass of the file
							# clean up the file removing empty lines and whitespace, lower case headers for matching in db
							upload_object.trim_file(worksheet)
							# parse the trimmed file/worksheet for errors
							# also adds parse_result to upload_object.parse_result dict (by record_type)
							upload_object.parse_rows(worksheet)
							# with string parsing performed, now we check against the database for UID, feature, value
							upload_object.db_check(tx, worksheet)
						if not upload_object.contains_data:
							upload_object.error_messages.append(
								'The uploaded file ('
								+ upload_object.raw_filename
								+ ') appears to contain no input values. '
							)
						if upload_object.error_messages:
							error_messages = '<br>'.join(upload_object.error_messages)
							with app.app_context():
								html = render_template(
									'emails/upload_report.html',
									response=error_messages
								)
								subject = "BreedCAFS upload rejected"
								recipients = [User(username).find('')['email']]
								body = error_messages
								send_email(subject, app.config['ADMINS'][0], recipients, body, html)
							return {
								'status': 'ERRORS',
								'result': error_messages
							}
						conflict_files = []
						submissions = []
						for worksheet in upload_object.fieldnames.keys():
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
									response=response
								)
								# send result of merger in an email
								subject = "BreedCAFS upload rejected"
								recipients = [User(username).find('')['email']]
								body = response
								send_email(subject, app.config['ADMINS'][0], recipients, body, html)
								return {
									'status': 'ERRORS',
									'result': response
								}
						# update properties where needed,
						# todo should probably be handled when consuming the result rather than storing
						# todo :and later updating, consider why I did it this way, likely just an artifact
						# todo :of my early failure to consume results properly.
						if 'property' in upload_object.record_types:
							upload_object.submission_results['property'].update_properties(tx)
						tx.commit()
						# now need app context for the following (this is running asynchronously)
						with app.app_context():
							if not submissions:
								response = 'No data submitted, please check that you uploaded a completed file'
								return {
									'status': 'ERRORS',
									'result': response
								}
							else:
								response = "<p> Submission report:\n "
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
									response=response
								)
								# send result of merger in an email
								subject = "BreedCAFS upload summary"
								recipients = [User(username).find('')['email']]
								body = response
								send_email(subject, app.config['ADMINS'][0], recipients, body, html)
								return {
									'status': 'SUCCESS',
									'result': response
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
			'	toLower(csvLine.feature) as feature_name, '
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
			'	-[:SUBMITTED]->(uff:UserFieldFeature) '
			'	-[submitted:SUBMITTED]->(record :Record) '
			'	-[record_for:RECORD_FOR]->(if:ItemFeature) '
			'	-[:FOR_ITEM]-(item:Item {'
			'		uid: uid'
			'	}), '
			'	(if)'
			'	-[FOR_FEATURE*..2]->(feature:Feature {'
			'		name_lower:feature_name'
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
			'		`deleted at`: timestamp() '
			'	}]->(record) '
			' CREATE '
			'	(record)-[:DELETED_FROM]->(if) '
			' DELETE '
			'	record_for '
			' RETURN { '
			'	time: time, '
			'	record_time: record.time, '
			'	uid: uid, '
			'	feature: feature.name_lower, '
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
		if property_uid['custom id']:
			tx.run(
				' UNWIND $uid_list as uid'
				' MATCH '
				'	(item: Item {uid: uid}) '
				' REMOVE item.custom_id ',
				uid_list=property_uid['custom id']
			)
		if property_uid['tissue']:
			tx.run(
				' UNWIND $uid_list as uid'
				' MATCH '
				'	(sample: Sample {uid: uid}) '
				' REMOVE sample.tissue ',
				uid_list=property_uid['tissue']
			)
		if property_uid['assign to block']:
			tx.run(
				' UNWIND $uid_list as uid '
				' MATCH '
				'	(tree: Tree {uid: uid}) '
				'	-[is_in:IS_IN]->(bt:BlockTrees) '
				'	<-[:FOR]-(c:Counter), '
				'	(bt)-[:IS_IN]->(block:Block) '
				' SET c._LOCK_ = True '
				' DELETE is_in '
				' SET c.count = c.count - 1 '
				' REMOVE c._LOCK_ ',
				uid_list=property_uid['assign to block']
			)
		if property_uid['assign to trees']:
			tx.run(
				' UNWIND $uid_list as uid '
				' MATCH '
				'	(sample: Sample {'
				'		uid: uid'
				'	})-[from_tree: FROM]->(: ItemSamples) '
				'	-[: FROM]-(tree: Tree) '
				'	-[: IS_IN]->(: FieldTrees) '
				'	-[: IS_IN]->(field: Field) '
				' DELETE from_tree '
				' MERGE '
				'	(fs: ItemSamples) '
				'	-[:FROM]->(field) '
				' MERGE '
				'	(sample)-[:FROM]->(fs) ',
				uid_list=property_uid['assign to trees']
			)
		if property_uid['assign to samples']:
			tx.run(
				' UNWIND $uid_list as uid '
				' MATCH '
				'	(sample: Sample {'
				'		uid: uid'
				'	})-[from_sample: FROM]->(: Sample) '
				'	-[:FROM | IS_IN *]->(field: Field) '
				' DELETE from_sample '
				' MERGE '
				'	(fs: ItemSamples) '
				'	-[:FROM]->(field) '
				' MERGE '
				'	(sample)-[:FROM]->(fs) ',
				uid_list=property_uid['assign to samples']
			)
		if property_uid['variety name']:
			tx.run(
				' UNWIND $uid_list as uid '
				' MATCH '
				'	(item: Item { '
				'		uid: uid '
				'	})-[of_variety:OF_VARIETY]->(fv:FieldVariety) '
				'	-[contains_variety:CONTAINS_VARIETY]->(field:Field) '
				' DELETE of_variety '
				' WITH '
				' item, fv, field, contains_variety '
				' OPTIONAL MATCH '
				' (:Item)-[of_variety:OF_VARIETY]->(fv) '
				' FOREACH (n IN CASE WHEN of_variety IS NULL THEN [1] ELSE [] END | '
				'	DELETE contains_variety '
				' ) '	
				' WITH item, field '
				' MATCH '
				'	(item) '
				'	<-[:FOR_ITEM]-(if:ItemFeature) '
				'	-[:FOR_FEATURE*..2]->(:Feature { '
				'		name_lower: "variety code"'
				'	}), '
				'	(if)<-[:RECORD_FOR]-(record:Record) '
				' MATCH '
				'	(variety:Variety {code_lower: toLower(record.value)}) '
				' MERGE '
				'	(field) '
				'	<-[:CONTAINS_VARIETY]-(fv:FieldVariety) '
				' MERGE '
				'	(item) '
				'	-[:OF_VARIETY]->(fv) ',
				uid_list=property_uid['variety name']
			)
		if property_uid['variety code']:
			tx.run(
				' UNWIND $uid_list as uid '
				' MATCH '
				'	(item: Item { '
				'		uid: uid '
				'	})-[of_variety:OF_VARIETY]->(fv:FieldVariety) '
				'	-[contains_variety:CONTAINS_VARIETY]->(field:Field) '
				' DELETE of_variety '
				' WITH '
				' item, fv, field, contains_variety '
				' OPTIONAL MATCH '
				' (:Item)-[of_variety:OF_VARIETY]->(fv) '
				' FOREACH (n IN CASE WHEN of_variety IS NULL THEN [1] ELSE [] END | '
				'	DELETE contains_variety '
				' ) '	
				' WITH item, field '
				' MATCH '
				'	(item) '
				'	<-[:FOR_ITEM]-(if:ItemFeature) '
				'	-[:FOR_FEATURE*..2]->(:Feature { '
				'		name_lower: "variety name"'
				'	}), '
				'	(if)<-[:RECORD_FOR]-(record:Record) '
				' MATCH '
				'	(variety:Variety {name_lower: toLower(record.value)}) '
				' MERGE '
				'	(field) '
				'	<-[:CONTAINS_VARIETY]-(fv:FieldVariety) '
				' MERGE '
				'	(item) '
				'	-[:OF_VARIETY]->(fv) ',
				uid_list=property_uid['variety code']
			)
		if property_uid['harvest date']:
			tx.run(
				' UNWIND $uid_list as uid '
				' MATCH (item: Item { '
				'	uid: uid '
				' }) '
				' REMOVE item.time ',
				uid_list=property_uid['harvest date']
			)
		if property_uid['harvest time']:
			tx.run(
				' UNWIND $uid_list as uid '
				' MATCH (item: Item { '
				'	uid: uid '
				' }) '
				' WHERE item.time IS NOT NULL '
				' WITH '
				'	item, '
				# round time to the date then set to 12:00
				'	datetime.truncate('
				'		"day", '
				'		date( '
				'			datetime({epochmillis: item.time})'
				'		)'
				'	) + duration({ hours: 12 }) as time '
				' SET item.time = time.epochMillis ',
				uid_list=property_uid['harvest date']
			)

	@celery.task(bind=True)
	def async_correct(self, username, access, upload_object):
		try:
			fieldname_errors = upload_object.set_fieldnames()
			if fieldname_errors:
				with app.app_context():
					html = render_template(
						'emails/upload_report.html',
						response=fieldname_errors
					)
					subject = "BreedCAFS upload rejected"
					recipients = [User(username).find('')['email']]
					response = "Submission rejected due to invalid file:\n " + fieldname_errors
					body = response
					send_email(subject, app.config['ADMINS'][0], recipients, body, html)
				return {
					'status': 'ERRORS',
					'result': fieldname_errors
				}
			with get_driver().session() as neo4j_session:
				header_report = neo4j_session.read_transaction(
					upload_object.check_headers
				)
				if header_report:
					with app.app_context():
						response = header_report
						html = render_template(
							'emails/upload_report.html',
							response=response
						)
						subject = "BreedCAFS correction rejected"
						recipients = [User(username).find('')['email']]
						response = "Correction rejected due to unrecognised or missing fields.\n "
						body = response
						send_email(subject, app.config['ADMINS'][0], recipients, body, html)
					return {
						'status': 'ERRORS',
						'result': header_report
					}
				with neo4j_session.begin_transaction() as tx:
					# there should only be one record type here, "mixed", so no need to iterate
					record_type = upload_object.record_types[0]
					# todo Stop trimming before parsing, this should be done in one pass of the file
					upload_object.trim_file(record_type)
					if not upload_object.row_count[record_type]:
						upload_object.error_messages.append('No records found to delete')
					upload_object.parse_rows(record_type)
					#if upload_object.parse_results[record_type].errors:
					#	upload_object.error_messages.append(
					#		upload_object.parse_results[record_type].html_table()
					#	)
					upload_object.db_check(tx, record_type)
					if upload_object.error_messages:
						error_messages = '<br>'.join(upload_object.error_messages)
						with app.app_context():
							html = render_template(
								'emails/upload_report.html',
								response=error_messages
							)
							subject = "BreedCAFS correction rejected"
							recipients = [User(username).find('')['email']]
							body = error_messages
							send_email(subject, app.config['ADMINS'][0], recipients, body, html)
						return {
							'status': 'ERRORS',
							'result': error_messages
						}
					deletion_result = upload_object.correct(tx, access, record_type)
					# update properties where needed
					property_uid = {
						'custom id': [],
						'assign to block': [],
						'assign to trees': [],
						'assign to samples': [],
						'tissue': [],
						'variety name': [],
						'variety code': [],
						'harvest date': [],
						'harvest time': []
					}
					# just grab the UID where records are deleted and do the update from any remaining records
					record_tally = {}
					missing_row_indexes = []
					expected_row_index = 1
					if not deletion_result.peek():
						missing_row_indexes += range(2, upload_object.row_count[record_type] + 2)
					else:
						for record in deletion_result:
							while (
									expected_row_index != record[0]['row_index']
									and expected_row_index <= upload_object.row_count[record_type] + 2
							):
								expected_row_index += 1
								if expected_row_index != record[0]['row_index']:
									missing_row_indexes.append(expected_row_index)
							if not record[0]['feature'] in record_tally:
								record_tally[record[0]['feature']] = 0
							record_tally[record[0]['feature']] += 1
							if record[0]['feature'] in property_uid:
								property_uid[record[0]['feature']].append(record[0]['uid'])
						upload_object.remove_properties(tx, property_uid)
						if not expected_row_index == upload_object.row_count[record_type]:
							missing_row_indexes += range(expected_row_index, upload_object.row_count[record_type] + 2)
					if missing_row_indexes:
						missing_row_ranges = (
							list(x) for _, x in itertools.groupby(enumerate(missing_row_indexes), lambda (i, x): i-x)
						)
						missing_row_str = str(
							",".join("-".join(map(str, (i[0][1], i[-1][1])[:len(i)])) for i in missing_row_ranges)
						)
						tx.rollback()
						with app.app_context():
							# send result of merger in an email
							subject = 'BreedCAFS correction rejected'
							recipients = [User(username).find('')['email']]
							response = 'Correction rejected:\n '
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
						response = 'Correction report:\n '
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
			app.instance_path,
			app.config['UPLOAD_FOLDER'],
			self.username
		)
		if not os.path.isdir(user_upload_dir):
			os.mkdir(user_upload_dir)
			# gid = grp.getgrnam(app.config['CELERYGRPNAME']).gr_gid
			# os.chown(user_upload_dir, -1, gid)
			os.chmod(user_upload_dir, app.config['IMPORT_FOLDER_PERMISSIONS'])
		self.temp_dir = os.path.join(
			app.instance_path,
			app.config['UPLOAD_FOLDER'],
			self.username,
			self.resumable_id
		)
		if not os.path.isdir(self.temp_dir):
			os.mkdir(self.temp_dir)
			# gid = grp.getgrnam(app.config['CELERYGRPNAME']).gr_gid
			# os.chown(self.temp_dir, -1, gid)
			os.chmod(self.temp_dir, app.config['IMPORT_FOLDER_PERMISSIONS'])
		self.filename = secure_filename(raw_filename)
		self.file_path = os.path.join(
			app.instance_path,
			app.config['UPLOAD_FOLDER'],
			self.username,
			self.filename
		)

	@staticmethod
	def allowed_file(raw_filename):
		if '.' in raw_filename:
			file_extension = raw_filename.rsplit('.', 1)[1].lower()
			if file_extension in  app.config['ALLOWED_EXTENSIONS']:
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
