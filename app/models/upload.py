from app import app, os, celery, ServiceUnavailable, SecurityError
import grp
from app.cypher import Cypher
from app.emails import send_email
from app.models.parsers import Parsers
from flask import render_template, url_for
from werkzeug import urls
from user import User
from config import ALLOWED_EXTENSIONS
from neo4j_driver import get_driver
import unicodecsv as csv
import datetime
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
		self.errors = {}
		self.error_comments = {
			"timestamp": {
				"format": "Timestamp doesn't match Field Book generated pattern (e.g. 2018-01-01 13:00:00+0100)"
			},
			"date": {
				"format": "Date format does not match the required input (e.g. 2018-01-01)"
			},
			"time": {
				"format": "Time format does not match the required input (e.g. 13:00)"
			},
			"uid": {
				"format": "UID doesn't match BreedCAFS pattern: \n"
				"  - Field UID should be an integers (e.g. '1')\n"
				"  - Block UID should include the Field and Block ID separated by '_B' (e.g. '1_B1')\n"
				"  - Tree UID should include the Field and Tree ID separated by '_T' (e.g. '1_T1')\n"
				"  - Branch UID should include the Field and Branch ID separated by '_Y' (e.g. '1_Y1')\n"
				"  - Leaf UID should include the Field and Leaf ID separated by '_L' (e.g. '1_L1')\n"
				"  - Sample UID should include the Field and Sample ID separated by '_S' (e.g. '1_S1')\n",
				"missing": "This UID is not found in the database. "
			},
			"feature": {
				"missing": "This feature is not found in the database. "
				"Please check the spelling and "
				"that this feature is found among those supported by BreedCAFS "
				"for the level of data you are submitting."
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
				}
			}
		}

	def add_error(
			self,
			field,
			error_type,
			# optional arguments only relevant to value errors
			feature_name=None,
			feature_format=None,
			category_list=None
	):
		if field not in self.errors:
			self.errors[field] = {
					'error_type': error_type,
					'feature_name': feature_name,
					'feature_format': feature_format,
					'category_list': category_list
				}
		else:
			pass

	def headers(self):
		return self.row.keys()

	def html_row(self, fieldnames):
		row_string = '<tr><td>' + str(self.row['row_index']) + '</td>'
		for field in fieldnames:
			if field not in self.errors:
				row_string += '<td>' + self.row[field] + '</td>'
			else:
				row_string += '<td bgcolor = #FFFF00 title = "'
				field_error_type = self.errors[field]['error_type']
				field_feature_name = self.errors[field]['feature_name']
				field_feature_format = self.errors[field]['feature_format']
				field_category_list = self.errors[field]['category_list']
				# if it is a simple error (time format, UID format or UID/Feature not found)
				if field in self.error_comments:
					row_string += self.error_comments[field][field_error_type]
				else:
					row_string += self.error_comments['other'][field_error_type][field_feature_format]
					if field_feature_name == 'variety name':
						row_string += 'Expected one of the following variety names: \n'
					elif field_feature_name == 'variety code':
						row_string += 'Expected one of the following codes: \n'
					elif field_feature_name == 'fertiliser n:p:k ratio':
						row_string += 'Expected N:P:K ratio format, e.g. 1:1:1'
					elif 'time' in field_feature_name:
						row_string += 'Expected time format as HH:MM e.g. 13:01'
					elif 'assign to' in field_feature_name:
						row_string += (
							'Expected a comma separated list of integers corresponding to the ID within the field '
						)
					if field_category_list:
						row_string += ", ".join([i for i in field_category_list])
				row_string += '">' + str(self.row[field]) + '</td>'
		return row_string


class ParseResult:
	def __init__(self, submission_type, fieldnames):
		self.submission_type = submission_type
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

	def parse_row(self, row):
		submission_type = self.submission_type
		# check uid formatting
		parsed_uid = Parsers.uid_format(row['uid'])
		if not parsed_uid:
			self.merge_error(
				row,
				"uid",
				"format"
			)
		# check time formatting and for FB use trait in unique key.
		if submission_type == "FB":
			if row['trait']:
				self.contains_data = True
			parsed_timestamp = Parsers.timestamp_fb_format(row['timestamp'])
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
		# Check time, and for trait duplicates in tables simply check for duplicate fields in header
		else:  # submission_type == "table":
			# check for date time info.
			if 'date' in row:
				parsed_date = Parsers.date_format(row['date'])
				# date required for trait data
				if not parsed_date or parsed_date is True:
					self.merge_error(
						row,
						"date",
						"format"
					)
				parsed_time = Parsers.time_format(row['time'])
				if not parsed_time:
					self.merge_error(
						row,
						"time",
						"format"
					)
				unique_key = (parsed_uid, parsed_date, parsed_time)
			elif 'start date' in row:
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
				unique_key = (
					parsed_uid,
					parsed_start_date,
					parsed_start_time,
					parsed_end_date,
					parsed_end_time
				)
			else:
				unique_key = None
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
	):
		errors = self.errors
		if int(row['row_index']) in errors:
			errors[int(row['row_index'])].add_error(field, error_type, feature_name, feature_format, category_list)
		else:
			errors[int(row['row_index'])] = RowParseResult(row)
			errors[int(row['row_index'])].add_error(field, error_type, feature_name, feature_format, category_list)

	def merge_conflict(
		self,
		row,
		conflicts
	):
		pass

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
			return '<table>' + html_table + '</table>'
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
		record['time'] = datetime.datetime.utcfromtimestamp(int(record['time']) / 1000).strftime("%Y-%m-%d %H:%M")
		record['start'] = datetime.datetime.utcfromtimestamp(int(record['start']) / 1000).strftime("%Y-%m-%d %H:%M")
		record['end'] = datetime.datetime.utcfromtimestamp(int(record['end']) / 1000).strftime("%Y-%m-%d %H:%M")
		record['submitted_at'] = datetime.datetime.utcfromtimestamp(int(record['submitted_at']) / 1000).strftime("%Y-%m-%d %H:%M:%S")
		self.record = record

	def conflict(self):
		if self.record['access']:
			if self.record['value'] == self.record['uploaded_value']:
				return False
			else:
				if isinstance(self.record['value'], list):
					if isinstance(self.record['uploaded_value'], list):
						if set(self.record['uploaded_value']) == set(self.record['value']):
							return False
					elif set([i.lower() for i in self.record['uploaded_value'].split(":")]) == set([y.lower() for y in self.record['value']]):
						return False
				return True
		else:
			return True


class SubmissionResult:
	def __init__(self, username, filename, submission_type):
		self.username = username
		self.filename = filename
		self.submission_type = submission_type
		self.conflicts = []
		self.resubmissions = []
		self.submitted = []

	def summary(self):
		return {
			"conflicts": len(self.conflicts),
			"resubmissions": len(self.resubmissions),
			"submitted": len(self.submitted)
		}

	def parse_record(self, record):
		submission_item = SubmissionRecord(record)
		if not record['found']:
			self.submitted.append(submission_item)
		elif submission_item.conflict():
			self.conflicts.append(submission_item)
		else:
			self.resubmissions.append(submission_item)

	def conflicts_file(self):
		username = self.username
		filename = self.filename
		if len(self.conflicts) == 0:
			return None
		else:
			download_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], self.username)
			if not os.path.isdir(download_path):
				os.mkdir(download_path)
				gid = grp.getgrnam(app.config['CELERYGRPNAME']).gr_gid
				os.chown(download_path, -1, gid)
				os.chmod(download_path, 0775)
			conflicts_filename = os.path.splitext(filename)[0] + '_conflicts.csv'
			conflicts_file_path = os.path.join(
				app.instance_path,
				app.config['DOWNLOAD_FOLDER'],
				username,
				conflicts_filename
			)
			conflicts_fieldnames = [
				"uid",
				"feature",
				"time",
				"submitted_by",
				"submitted_at",
				"value",
				"uploaded_value"
			]
			with open(conflicts_file_path, 'w') as conflicts_file:
				writer = csv.DictWriter(
					conflicts_file,
					fieldnames=conflicts_fieldnames,
					quoting=csv.QUOTE_ALL,
					extrasaction='ignore'
				)
				writer.writeheader()
				for row in self.conflicts:
					if not row.record['access']:
						row.record['value'] = 'ACCESS DENIED'
						row.record['submitted_by'] = row.record['partner']
					for item in row.record:
						if isinstance(row.record[item], list):
							row.record[item] = str(':'.join([str(i).encode() for i in row.record[item]]))
					writer.writerow(row.record)
		return conflicts_filename

	def resubmissions_file(self):
		username = self.username
		filename = self.filename
		if len(self.resubmissions) == 0:
			return None
		else:
			download_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], self.username)
			if not os.path.isdir(download_path):
				os.mkdir(download_path)
				gid = grp.getgrnam(app.config['CELERYGRPNAME']).gr_gid
				os.chown(download_path, -1, gid)
				os.chmod(download_path, 0775)
			resubmissions_filename = os.path.splitext(filename)[0] + '_resubmissions.csv'
			resubmissions_file_path = os.path.join(
				app.instance_path,
				app.config['DOWNLOAD_FOLDER'],
				username,
				resubmissions_filename)
			resubmissions_fieldnames = [
				"uid",
				"feature",
				"time",
				"submitted_by",
				"submitted_at",
				"value",
				"uploaded_value"
			]
			with open(resubmissions_file_path, 'w') as resubmissions_file:
				writer = csv.DictWriter(
					resubmissions_file,
					fieldnames = resubmissions_fieldnames,
					quoting = csv.QUOTE_ALL,
					extrasaction = 'ignore')
				writer.writeheader()
				for row in self.resubmissions:
					for item in row.record:
						if isinstance(row.record[item], list):
							row.record[item] = str(':'.join([str(i).encode() for i in row.record[item]]))
					writer.writerow(row.record)
		return resubmissions_filename

	def submitted_file(self):
		username = self.username
		filename = self.filename
		if len(self.submitted) == 0:
			return None
		else:
			download_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], self.username)
			if not os.path.isdir(download_path):
				os.mkdir(download_path)
				gid = grp.getgrnam(app.config['CELERYGRPNAME']).gr_gid
				os.chown(download_path, -1, gid)
				os.chmod(download_path, 0775)
			submitted_filename = os.path.splitext(filename)[0] + '_submitted.csv'
			submitted_file_path = os.path.join(
				app.instance_path,
				app.config['DOWNLOAD_FOLDER'],
				username,
				submitted_filename)
			submitted_fieldnames = [
				"uid",
				"feature",
				"time",
				"submitted_by",
				"submitted_at",
				"value"
			]
			with open(submitted_file_path, 'w') as submitted_file:
				writer = csv.DictWriter(
					submitted_file,
					fieldnames = submitted_fieldnames,
					quoting = csv.QUOTE_ALL,
					extrasaction = 'ignore')
				writer.writeheader()
				for row in self.submitted:
					for item in row.record:
						if isinstance(row.record[item], list):
							row.record[item] = str(':'.join([str(i).encode() for i in row.record[item]]))
					writer.writerow(row.record)
		return submitted_filename


class Upload:
	def __init__(self, username, submission_type, raw_filename):
		time = datetime.datetime.utcnow().strftime('_%Y%m%d-%H%M%S_')
		self.username = username
		self.raw_filename = raw_filename
		self.filename = secure_filename(time + '_' + raw_filename)
		self.submission_type = submission_type
		self.file_path = os.path.join(app.instance_path, app.config['UPLOAD_FOLDER'], username, self.filename)
		self.record_type = None
		self.trimmed_file_path = None
		self.parse_result = None
		self.submission_result = None
		self.fieldnames = None
		self.file_extension = None
		self.contains_data = None
		self.features = None

	def allowed_file(self):
		if '.' in self.raw_filename:
			self.file_extension = self.raw_filename.rsplit('.', 1)[1].lower()
			if self.file_extension in ALLOWED_EXTENSIONS:
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
			os.chmod(upload_path, 0775)
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
			wb = load_workbook(self.file_path, read_only=True)
			if "Template" not in wb.sheetnames:
				return 'This workbook does not contain a "Template" worksheet'
		else:
			return None

	def check_headers(self):
		if self.file_extension == 'csv':
			with open(self.file_path) as uploaded_file:
				file_dict = DictReaderInsensitive(uploaded_file)
				self.fieldnames = file_dict.fieldnames
				if len(self.fieldnames) > len(set(self.fieldnames)):
					return "This file contains duplicated header fields. This is not supported"
				fieldnames = set(self.fieldnames)
				if self.submission_type == 'FB':
					required = {'uid', 'trait', 'value', 'timestamp', 'person', 'location'}
				else:  # submission_type == 'table'
					required = {'uid', 'date', 'time', 'person'}
				if not required.issubset(fieldnames):
					missing_fieldnames = required - fieldnames
					return (
							'This file is missing the following headers: '
							+ ', '.join([i for i in missing_fieldnames])
					)
		elif self.file_extension == 'xlsx':
			try:
				wb = load_workbook(self.file_path, read_only=True)
			except BadZipfile:
				return 'This file does not appear to be a valid xlsx file'
			if "Template" not in wb.sheetnames:
				return 'This workbook does not contain the expected "Template" worksheet'
			ws = wb['Template']
			rows = ws.iter_rows(min_row=1, max_row=1)
			first_row = next(rows)
			self.fieldnames = [c.value.strip().lower() for c in first_row if c.value]
			fieldnames_lower = {c.value.strip().lower() for c in first_row if c.value}
			required = {'uid', 'person'}
			if not required.issubset(fieldnames_lower):
				missing_fieldnames = required - fieldnames_lower
				return (
						'The "Template" worksheet is missing the following fields: '
						+ ', '.join([i for i in missing_fieldnames])
				)
			other_required = [
				('trait', {'date', 'time'}),
				('condition',{'start date', 'start time', 'end date', 'end time'})
			]
			if not any(
				other_set[1].issubset(fieldnames_lower) for other_set in other_required
			):
				return (
						'The "Template" worksheet is missing required date/time fields.'
						' For Trait data these are "date" and "time", for condition data these are '
						' "start date", "start time", "end date", "end time".'
				)
			# check if more than one of the "other required" sets are found
			elif sum(bool(other_set[1] & fieldnames_lower) for other_set in other_required) > 1:
				return (
					'If you are submitting Trait data then please just include "date" and "time". '
					'If you are submitting Condition data then please just include '
					'"start date", "start time", "end date", "end time". '
					'Do not provide date/time elements from the other data type.'
				)
			else:
				for other_set in other_required:
					if other_set[1].issubset(fieldnames_lower):
						self.record_type = other_set[0]
		else:
			return None

	# clean up the csv by passing through dict reader and rewriting
	# consider writing the xls to a csv here...
	def trim_file(self):
		file_path = self.file_path
		trimmed_file_path = os.path.splitext(file_path)[0] + '_trimmed.csv'
		self.trimmed_file_path = trimmed_file_path
		if self.file_extension == 'csv':
			with open(file_path, 'r') as uploaded_file, open(trimmed_file_path, "w") as trimmed_file:
				# this dict reader lowers case and trims whitespace on all headers
				file_dict = DictReaderInsensitive(
					uploaded_file,
					skipinitialspace=True
				)
				file_writer = csv.DictWriter(
					trimmed_file,
					fieldnames=file_dict.fieldnames,
					quoting=csv.QUOTE_ALL
				)
				file_writer.writeheader()
				for j, row in file_dict:
					# remove rows without entries
					if any(field.strip() for field in row):
						for field in file_dict.fieldnames:
							if row[field]:
								row[field] = row[field].strip()
						file_writer.writerow(row)
		elif self.file_extension == 'xlsx':
			wb = load_workbook(self.file_path, read_only=True)
			ws = wb['Template']
			with open(trimmed_file_path, "wb") as trimmed_file:
				file_writer = csv.writer(
					trimmed_file,
					quoting=csv.QUOTE_ALL
				)
				rows = ws.iter_rows()
				first_row = next(rows)
				file_writer.writerow(['row_index'] + [cell.value.lower() for cell in first_row if cell.value])
				# handle deleted columns in the middle of the worksheet
				empty_headers = []
				for i, cell in enumerate(first_row):
					if not cell.value:
						empty_headers.append(i)
				for j, row in enumerate(rows):
					# j+2 to store the "Line number" as index
					# this is 0 based, and accounts for header
					cell_values = [j+2] + [cell.value for cell in row]
					# remove columns with empty header
					for i in sorted(empty_headers, reverse=True):
						del cell_values[i]
					# remove empty rows
					for i, value in enumerate(cell_values):
						if isinstance(value, datetime.datetime):
							cell_values[i] = value.strftime("%Y-%m-%d")
						elif isinstance(value, datetime.time):
							cell_values[i] = value.strftime("%H:%M")
						else:
							if isinstance(value, basestring):
								cell_values[i] = value.strip()
					if any(value for value in cell_values):
						file_writer.writerow(cell_values)

	def parse_rows(self):
		trimmed_file_path = self.trimmed_file_path
		submission_type = self.submission_type
		parse_result = ParseResult(submission_type, self.fieldnames)
		self.parse_result = parse_result
		with open(trimmed_file_path, 'r') as trimmed_file:
			trimmed_dict = DictReaderInsensitive(trimmed_file)
			for row in trimmed_dict:
				# firstly, if a table check for feature data, if none then just skip
				if self.submission_type == 'table':
					# firstly check if feature data in the row and ignore if empty
					record_properties = [
						'date',
						'start date',
						'start time',
						'end date',
						'end time',
						'time',
						'person'
					]
					index_headers = [
						'row_index',
						'uid'
					]
					not_features = record_properties + index_headers
					self.features = list(set(row.keys()).difference(set(not_features)))
					if [row[feature] for feature in self.features if row[feature]]:
						parse_result.parse_row(row)
					else:
						pass
				if submission_type == "FB":
					row_index = int(row['row_index'])
					parse_result.parse(row_index, row)
				# if many errors already then return immediately
				if parse_result.long_enough():
					return parse_result
		return parse_result

	def db_check(
		self,
		tx,
		parse_result
	):
		username = self.username
		trimmed_file_path = self.trimmed_file_path
		trimmed_filename = os.path.basename(trimmed_file_path)
		submission_type = self.submission_type
		with open(trimmed_file_path, 'r') as trimmed_file:
			#if submission_type == "FB":
				#check_result = [record[0] for record in tx.run(
				#	Cypher.upload_fb_check,
				#	username=username,
				#	filename=("file:///" + username + '/' + trimmed_filename),
				#	submission_type=submission_type
				#)]
				#for row_data in trimmed_dict:
				#	row_index = int(row_data['row_index'])
				#	item = check_result[row_index]
				#	if not item['uid']:
				#		parse_result.merge_error(
				#			row_index,
				#			row_data,
				#			"uid",
				#			"missing"
				#		)
				#	if not item['trait']:
				#		parse_result.merge_error(
				#			row_index,
				#			row_data,
				#			"trait",
				#			"missing"
				#		)
				#	if not item['value']:
				#		if all([item['trait'], item['uid']]):
				#			if 'category_list' in item:
				#				parse_result.merge_error(
				#					row_index,
				#					row_data,
				#					"value",
				#					"format",
				#					item['trait'],
				#					item['format'],
				#					item['category_list']
				#				)
				#			elif 'format' in item:
				#				parse_result.merge_error(
				#					row_index,
				#					row_data,
				#					"value",
				#					"format",
				#					item['trait'],
				#					item['format']
				#				)
				#			else:
				#				parse_result.merge_error(
				#					row_index,
				#					row_data,
				#					"value",
				#					"format",
				#					item['trait']
				#				)
			#else:
			# need to check for condition conflicts and
			# TODO also have to make sure that start < end, maybe do this earlier as doesn't need database
			#if self.record_type == 'condition':
			check_result = tx.run(
				Cypher.upload_table_condition_conflict_check,
				username=username,
				filename=urls.url_fix('file:///' + username + '/' + trimmed_filename ),
				submission_type=submission_type,
				features=self.features
			)
			#else:  # trait data
			#	check_result = tx.run(
			#		Cypher.upload_table_feature_format_check,
			#		username=username,
			#		filename=("file:///" + username + '/' + trimmed_filename),
			#		submission_type=submission_type,
			#		features=self.features
			#	)
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
				if not record['feature']:
					parse_result.add_field_error(
						record['input_feature'],
						(
							"This feature is not found. Please check your spelling. "
							"This may also be because the feature is not available at the level of these items"
						)
					)
				else:
				# we add found fields to a list to handle mixed items in input
				# i.e. if found at level of one item but not another
					parse_result.add_field_found(record['feature'])
				if all([
					record['UID'],
					record['feature'],
					not record['value']
				]):
					parse_result.merge_error(
						row,
						record['input_feature'],
						"format",
						record['feature'],
						record['format'],
						record['category_list']
					)
				if record['conflicts']:
					parse_result.merge_conflict(
						row,
						record['conflicts']
					)
			if parse_result.field_found:
				for field in parse_result.field_found:
					parse_result.rem_field_error(field)
			return parse_result

	def submit(
			self,
			tx
	):
		username = self.username
		trimmed_file_path = self.trimmed_file_path
		trimmed_filename = os.path.basename(trimmed_file_path)
		submission_type = self.submission_type
		filename = self.filename
		features = self.features
		self.submission_result = SubmissionResult(username, filename, submission_type)
		submission_result = self.submission_result
		if submission_type == 'FB':
			query = Cypher.upload_fb
			result = [record[0] for record in tx.run(
				query,
				username=username,
				filename=("file:///" + username + '/' + trimmed_filename),
				submission_type=submission_type
			)]
		else:  # submission_type == 'table':
			statement = Cypher.upload_table
			result = [record[0] for record in tx.run(
				statement,
				username=username,
				filename=urls.url_fix("file:///" + username + '/' + trimmed_filename),
				submission_type=submission_type,
				features=features
			)]
		# create a submission result
		for record in result:
			submission_result.parse_record(record)
		return submission_result

	@celery.task(bind=True)
	def async_submit(self, username, upload_object):
		try:
			with get_driver().session() as neo4j_session:
				# clean up the file removing empty lines and whitespace, lower case headers for matching in db
				upload_object.trim_file()
				# parse this trimmed file for errors, sets parse_result attribute to upload_object
				parse_result = upload_object.parse_rows()
				if parse_result.long_enough():
					return {
						'status': 'ERRORS',
						'result': parse_result
					}
				elif parse_result.duplicate_keys:
					return {
						'status': 'ERRORS',
						'result': parse_result
					}
				# if not too many errors continue to db check (reducing the number of client feedback loops)
				else:
					db_check_result = neo4j_session.read_transaction(
						upload_object.db_check,
						parse_result
					)
				if any([db_check_result.field_errors, db_check_result.errors]):
					return {
						'status': 'ERRORS',
						'result': db_check_result
					}
				else:
					# submit data
					submission_result = neo4j_session.write_transaction(
						upload_object.submit
					)
					# create summary dict
					submission_summary = submission_result.summary()
					# create files
					conflicts_file = submission_result.conflicts_file()
					resubmissions_file = submission_result.resubmissions_file()
					submitted_file = submission_result.submitted_file()
					# now need app context for the following (this is running asynchronously)
					with app.app_context():
						# create urls
						if conflicts_file:
							conflicts_file_url = url_for(
								'download_file',
								username=username,
								filename=conflicts_file,
								_external=True
							)
						else:
							conflicts_file_url = None
						if resubmissions_file:
							resubmissions_file_url = url_for(
								'download_file',
								username = username,
								filename = resubmissions_file,
								_external = True
							)
						else:
							resubmissions_file_url = None
						if submitted_file:
							submitted_file_url = url_for(
								'download_file',
								username = username,
								filename = submitted_file,
								_external = True
							)
						else:
							submitted_file_url = None
						if not any([conflicts_file, resubmissions_file, submitted_file]):
							response = 'No data submitted, please check that you uploaded a completed file'
							return {
								'status': 'SUCCESS',
								'result': response
							}
						# send result of merger in an email
						subject = "BreedCAFS upload summary"
						recipients = [User(username).find('')['email']]
						response = "Submission report:\n "
						if submission_summary['submitted']:
							response += "<p> - <a href= " + str(submitted_file_url) + ">" + str(submission_summary['submitted']) \
								+ "</a> new values were submitted to the database.\n </p>"
						if submission_summary['resubmissions']:
							response += "<p> - <a href= " + str(resubmissions_file_url) + ">" + str(submission_summary['resubmissions']) \
								+ "</a> existing values were already found.\n</p>"
						if submission_summary['conflicts']:
							response += "<p> - <a href= " + str(conflicts_file_url) + ">" + str(submission_summary['conflicts']) \
								+ "</a> conflicts with existing values were found and not submitted.\n</p>"
						body = "Submission_report: \n"
						body += " - " + str(submission_summary['submitted']) + 'new values were submitted to the database.\n'
						body += "   These are available at the following url: " + str(submitted_file_url) + "\n"
						body += " - " + str(submission_summary['resubmissions']) + 'existing values were already found.\n'
						body += "   These are available at the following url: " + str(resubmissions_file_url) + "\n"
						body += (
							" - "
							+ str(submission_summary['conflicts'])
							+ 'conflicts with existing values were found and not submitted.\n'
						)
						body += "   These are available at the following url: " + str(conflicts_file_url) + "\n"
						html = render_template(
							'emails/upload_report.html',
							response = response
						)
						send_email(subject, app.config['ADMINS'][0], recipients, body, html)
					return {
						'status': 'SUCCESS',
						'result': response
						}
		except (ServiceUnavailable, SecurityError) as exc:
			raise self.retry(exc=exc)
