from app import app, os, celery, ServiceUnavailable
from app.cypher import Cypher
from app.emails import send_email
from flask import render_template, url_for
from user import User
from config import ALLOWED_EXTENSIONS
from neo4j_driver import get_driver
import unicodecsv as csv
from datetime import datetime
from werkzeug.utils import secure_filename

from celery.contrib import rdb


class DictReaderInsensitive(csv.DictReader):
	# overwrites csv.fieldnames property so uses without surrounding whitespace and in lowercase
	@property
	def fieldnames(self):
		return [field.strip().lower() for field in csv.DictReader.fieldnames.fget(self)]

	def next(self):
		return DictInsensitive(csv.DictReader.next(self))


class DictInsensitive(dict):
	# This class overrides the __getitem__ method to automatically strip() and lower() the input key
	def __getitem__(self, key):
		return dict.__getitem__(self, key.strip().lower())

class RowParseResult:
	def __init__(self, row_num, row_data):
		self.row_num = int(row_num)
		self.row_data = row_data
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
				"format": "UID doesn't match BreedCAFS pattern: "
						  "  - plots are integers (e.g. '1')\n"
						  "  - blocks include the plot and block ID separated by '_B' (e.g. '1_B1')\n"
						  "  - trees include the plot and tree ID separated by '_T' (e.g. '1_T1')\n"
						  "  - branches include the plot and branch ID separated by '_Y' (e.g. '1_Y1')\n"
						  "  - leaves include the plot and leaf ID separated by '_L' (e.g. '1_L1')\n"
						  "  - samples include the plot and Sample ID separated by '_S' (e.g. '1_S1')\n",
				"missing": "This UID is not found in the database. "
			},
			"trait": {
				"missing": "This trait is not found in the database. "
						   "Please check the spelling and "
						   "that this trait is found among those supported by BreedCAFS "
						   "for the level of data you are submitting."
			},
			"value": {
				"format": "The value entered does not conform to expectations of that trait. "
						  "Please check the trait details available from this "
						  "and if the trait is categorical please check that this value is registered. "
			}
		}
	def add_error(self, field, error_type):
		if field not in self.errors:
			self.errors[field] = [error_type]
		else:
			self.errors[field].append(error_type)

	def headers(self):
		return self.row_data.keys()

	def get_row_data(self):
			return self.row_data

	def get_row_errors(self):
			return self.errors

	def html_row (self, fieldnames):
		row_string = '<tr><td>' + str(self.row_num) + '</td>'
		for field in fieldnames:
			if not self.row_data[field]:
				row_string += '<td></td>'
			else:
				if not field in self.errors:
					row_string += '<td>' + str(self.row_data[field]) + '</td>'
				else:
					if not field in self.error_comments:
						if field in fieldnames:
							row_string += '<td bgcolor = #FFFF00 title = "' \
								+ str(self.error_comments['value']['format']) + '">' \
								+ str(self.row_data[field]) + '</td>'
						else:
							row_string += '<td bgcolor = #FFFF00 title = "unknown error">' + str(self.row_data[field]) + '</td>'
					else:
						row_string += '<td bgcolor = #FFFF00 title = "Errors: '
						for error in self.errors[field]:
							if not error in self.error_comments[field]:
								row_string += ' - unknown error\n'
							else:
								row_string += ' - ' + str(error) + ':' + str(self.error_comments[field][error]) + '\n'
						row_string += '">' + str(self.row_data[field]) + '</td>'
		return row_string

class ParseResult:
	def __init__(self, submission_type, fieldnames):
		self.submission_type = submission_type
		self.fieldnames = fieldnames
		self.field_errors = None
		self.field_found = None
		self.parse_result = None

	def add_field_error(self, field, type):
		if not self.field_errors:
			self.field_errors = {}
		self.field_errors[field] = type

	# this is needed to create a list of found fields in case the error is found at one level in a table but not others
	# the list is removed from field_errors at the end of parsing
	def add_field_found(self, field):
		if not self.field_found:
			self.field_found = []
		self.field_found.append(field)

	def field_found_list(self):
		return self.field_found

	def rem_field_error(self, field):
		if field in self.field_errors:
			del self.field_errors[field]

	def parse_row(self, line_num, row_data):
		submission_type = self.submission_type
		if submission_type == "FB":
			if not Parsers.timestamp_fb_format(row_data['timestamp']):
				self.merge_error(
					line_num,
					row_data,
					"timestamp",
					"format"
				)
		else:  # submission_type == "table":
			if not Parsers.date_format(row_data['date']):
				self.merge_error(
					line_num,
					row_data,
					"date",
					"format"
				)
			if not Parsers.time_format(row_data['time']):
				self.merge_error(
					line_num,
					row_data,
					"time",
					"format"
				)
		# check uid formatting
		if not Parsers.uid_format(row_data['uid']):
			self.merge_error(
				line_num,
				row_data,
				"uid",
				"format"
			)

	def merge_error(self, line_num, row_data, field, error_type):
		if not self.parse_result:
			self.parse_result = {}
		parse_result = self.parse_result
		if line_num in parse_result:
			parse_result[line_num].add_error(field, error_type)
		else:
			parse_result[line_num] = RowParseResult(line_num, row_data)
			parse_result[line_num].add_error(field, error_type)

	def row_errors(self):
		return self.parse_result

	def field_errors_dict(self):
		return self.field_errors

	def html_table(self):
		max_length = 100
		# create a html table string with tooltip for details
		if self.parse_result:
			header_string = '<tr><th><p>Line#</p></th>'
			for field in self.fieldnames:
				if self.field_errors:
					if field in self.field_errors:
						header_string += '<th bgcolor = #FFFF00 title = "' + str(self.field_errors[field]) \
							+ '"><p>' + str(field) + '</p></th>'
					else:
						header_string += '<th><p>' + str(field) + '</p></th>'
				else:
					header_string += '<th><p>' + str(field) + '</p></th>'
			header_string += '</tr>'
			html_table = header_string
			# construct each row and append to growing table
			for i, item in enumerate(self.parse_result):
				if i >= max_length:
					return html_table
				row_string = self.parse_result[item].html_row(self.fieldnames)
				html_table += row_string
			return html_table
		else:
			return None

	def long_enough(self):
		max_length = 10
		if self.parse_result:
			if len(self.parse_result) >= max_length:
				return True
		return False

class ItemSubmissionResult:
	def __init__(
			self,
			found,
			submitted_by,
			submitted_at,
			value,
			uploaded_value,
			uid,
			trait,
			time
	):
		self.found = found
		self.submitted_by = submitted_by
		self.submitted_at = datetime.fromtimestamp(int(submitted_at) / 1000).strftime("%Y-%m-%d %H:%M:%S")
		self.value = value
		self.uploaded_value = uploaded_value
		self.uid = uid
		self.trait = trait
		self.time = datetime.fromtimestamp(int(time) / 1000).strftime("%Y-%m-%d %H:%M:%S")

	def fb_item(self, timestamp):
		self.timestamp = timestamp

	def table_item(self, table_date, table_time):
		self.table_date = table_date
		self.table_time = table_time

	def conflict(self):
		if self.value == self.uploaded_value:
			return False
		else:
			return True

	def as_dict(self, submission_type):
		item_dict = {
			"found": self.found,
			"submitted_by": self.submitted_by,
			"submitted_at": self.submitted_at,
			"value": self.value,
			"uploaded_value": self.uploaded_value,
			"uid": self.uid,
			"trait": self.trait,
			"time": self.time
		}
		if submission_type == "FB":
			item_dict['timestamp'] = self.timestamp
		else: #  submission type == 'table'
			item_dict['table_date'] = self.table_date
			item_dict['table_time'] = self.table_time
		return item_dict

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
		submission_type = self.submission_type
		submission_item = ItemSubmissionResult(
			record['found'],
			record['submitted_by'],
			record['submitted_at'],
			record['value'],
			record['uploaded_value'],
			record['uid'],
			record['trait'],
			record['time']
		)
		if submission_type == "FB":
			submission_item.fb_item(record['timestamp'])
		else: #  submission type == 'table'
			submission_item.table_item(record['table_date'], record['table_time'])
		if not record['found']:
			self.submitted.append(submission_item)
		elif submission_item.conflict():
			self.conflicts.append(submission_item)
		else:
			self.resubmissions.append(submission_item)

	def conflicts_file(self):
		username = self.username
		filename = self.filename
		submission_type = self.submission_type
		if len(self.conflicts) == 0:
			return None
		else:
			conflicts_filename = os.path.splitext(filename)[0] + '_conflicts.csv'
			conflicts_file_path = os.path.join(
				app.instance_path,
				app.config['DOWNLOAD_FOLDER'],
				username,
				conflicts_filename
			)
			conflicts_fieldnames = [
				"uid",
				"trait",
				"time",
				"submitted_by",
				"submitted_at",
				"value",
				"uploaded_value"
			]
			with open(conflicts_file_path, 'w') as conflicts_file:
				writer = csv.DictWriter(
					conflicts_file,
					fieldnames = conflicts_fieldnames,
					quoting = csv.QUOTE_ALL,
					extrasaction = 'ignore'
				)
				writer.writeheader()
				for item in self.conflicts:
					writer.writerow(item.as_dict(submission_type))
		return conflicts_filename

	def resubmissions_file(self):
		username = self.username
		filename = self.filename
		submission_type = self.submission_type
		if len(self.resubmissions) == 0:
			return None
		else:
			resubmissions_filename = os.path.splitext(filename)[0] + '_resubmissions.csv'
			resubmissions_file_path = os.path.join(
				app.instance_path,
				app.config['DOWNLOAD_FOLDER'],
				username,
				resubmissions_filename)
			resubmissions_fieldnames = [
				"uid",
				"trait",
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
				for item in self.resubmissions:
					writer.writerow(item.as_dict(submission_type))
		return resubmissions_filename


	def submitted_file(self):
		username = self.username
		filename = self.filename
		submission_type = self.submission_type
		if len(self.submitted) == 0:
			return None
		else:
			submitted_filename = os.path.splitext(filename)[0] + '_submitted.csv'
			submitted_file_path = os.path.join(
				app.instance_path,
				app.config['DOWNLOAD_FOLDER'],
				username,
				submitted_filename)
			submitted_fieldnames = [
				"uid",
				"trait",
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
				for item in self.submitted:
					writer.writerow(item.as_dict(submission_type))
		return submitted_filename


class Parsers:
	def __init__(self):
		pass
	@staticmethod
	def timestamp_fb_format(timestamp_string):
		timestamp = str(timestamp_string).strip()
		try:
			datetime.strptime(timestamp[0:19], '%Y-%m-%d %H:%M:%S')
			if not all([
				timestamp[-5] in ['+','-'],
				int(timestamp[-4:-2]) < 24,
				int(timestamp[-4:-2]) >= 0,
				int(timestamp[-2:]) < 60,
				int(timestamp[-2:]) >= 0
			]):
				return False
			else:
				return True
		except ValueError:
			return False

	@staticmethod
	def date_format(date_string):
		date_string = str(date_string).strip()
		try:
			datetime.strptime(date_string, '%Y-%m-%d')
			return True
		except ValueError:
			return False

	@staticmethod
	def time_format(time_string):
		time_string = str(time_string).strip()
		try:
			if not all([
				time_string[2] == ':',
				int(time_string[0:2]) < 24,
				int(time_string[0:2]) >= 0,
				int(time_string[3:5]) < 60,
				int(time_string[3:5]) >= 0
			]):
				return False
			else:
				return True
		except ValueError:
			return False

	@staticmethod
	def uid_format(uid):
		uid = str(uid).strip().upper()
		if uid.isdigit():
			return True
		else:
			if len(uid.split("_")) == 2:
				if all([
					uid.split("_")[0].isdigit(),
					uid.split("_")[1][0] in ["B", "T", "R", "L", "S"],
					uid.split("_")[1][1:].isdigit()
				]):
					return True
				else:
					return False
			else:
				return False

class Upload:
	def __init__(self, username, submission_type, raw_filename):
		time = datetime.now().strftime('_%Y%m%d-%H%M%S_')
		self.username = username
		self.filename = secure_filename(time + '_' + raw_filename)
		self.submission_type = submission_type
		self.file_path = os.path.join(app.instance_path, app.config['UPLOAD_FOLDER'], username, self.filename)
		self.trimmed_file_path = None
		self.parse_result = None
		self.submission_result = None


	@staticmethod
	def allowed_file(filename):
		return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

	def file_save(self, file_data):
		# create user upload path if not found
		if not os.path.isdir(os.path.join(app.instance_path, app.config['UPLOAD_FOLDER'], self.username)):
			os.mkdir(os.path.join(app.instance_path, app.config['UPLOAD_FOLDER'], self.username))
		file_data.save(self.file_path)

	def is_valid_csv(self):
		with open(self.file_path) as uploaded_file:
			# TODO implement CSV kit checks - in particular csvstat to check field length (avoid stray quotes)
			# now get the dialect and check it conforms to expectations
			dialect = csv.Sniffer().sniff(uploaded_file.read())
			if not all((dialect.delimiter == ',', dialect.quotechar == '"')):
				return False
			else:
				return True

	def check_headers(self):
		with open(self.file_path) as uploaded_file:
			file_dict = DictReaderInsensitive(uploaded_file)
			self.fieldnames = file_dict.fieldnames
			fieldnames = set(self.fieldnames)
			if self.submission_type == 'FB':
				required = set(['uid', 'trait', 'value', 'timestamp', 'person', 'location'])
			else:  # submission_type == 'table'
				required = set(['uid', 'date', 'time'])
			if not required.issubset(fieldnames):
				missing_headers = required - fieldnames
				return "This file is missing the following headers: " + str([i for i in missing_headers])
			else:
				return None

	# clean up the csv by passing through dict reader and rewriting
	def trim_file(self):
		file_path = self.file_path
		self.trimmed_file_path = os.path.splitext(file_path)[0] + '_trimmed.csv'
		trimmed_file_path = self.trimmed_file_path
		with open(file_path, 'r') as uploaded_file, open(trimmed_file_path, "w") as trimmed_file:
			# this dict reader lowers case and trims whitespace on all headers
			file_dict = DictReaderInsensitive(
				uploaded_file,
				skipinitialspace=True
			)
			file_writer = csv.DictWriter(
				trimmed_file,
				fieldnames = file_dict.fieldnames,
				quoting = csv.QUOTE_ALL
			)
			file_writer.writeheader()
			for row in file_dict:
				# remove rows without entries
				if any(field.strip() for field in row):
					for field in file_dict.fieldnames:
						row[field] = row[field].strip()
					file_writer.writerow(row)

	def parse_rows(self):
		trimmed_file_path = self.trimmed_file_path
		submission_type = self.submission_type
		parse_result = ParseResult(submission_type, self.fieldnames)
		self.parse_result = parse_result
		with open(trimmed_file_path, 'r') as trimmed_file:
			trimmed_dict = DictReaderInsensitive(trimmed_file)
			for row_data in trimmed_dict:
				line_num = int(trimmed_dict.line_num)
				# check timestamp formatting against regex
				parse_result.parse_row(line_num, row_data)
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
			trimmed_dict = DictReaderInsensitive(trimmed_file)
			if submission_type == "FB":
				check_result = [record[0] for record in tx.run(
					Cypher.upload_fb_check,
					username = username,
					filename = ("file:///" + username + '/' + trimmed_filename),
					submission_type = submission_type
				)]
				for row_data in trimmed_dict:
					line_num = int(trimmed_dict.line_num)
					# 0 based list call and have to account for header row so -2
					item = check_result[line_num - 2]
					if not item['uid']:
						parse_result.merge_error(
							line_num,
							row_data,
							"uid",
							"missing"
						)
					if not item['trait']:
						parse_result.merge_error(
							line_num,
							row_data,
							"trait",
							"missing"
						)
					if not item['value']:
						if item['trait']:
							parse_result.merge_error(
								line_num,
								row_data,
								"value",
								"format"
							)
			else:
				required = ['uid', 'date', 'time', 'person']
				index_headers = [
					'country',
					'region',
					'farm',
					'plot',
					'block',
					'variety',
					'treeid',
					'treecustomid',
					'branchid',
					'leafid',
					'tissue',
					'storage'
				]
				not_traits = required + index_headers
				traits = [i for i in trimmed_dict.fieldnames if i not in not_traits]
				check_result = [record[0] for record in tx.run(
					Cypher.upload_table_check,
					username = username,
					filename = ("file:///" + username + '/' + trimmed_filename),
					submission_type = submission_type,
					traits = traits
				)]
				for row_data in trimmed_dict:
					line_num = int(trimmed_dict.line_num)
					# 0 based list call and have to account for header row so -2
					item_num = line_num -2
					for i, trait in enumerate(traits):
						item = check_result[item_num * len(traits) + i]
						if not item['uid']:
							parse_result.merge_error(
								line_num,
								row_data,
								"uid",
								"missing"
							)
						#this isn't so simple with mixed levels, sometimes the trait is found for some levels.
						if not item['trait']:
							parse_result.add_field_error(
								trait,
								"This trait is not found. Please check your spelling. This may also be because the trait is not registered at the level of these items"
							)
						else:
							#this is to handle mixed levels, otherwise the upload would fail if a trait was only found for some levels
							parse_result.add_field_found(trait)
						if not item['value']:
							if all([row_data[trait], item['trait']]):
								parse_result.merge_error(
									line_num,
									row_data,
									trait,
									"format"
								)
				if parse_result.field_found_list():
					for field in parse_result.field_found_list():
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
		self.submission_result = SubmissionResult(username, filename, submission_type)
		submission_result = self.submission_result
		if submission_type == 'FB':
			query = Cypher.upload_fb
			result = [record[0] for record in tx.run(
				query,
				username = username,
				filename = ("file:///" + username + '/' + trimmed_filename),
				submission_type = submission_type
			)]
		else:  # submission_type == 'table':
			with open(trimmed_file_path, 'r') as uploaded_file:
				uploaded_dict = DictReaderInsensitive(uploaded_file)
				required = ['uid', 'date', 'time', 'person']
				index_headers = [
					'country',
					'region',
					'farm',
					'plot',
					'block',
					'variety',
					'treeid',
					'treecustomid',
					'branchid',
					'leafid',
					'tissue',
					'storage'
				]
				not_traits = required + index_headers
				traits = [i for i in uploaded_dict.fieldnames if i not in not_traits]
			query = Cypher.upload_table
			result = [record[0] for record in tx.run(
				query,
				username = username,
				filename = ("file:///" + username + '/' + trimmed_filename),
				submission_type = submission_type,
				traits = traits
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
				else:
					# if not too many errors continue to db check (reducing the number of client feedback loops)
					db_check_result = neo4j_session.read_transaction(
						upload_object.db_check,
						parse_result
					)
				if any([db_check_result.field_errors_dict(), db_check_result.row_errors()]):
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
								username = username,
								filename = conflicts_file,
								_external = True
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
						body += " - " + str(submission_summary['conflicts']) + 'conflicts with existing values were found and not submitted.\n'
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
		except ServiceUnavailable as exc:
			raise self.retry(exc=exc)
