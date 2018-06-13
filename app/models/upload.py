from app import app, os, celery, ServiceUnavailable
from app.cypher import Cypher
from app.emails import send_email
from flask import render_template, url_for
from user import User
from config import ALLOWED_EXTENSIONS
from neo4j_driver import get_driver
import unicodecsv as csv
from datetime import datetime

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

	def html_row (self, headers):
		row_string = '<tr><td>' + str(self.row_num) + '</td>'
		for column in headers:
			if column in self.errors:
				row_string += '<td bgcolor = #FFFF00 title = "'
				for error in self.errors[column]:
					row_string += ' - ' + str(error) + ':' + str(self.error_comments[column][error]) + '\n'
				row_string += '">' + str(self.row_data[column]) + '</td>'
			else:
				row_string += '<td>' + str(self.row_data[column]) + '</td>'
		return row_string

class ParseResult:
	def __init__(self):
		self.parse_result = {}

	def merge_error(self, row_num, row_data, field, error_type):
		if row_num in self.parse_result:
			self.parse_result[row_num].add_error(field, error_type)
		else:
			self.parse_result[row_num] = RowParseResult(row_num, row_data)
			self.parse_result[row_num].add_error(field, error_type)

	def row_errors(self):
		return self.parse_result

	def html_table(self):
		# create a html table string with tooltip for details
		if self.parse_result:
			# get headers from a random element in the dictionary (all should be equivalent)
			headers = self.parse_result[next(iter(self.parse_result))].headers()
			header_string = '<tr><th><p>Line#</p></th>'
			for i in headers:
				header_string += '<th><p>' + str(i) + '</p></th>'
			header_string += '</tr>'
			html_table = header_string
			# construct each row and append to growing table
			for item in self.parse_result:
				row_string = self.parse_result[item].html_row(headers)
				html_table += row_string
			return html_table
		else:
			return None

	def long_enough(self):
		max_length = 10
		if len(self.parse_result) >= max_length:
			return True
		else:
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
			submission_item.table_item(record['date'], record['time'])
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
	def __init__(self, username, filename):
		self.username = username
		self.filename = filename

	def allowed_file(self):
		return '.' in self.filename and \
			self.filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

	def preparse(
			self,
			submission_type
	):
		username = self.username
		filename = self.filename
		uploaded_file_path = os.path.join(
			app.instance_path,
			app.config['UPLOAD_FOLDER'],
			username,
			filename
		)
		with open(uploaded_file_path, 'r') as uploaded_file:
			uploaded_dict = DictReaderInsensitive(uploaded_file)
			parse_results = ParseResult()
			for row_data in uploaded_dict:
				line_num = int(uploaded_dict.line_num)
				# check timestamp formatting against regex
				if submission_type == "FB":
					if not Parsers.timestamp_fb_format(row_data['timestamp']):
						parse_results.merge_error(
							line_num,
							row_data,
							"timestamp",
							"format"
						)
				else:  # submission_type == "table":
					if not Parsers.date_format(row_data['date']):
						parse_results.merge_error(
							line_num,
							row_data,
							"date",
							"format"
						)
					if not Parsers.time_format(row_data['time']):
						parse_results.merge_error(
							line_num,
							row_data,
							"time",
							"format"
						)
				# check uid formatting
				if not Parsers.uid_format(row_data['uid']):
					parse_results.merge_error(
						line_num,
						row_data,
						"uid",
						"format"
					)
				# if many errors already then return immediately
				if parse_results.long_enough():
					return parse_results
		return parse_results


	def db_check(
		self,
		tx,
		submission_type,
		# handing parse results in from the prior pre-parsing
		parse_results
	):
		username = self.username
		filename = self.filename
		uploaded_file_path = os.path.join(
			app.instance_path,
			app.config['UPLOAD_FOLDER'],
			username,
			filename
		)
		with open(uploaded_file_path, 'r') as uploaded_file:
			uploaded_dict = DictReaderInsensitive(uploaded_file)
			if submission_type == "FB":
				check_result = [record[0] for record in tx.run(
					Cypher.upload_fb_check,
					username = username,
					filename = ("file:///" + username + '/' + filename),
					submission_type = submission_type
				)]
			else:
				required = ['uid', 'date', 'time', 'person']
				traits = [i for i in uploaded_dict.fieldnames if i not in required]

				check_result = [record[0] for record in tx.run(
					Cypher.upload_table_check,
					username = username,
					filename = ("file:///" + username + '/' + filename),
					submission_type = submission_type,
					traits = traits
				)]
			for row_data in uploaded_dict:
				line_num = int(uploaded_dict.line_num)
				# 0 based list call and have to account for header row so -2
				item = check_result[line_num -2]
				if not item['uid']:
					parse_results.merge_error(
						line_num,
						row_data,
						"uid",
						"missing"
					)
				if not item['trait']:
					parse_results.merge_error(
						line_num,
						row_data,
						"trait",
						"missing"
					)
				if not item['value']:
					parse_results.merge_error(
						line_num,
						row_data,
						"value",
						"format"
					)
			return parse_results

	def submit(
			self,
			tx,
			submission_type
	):
		username = self.username
		filename = self.filename
		uploaded_file_path = os.path.join(
			app.instance_path,
			app.config['UPLOAD_FOLDER'],
			username,
			filename
		)
		if submission_type == 'FB':
			query = Cypher.upload_fb
			result = [record[0] for record in tx.run(
				query,
				username = username,
				filename = ("file:///" + username + '/' + filename),
				submission_type=submission_type
			)]
		else:  # submission_type == 'table':
			with open(uploaded_file_path, 'r') as uploaded_file:
				uploaded_dict = DictReaderInsensitive(uploaded_file)
				required = ['uid', 'date', 'time', 'person']
				traits = [i for i in uploaded_dict.fieldnames if i not in required]
			query = Cypher.upload_table
			result = [record[0] for record in tx.run(
				query,
				username=username,
				filename=("file:///" + username + '/' + filename),
				submission_type=submission_type,
				traits=traits
			)]
		# create a submission result
		submission_result = SubmissionResult(username, filename, submission_type)
		for record in result:
			submission_result.parse_record(record)
		return submission_result


# needed to separate this for celery as the class is not easily serialised into JSON for async calling
@celery.task(bind=True)
def async_submit(
		self,
		username,
		filename,
		submission_type
):
	try:
		with get_driver().session() as neo4j_session:
			upload = Upload(username, filename)
			preparse_result = upload.preparse(submission_type)
			if preparse_result.long_enough():
				return {
					'status': 'ERRORS',
					'result': preparse_result
				}
			else:
				# if not too many errors continue to db check (reducing the number of client feedback loops)
				db_check_result = neo4j_session.read_transaction(
					upload.db_check,
					submission_type,
					preparse_result
				)
			if db_check_result.row_errors():
				return {
					'status': 'ERRORS',
					'result': db_check_result
				}
			else:
				# submit data
				submission_result = neo4j_session.write_transaction(
					upload.submit,
					submission_type
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
						resubmissions_file = None
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
						response += "<p> - <a href= " + submitted_file_url + ">" + str(submission_summary['submitted']) \
							+ "</a> new values were submitted to the database.\n </p>"
					if submission_summary['resubmissions']:
						response += "<p> - <a href= " + resubmissions_file_url + ">" + str(submission_summary['resubmissions']) \
							+ "</a> existing values were already found.\n</p>"
					if submission_summary['conflicts']:
						response += "<p> - <a href= " + conflicts_file_url + ">" + str(submission_summary['conflicts']) \
							+ "</a> conflicts with existing values were found and not submitted.\n</p>"
					body = response
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
