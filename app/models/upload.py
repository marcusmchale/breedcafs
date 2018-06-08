from app import app, os, celery, ServiceUnavailable
from app.cypher import Cypher
from app.emails import send_email
from flask import render_template
from user import User
from config import ALLOWED_EXTENSIONS
from neo4j_driver import get_driver
import unicodecsv as csv
import re
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


class Upload:
	def __init__(self, username, filename):
		self.username = username
		self.filename = filename

	def allowed_file(self):
		return '.' in self.filename and \
			self.filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


# needed to separate this for celery as the class is not easily serialised into JSON for async calling
@celery.task(bind=True)
def async_submit(
		self,
		username,
		filename,
		submission_type,
		traits = None
):
	try:
		with get_driver().session() as neo4j_session:
			check = neo4j_session.read_transaction(
				_check,
				username,
				filename,
				submission_type,
				traits
			)
			if any([check[i] for i in check.keys()]):
				return {
					"success": False,
					"result": check
				}
			else:
				result = neo4j_session.write_transaction(
					_submit,
					username,
					filename,
					submission_type,
					traits
				)
				with app.app_context():
					# send result of merger in an email
					subject = "BreedCAFS upload summary"
					recipients = [User(username).find('')['email']]
					body = (
						"You uploaded a file to the BreedCAFS database."
						+ str(result["new_submissions"]) + " new values were submitted to the database and "
						+ str(result["resubmissions"]) + " existing values were found. Conflicts file: "
						+ str(result["conflicts"])
					)
					html = render_template(
						'emails/upload_report.html',
						new = result["new_submissions"],
						old = result["resubmissions"]
					)
					send_email(subject, app.config['ADMINS'][0], recipients, body, html)
				return {
					"success": True,
					"result": result
				}
	except ServiceUnavailable as exc:
		raise self.retry(exc=exc)


def _check(
		tx,
		username,
		filename,
		submission_type,
		traits
):
	uploaded_file_path = os.path.join(
		app.instance_path,
		app.config['UPLOAD_FOLDER'],
		username,
		filename)
	max_errors = 10
	with open(uploaded_file_path, 'r') as uploaded_file:
		uploaded_dict = DictReaderInsensitive(uploaded_file)
		if submission_type == "FB":
			errors = {
				"timestamp": [],
				"uid_format": []
			}
			timestamp_re = re.compile(
				"^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9][+-][0-9][0-9][0-9][0-9]$"
			)
			for item in uploaded_dict:
				if len([i for i in errors.keys()]) > max_errors:
					return errors
				row_item = (uploaded_dict.line_num, item)
				if not timestamp_re.match(item['timestamp'].strip()):
					errors["timestamp"].append(row_item)
				uid = str(item['uid']).strip().upper()
				if not uid.isdigit():
					if len(uid.split("_")) == 2:
						if not all([
							uid.split("_")[0].isdigit(),
							uid.split("_")[1][0] in ["B", "T", "R", "L", "S"],
							uid.split("_")[1][1:].isdigit()
						]):
							errors['uid_format'].append(row_item)
					else:
						errors['uid_format'].append(row_item)
			if any(errors[i] for i in errors):
				return errors
			# now if no obvious errors then further check against the database for UIDs, traits and trait/level combo
			else:
				check_result = [record[0] for record in tx.run(
					Cypher.upload_fb_check,
					username=username,
					filename=("file:///" + username + '/' + filename),
					submission_type = submission_type,
					traits = traits
				)]
		else:  # submission_type == "table":
			errors = {
				"date": [],
				"time": [],
				"uid_format": []
			}
			date_re = re.compile(
				'^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$'
			)
			time_re = re.compile(
				'^[0-9][0-9]:[0-9][0-9]$'
			)
			for item in uploaded_dict:
				if len([i for i in errors.keys()]) > max_errors:
					return errors
				row_item = (uploaded_dict.line_num, item)
				if not date_re.match(item['date'].strip()):
					errors["date"].append(row_item)
				if not time_re.match(item['time'].strip()):
					errors["time"].append(row_item)
				uid = str(item['uid']).strip().upper()
				if not uid.isdigit():
					if len(uid.split("_")) == 2:
						if not all([
							uid.split("_")[0].isdigit(),
							uid.split("_")[1][0] in ["B", "T", "R", "L", "S"],
							uid.split("_")[1][1:].isdigit()
						]):
							errors['uid_format'].append(row_item)
					else:
						errors['uid_format'].append(row_item)
			if any(errors[i] for i in errors):
				return errors
			else:
				# now if no obvious errors then further check against the database for UIDs, traits and trait/level combo
				check_result = [record[0] for record in tx.run(
					Cypher.upload_table_check,
					username=username,
					filename=("file:///" + username + '/' + filename),
					submission_type=submission_type
				)]
		errors = {
			'uid_missing': [],
			'trait_missing': [],
			'value': []
		}
		uploaded_file.seek(0)
		uploaded_dict = DictReaderInsensitive(uploaded_file)
		if check_result:
			for row in check_result:
				row_item = (uploaded_dict.line_num, uploaded_dict.next())
				if not row['uid']:
					errors['uid_missing'].append(row_item)
				if not row['trait']:
					errors['trait_missing'].append(row_item)
				if not row['value']:
					errors['value'].append(row_item)
			return errors


def _submit(
		tx,
		username,
		filename,
		submission_type,
		traits
):
	if submission_type == 'FB':
		query = Cypher.upload_fb
		result = [record[0] for record in tx.run(
			query,
			username=username,
			filename=("file:///" + username + '/' + filename),
			submission_type=submission_type
		)]
	else:  # submission_type == 'table':
		query = Cypher.upload_table
		result = [record[0] for record in tx.run(
			query,
			username = username,
			filename = ("file:///" + username + '/' + filename),
			submission_type = submission_type,
			traits = traits
			)]
	new_submissions = []
	resubmissions = []
	conflicts = []
	for item in result:
		if not item["found"]:
			new_submissions += [item]
		else:
			if item["value"] == item["uploaded_value"]:
				resubmissions += [item]
			else:
				conflicts += [item]
	# handle conflicts
	if len(conflicts) >= 0:
		conflicts_filename = os.path.splitext(filename)[0] + '_conflicts.csv'
		conflicts_file_path = os.path.join(
			app.instance_path,
			app.config['DOWNLOAD_FOLDER'],
			username,
			conflicts_filename)
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
				extrasaction = 'ignore')
			writer.writeheader()
			for row in conflicts:
				row["time"] = datetime.fromtimestamp(int(row["time"])/1000).strftime("%Y-%m-%d %H:%M:%S")
				row["submitted_at"] = datetime.fromtimestamp(int(row["submitted_at"]) / 1000).strftime("%Y-%m-%d %H:%M:%S")
				writer.writerow(row)
	return {
		"conflicts": conflicts_file_path,
		"resubmissions": len(resubmissions),
		"new_submissions": len(new_submissions)
	}