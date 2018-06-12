import os
from app import app, ServiceUnavailable
from flask import ( flash, redirect, url_for, request, session, render_template, jsonify)
from app.models import (
	Upload,
	async_submit,
	DictReaderInsensitive
)
from app.forms import UploadForm
from werkzeug.utils import secure_filename
from datetime import (
	datetime
)
import unicodecsv as csv 


@app.route('/upload', methods=['GET', 'POST'])
def upload():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			form = UploadForm()
			return render_template(
				'upload.html',
				form = form,
				title = 'Upload'
			)
		except ServiceUnavailable:
			flash("Database unavailable")
			return redirect(url_for('index'))

@app.route('/upload_submit', methods=['POST'])
def upload_submit():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		form = UploadForm()
		if form.validate_on_submit():
			username = session['username']
			time = datetime.now().strftime('_%Y%m%d-%H%M%S_')
			file = form.file.data
			submission_type = form.submission_type.data
			if Upload(username, file.filename).allowed_file():
				try:
					# create user upload path if not found
					if not os.path.isdir(os.path.join(app.instance_path, app.config['UPLOAD_FOLDER'], username)):
						os.mkdir(os.path.join(app.instance_path, app.config['UPLOAD_FOLDER'], username))
					# prepare a secure filename to save with
					filename = secure_filename(time + file.filename)
					uploaded_file_path = os.path.join(app.instance_path, app.config['UPLOAD_FOLDER'], username, filename)
					file.save(uploaded_file_path)
					with open(uploaded_file_path) as uploaded_file:
						# TODO implement CSV kit checks - in particular csvstat to check field length (avoid stray quotes)
						# now get the dialect and check it conforms to expectations
						uploaded_file.seek(0)
						dialect = csv.Sniffer().sniff(uploaded_file.read())
						if not all((dialect.delimiter == ',', dialect.quotechar == '"')):
							return jsonify(
								{'submitted': 'Please upload comma (,) separated file with quoted (") fields' })
						# clean up the csv file by passing through dict reader and rewriting
						trimmed_file_path = os.path.splitext(uploaded_file_path)[0] + '_trimmed.csv'
						with open(trimmed_file_path, "w") as trimmed_file:
							uploaded_file.seek(0)
							# this dict reader lowers case and trims whitespace on all headers
							uploaded_file_dict = DictReaderInsensitive(
								uploaded_file,
								skipinitialspace=True
							)
							trimmed_file_writer = csv.DictWriter(
								trimmed_file,
								fieldnames = uploaded_file_dict.fieldnames,
								quoting = csv.QUOTE_ALL
							)
							trimmed_file_writer.writeheader()
							for row in uploaded_file_dict:
								# remove rows without entries
								if any(field.strip() for field in row):
									for field in uploaded_file_dict.fieldnames:
										row[field] = row[field].strip()
									trimmed_file_writer.writerow(row)
					# re-open the cleaned up file
					trimmed_filename = os.path.basename(trimmed_file_path)
					with open(trimmed_file_path) as file:
						file_dict = DictReaderInsensitive(file)
						#first simply check the file contains UIDs
						if request.form['submission_type'] == 'FB':
							required = set(['uid', 'trait', 'value', 'timestamp', 'person', 'location'])
							if not required.issubset(file_dict.fieldnames):
								return jsonify({"submitted": "This file is missing fields typical of a "
															 "FieldBook exported CSV file. "
															 "Please use check the submission type"
									})
						else: # request.form['submission_type'] == 'table':
							required = ['uid', 'date', 'time', 'person']
						if not set(required).issubset(file_dict.fieldnames):
							return jsonify({"submitted": "This table file appears to be missing a required field (UID, Date, Time, Person)"})
						# as an asynchonous function with celery, result will be stored in redis and accessible from the status/task_id endpoint
						task = async_submit.apply_async(args=[
							username,
							trimmed_filename,
							submission_type
						])
				except ServiceUnavailable:
					return jsonify({'submitted':'The database is currently unavailable - please try again later'})
				return jsonify({'submitted': ('Your file has been uploaded and will be merged into the database. '
						' <br><br> Depending on the size of the file this may take some time so you will receive an email including a summary of the update. '
						' <br><br> If you wait here you will also get feedback on this page.'), 
					'task_id': task.id })
			else:
				return jsonify({'submitted':'Please select a valid file'})
		else:
			return jsonify(form.errors)

@app.route('/status/<task_id>/')
def taskstatus(task_id):
	task = async_submit.AsyncResult(task_id)
	if task.status != 'SUCCESS':
		return jsonify({'status': task.status})
	else:
		result = task.get()
		if result['status'] == 'ERRORS':
			error_table = result['result'].html_table()
			return jsonify({
				'status': 'ERRORS',
				'result': error_table
			})
		else:
			return jsonify({'status': task.status, 'result':result})

