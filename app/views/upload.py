from app import app, ServiceUnavailable, SecurityError
from flask import (
	flash, redirect, url_for, session, render_template, jsonify
)
from app.models import Upload
from app.forms import UploadForm


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
				form=form,
				title='Upload'
			)
		except (ServiceUnavailable, SecurityError):
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
			submission_type = form.submission_type.data
			raw_filename = form.file.data.filename
			upload_object = Upload(username, submission_type, raw_filename)
			if not upload_object.allowed_file():
				return jsonify({'submitted': 'Please select a supported file type'})
			file_data = form.file.data
			upload_object.file_save(file_data)
			file_format_errors = upload_object.file_format_errors()
			if file_format_errors:
				return jsonify({
					'submitted': file_format_errors
				})
			# check for UID/person then time/date etc to set record_type
			try:
				# as an asynchronous function with celery
				# result is stored in redis and accessible from the status/task_id endpoint
				task = Upload.async_submit.apply_async(args=[username, upload_object])
			except (ServiceUnavailable, SecurityError):
				return jsonify({'submitted': 'The database is currently unavailable - please try again later'})
			return jsonify({'submitted': (
				'Your file has been uploaded and is being checked before submission to the database.\n'
				'You will receive a report both here and via email that will either confirm your submission '
				'or describe any issues that require your attention.'
			),
				'task_id': task.id})
		else:
			return jsonify(form.errors)


@app.route('/status/<task_id>/')
def task_status(task_id):
	task = Upload.async_submit.AsyncResult(task_id)
	if task.status != 'SUCCESS':
		return jsonify({'status': task.status})
	else:
		result = task.get()
		if result['status'] == 'ERRORS':
			if result['type'] == 'string':
				error_table = result['result']
			elif result['result'].duplicate_keys:
				error_table = result['result'].duplicate_keys_table()
			elif result['result'].errors:
				error_table = result['result'].html_table()
			elif result['result'].field_errors:
				errors_dict = result['result'].field_errors
				error_table = (
					'<p>The uploaded table includes the below unrecognised fields. ' 
					'Please check the spelling of any traits ' 
					'and ensure they are appropriate to the level of items included ' 
					'in this file:</p>'
				)
				for i in errors_dict:
					error_table += '<p> - ' + i + '</p>\n'
			else:
				error_table = None
			return jsonify({
				'status': 'ERRORS',
				'result': error_table
			})
		else:
			return jsonify({
				'status': task.status,
				'result': result
			})
