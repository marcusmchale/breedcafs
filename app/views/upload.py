from app import app, ServiceUnavailable, AuthError
from flask import ( flash, redirect, url_for, session, render_template, jsonify)
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
				form = form,
				title = 'Upload'
			)
		except (ServiceUnavailable, AuthError):
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
			if not Upload.allowed_file(form.file.data.filename):
				return jsonify({'submitted': 'Please select a supported file type'})
			username = session['username']
			submission_type = form.submission_type.data
			raw_filename = form.file.data.filename
			file_data = form.file.data
			upload_object = Upload(username, submission_type, raw_filename)
			upload_object.file_save(file_data)
			if not upload_object.is_valid_csv():
				return jsonify({
					'submitted': 'Please upload comma (,) separated file with quoted (") fields'
				})
			header_report = upload_object.check_headers()
			if header_report:
				return jsonify({
					'submitted': header_report
				})
			try:
				# as an asynchronous function with celery
				# result is stored in redis and accessible from the status/task_id endpoint
				task = Upload.async_submit.apply_async(args = [username, upload_object])
			except (ServiceUnavailable, AuthError):
				return jsonify({'submitted':'The database is currently unavailable - please try again later'})
			return jsonify({'submitted': (
				'Your file has been uploaded and is being checked before submission to the database.\n'
				'You will receive a report both here and via email that will either confirm your submission '
				'or describe any issues that require your attention.'
			),
				'task_id': task.id })
		else:
			return jsonify(form.errors)

@app.route('/status/<task_id>/')
def taskstatus(task_id):
	task = Upload.async_submit.AsyncResult(task_id)
	if task.status != 'SUCCESS':
		return jsonify({'status': task.status})
	else:
		result = task.get()
		if result['status'] == 'ERRORS':
			if result['result'].duplicate_keys_dict():
				error_table = result['result'].duplicate_keys_table()
			elif result['result'].row_errors():
				error_table = result['result'].html_table()
			elif result['result'].field_errors_dict():
				errors_dict = result['result'].field_errors_dict()
				error_table = '<p>The uploaded table includes the below unrecognised fields. ' \
							'Please check the spelling of any traits ' \
							'and ensure they are appropriate to the level of items included ' \
							'in this file:</p>'
				for i in errors_dict:
					error_table += '<p> - ' + str(i) + '</p>\n'
			return jsonify({
				'status': 'ERRORS',
				'result': error_table
			})
		else:
			return jsonify({
				'status': task.status,
				'result': result
			})

