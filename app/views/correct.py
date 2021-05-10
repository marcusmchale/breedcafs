from app import app
from neo4j.exceptions import ServiceUnavailable, AuthError

from flask import (
	flash,
	redirect,
	url_for,
	render_template,
	session,
	request,
	jsonify
)

from app.models import (
	Upload,
	Resumable
)

from app.forms import (
	CorrectForm
)

from datetime import datetime, timedelta


@app.route('/correct', methods=['GET', 'POST'])
def correct():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		correct_form = CorrectForm()
		return render_template(
			'correct.html',
			title='Correct',
			correct_form=correct_form
		)


@app.route('/correct_submit', methods=['POST'])
def correct_submit():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		form = CorrectForm()
		if form.validate_on_submit():
			username = session['username']
			if 'global_admin' in session['access']:
				access = 'global_admin'
			elif 'partner_admin' in session['access']:
				access = 'partner_admin'
			elif 'user' in session['access']:
				access = 'user'
			submission_type = form.submission_type.data
			raw_filename = form.file.data.filename
			upload_object = Upload(username, submission_type, raw_filename)
			if not Resumable.allowed_file(raw_filename, submission_type=submission_type):
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
				task = Upload.async_correct.apply_async(args=[username, access, upload_object], serialiser='pickle')
			except (ServiceUnavailable, AuthError):
				return jsonify({'submitted': 'The database is currently unavailable - please try again later'})
			return jsonify({'submitted': (
				'Your file has been uploaded and is being processed.\n'
				'You will receive a report both here and via email that will either confirm '
				'deletion of all records contained in the submitted file '
				'or describe any issues that require your attention.'
			),
				'task_id': task.id})
		else:
			return jsonify(form.errors)
