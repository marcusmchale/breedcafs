import os
from app import (
	app,
	ServiceUnavailable,
	SecurityError
)
from flask import (
	redirect,
	flash,
	url_for, 
	request, 
	session, 
	render_template, 
	jsonify,
	send_from_directory
)
from app.models import (
	Download,
	Parsers
)
from app.forms import (
	DownloadForm,
	LocationForm
)

from datetime import datetime, timedelta


@app.route('/download', methods=['GET', 'POST'])
def download():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:	
			location_form = LocationForm.update(optional=True)
			download_form = DownloadForm.update()
			return render_template(
				'download.html',
				download_form=download_form,
				location_form=location_form,
				title='Download'
			)
		except (ServiceUnavailable, SecurityError):
			flash("Database unavailable")
			return redirect(url_for('index'))


@app.route('/download/files/', methods=['GET'])
def download_files():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			return render_template(
				'download_files.html',
				title='Download Files'
			)
		except (ServiceUnavailable, SecurityError):
			flash("Database unavailable")
			return redirect(url_for('index'))


@app.route('/download/files/list', methods=['GET', 'POST'])
def download_files_list():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			path = os.path.join(app.config['DOWNLOAD_FOLDER'], session['username'])
			file_list = os.listdir(path)
			file_list.sort(key=lambda x: os.path.getmtime(os.path.join(path, x)), reverse=True)
			html_file_table = (
				'<table>'
				+ '	<tr>'
				+ '		<th>Database generated files</th> '
				+ '	</tr> '
			)
			for i in file_list:
				html_file_table += (
					'<tr>'
					+ '<td><a href=" '
					+ url_for(
						"download_file",
						username=session['username'],
						filename=i,
						_external=True
					)
					+ '">'
					+ str(i) + '</a></td>'
					+ '</tr> '
				)
			html_file_table += '</table>'
			return jsonify(
				html_file_table
			)
		except (ServiceUnavailable, SecurityError):
			flash("Database unavailable")
			return redirect(url_for('index'))


@app.route('/download/generate_file', methods=['POST'])
def generate_file():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			download_form = DownloadForm.update()
			location_form = LocationForm.update(optional=True)
			if all([
				download_form.validate_on_submit(),
				location_form.validate_on_submit()
			]):
				username = session['username']
				download_object = Download(username)
				record_type = request.form['record_type'] if request.form['record_type'] != '' else None
				data_format = request.form['data_format']
				file_type = request.form['file_type']
				# collect the filters into a dictionary to pass as parameters
				submission_start = int(
					(
						datetime.strptime(request.form['submission_date_from'], '%Y-%m-%d')
						- datetime(1970, 1, 1)
					).total_seconds() * 1000
				) if request.form['submission_date_from'] != '' else None
				submission_end = int(
					(
						datetime.strptime(request.form['submission_date_to'], '%Y-%m-%d')
						+ timedelta(days=1)
						- datetime(1970, 1, 1)
					).total_seconds() * 1000
				) if request.form['submission_date_to'] != '' else None
				record_start = int(
					(
						datetime.strptime(request.form['record_date_from'], '%Y-%m-%d')
						- datetime(1970, 1, 1)
					).total_seconds() * 1000
				) if request.form['record_date_from'] != '' else None
				record_end = int(
					(
						datetime.strptime(request.form['record_date_to'], '%Y-%m-%d')
						+ timedelta(days=1)
						- datetime(1970, 1, 1)
					).total_seconds() * 1000
				) if request.form['record_date_to'] != '' else None
				# sanity check on start and end
				if any([
					((submission_start and submission_end) and (submission_start >= submission_end)),
					((record_start and record_end) and (record_start >= record_end))
				]):
					return jsonify({
						'result': 'Please make sure the start date is before the end date'
					})
				item_level = request.form['item_level'] if request.form['item_level'] != '' else None
				country = request.form['country'] if request.form['country'] != '' else None
				region = request.form['region'] if request.form['region'] != '' else None
				farm = request.form['farm'] if request.form['farm'] != '' else None
				field_uid = int(request.form['field']) if request.form['field'].isdigit() else None
				block_uid = request.form['block'] if request.form['block'] != '' else None
				tree_id_list = (
					Parsers.parse_range_list(
						request.form['tree_id_list']
					) if request.form['tree_id_list'] != '' else None
				)
				sample_id_list = (
					Parsers.parse_range_list(
						request.form['sample_id_list']
					) if request.form['sample_id_list'] != '' else None
				)
				replicate_id_list = (
					Parsers.parse_range_list(
						request.form['replicate_id_list']
					) if request.form['replicate_id_list'] != '' else None
				)
				selected_inputs = request.form.getlist('inputs') if 'inputs' in request.form else None
				parameters = {
					'username': username,
					'submission_start': submission_start,
					'submission_end': submission_end,
					'record_start': record_start,
					'record_end': record_end,
					'item_level': item_level,
					'record_type': record_type,
					'country': country,
					'region': region,
					'farm': farm,
					'field_uid': field_uid,
					'block_uid': block_uid,
					'tree_id_list': tree_id_list,
					'sample_id_list': sample_id_list,
					'replicate_id_list': replicate_id_list,
					'selected_inputs': selected_inputs
				}
				records = download_object.collect_records(parameters, data_format)
				return jsonify(download_object.records_to_file(records, data_format, file_type))
			else:
				errors = jsonify({
					'errors':[location_form.errors, download_form.errors]
				})
				return errors
		except (ServiceUnavailable, SecurityError):
			flash("Database unavailable")
			return redirect(url_for('index'))


@app.route('/download/<username>/<filename>', methods=['GET', 'POST'])
def download_file(username, filename):
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	elif username != session['username']:
		flash('Files are currently only accessible to the user that generated them')
		return redirect(url_for('index'))
	else:
		try:
			if os.path.isfile(os.path.join(app.config['DOWNLOAD_FOLDER'], username, filename)):
				return send_from_directory(
					os.path.join(app.config['DOWNLOAD_FOLDER'], username),
					filename,
					as_attachment = True
				)
			else:
				flash('File no longer exists on the server, please generate a new file for download')
				return redirect(url_for('download'))
		except (ServiceUnavailable, SecurityError):
			flash("Database unavailable")
			return redirect(url_for('index'))
