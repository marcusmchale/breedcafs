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
	send_from_directory,
	make_response
)
from app.models import (
	User,
	Download,
	SelectionList,
	FeatureList,
	Parsers
)
from app.forms import (
	DownloadForm,
	LocationForm
)
from app.emails import (
	send_email,
	send_static_attachment
)
from datetime import datetime, timedelta


@app.route("/feature_groups")
def feature_groups():
	item_level = request.args.get('item_level', None)
	record_type = request.args.get('record_type', None)
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			feature_groups_list = SelectionList.get_feature_groups(item_level, record_type)
			response = make_response(jsonify(feature_groups_list))
			response.content_type = 'application/json'
			return response
		except (ServiceUnavailable, SecurityError):
			flash("Database unavailable")
			return redirect(url_for('index'))


@app.route("/features")
def features():
	item_level = request.args.get('item_level', None)
	record_type = request.args.get('record_type', None)
	feature_group = request.args.get('feature_group', None)
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			features_details = FeatureList(item_level, record_type).get_features(feature_group=feature_group)
			response = make_response(jsonify(features_details))
			response.content_type = 'application/json'
			return response
		except (ServiceUnavailable, SecurityError):
			flash("Database unavailable")
			return redirect(url_for('index'))


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
				if any([ submission_start >= submission_end, record_start >= record_end]):
					return jsonify({
						'submitted': 'Please make sure the start date is before the end date'
					})
				item_level = request.form['item_level'] if request.form['item_level'] != '' else None
				country = request.form['country'] if request.form['country'] != '' else None
				region = request.form['region'] if request.form['region'] != '' else None
				farm = request.form['farm'] if request.form['farm'] != '' else None
				field_uid = int(request.form['field']) if request.form['field'].isdigit() else None
				block_uid = request.form['block'] if request.form['block'] != '' else None
				tree_id_list = (
					Parsers.parse_range_list(request.form['tree_id_list']) if request.form['tree_id_list'] != '' else None
				)
				sample_id_list = (
					Parsers.parse_range_list(request.form['sample_id_list']) if request.form['sample_id_list'] != '' else None
				)
				if 'select_features' in request.form:
					selected_features = request.form.getlist('select_features')
				else:
					selected_features = None
				download_filters = {
					'username': username,
					'submission_start': submission_start,
					'submission_end': submission_end,
					'record_start': record_start,
					'record_end': record_end,
					'item_level': item_level,
					'country': country,
					'region': region,
					'farm': farm,
					'field_uid': field_uid,
					'block_uid': block_uid,
					'tree_id_list': tree_id_list,
					'sample_id_list': sample_id_list,
					'selected_features': selected_features
				}
				download_object.collect_records(download_filters)





				# make the file and return file details
				file_details = Download(username).get_csv(
					country,
					region,
					farm,
					field_uid,
					block_uid,
					level,
					traits,
					data_format,
					start_time,
					end_time
				)
				# if result = none then no data was found
				if not file_details:
					return jsonify({'submitted': "No entries found that match your selection"})
				# create a download url
				download_url = url_for(
					'download_file',
					username = session['username'], 
					filename=file_details['filename'],
					_external = True
				)
				# if request.form.get(level + '-email_checkbox'):
				if request.form.get('block-email_checkbox'):
					recipients = [User(session['username']).find('')['email']]
					subject = "BreedCAFS: Data file generated"
					body = (
							'You requested data from the BreedCAFS database. '
							'The file is attached (if less than 5mb) and available at the following address: '
							+ download_url
					)
					html = render_template(
						'emails/data_file.html',
						download_url = download_url
					)
					if file_details['file_size'] < 5000000:
						send_static_attachment(
							subject,
							app.config['ADMINS'][0],
							recipients, 
							body, 
							html,
							file_details['filename'],
							'text/csv',
							file_details['file_path']
						)
						return jsonify({
							'submitted': (
								'Your file is ready for download: '
								'"<a href="' + download_url + '">' + file_details['filename'] + '</a>"'
								' A copy of this file has been sent to your email address'
							)
						})
					else:
						send_email(
							subject,
							app.config['ADMINS'][0],
							recipients, 
							body, 
							html
						)
						return jsonify({
							'submitted': (
								'Your file is ready for download: '
								'"<a href="' + download_url + '">' + file_details['filename'] + '</a>"'
								' A copy of this link has been sent to your email address'
							)
						})
				else:
					return jsonify({
						'submitted': (
							'Your file is ready for download: '
							'"<a href="' + download_url + '">' + file_details['filename'] + '</a>"'
						)
					})
			else:
				errors = jsonify([location_form.errors, download_form.errors])
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
			if os.path.isfile(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], username, filename)):
				return send_from_directory(
					os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], username),
					filename,
					as_attachment = True
				)
			else:
				flash('File no longer exists on the server, please generate a new file for download')
				return redirect(url_for('download'))
		except (ServiceUnavailable, SecurityError):
			flash("Database unavailable")
			return redirect(url_for('index'))
