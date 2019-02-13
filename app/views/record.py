from app import (
	app,
	ServiceUnavailable,
	SecurityError
)

from flask import (
	session,
	flash,
	redirect,
	url_for,
	render_template,
	jsonify,
	request,
	make_response
)

from app.forms import (
	LocationForm,
	RecordForm,
)

from app.models import (
	SelectionList,
	Record,
	FeatureList,
	Parsers,
	Download,
	User
)

from app.emails import send_email

from flask.views import MethodView

from datetime import datetime, timedelta


class ListFeatureGroups(MethodView):
	def get(self, record_type, item_level):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				feature_groups = SelectionList.get_feature_groups(item_level, record_type)
				response = make_response(jsonify(feature_groups))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable, SecurityError):
				flash("Database unavailable")
				return redirect(url_for('index'))


class ListFeatures(MethodView):
	def get(self, record_type, item_level, feature_group):
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


@app.route('/record', methods=['GET', 'POST'])
def record():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			record_form = RecordForm().update()
			location_form = LocationForm.update(optional=True)
			return render_template(
				'record.html',
				title='Record',
				record_form=record_form,
				location_form=location_form
			)
		except (ServiceUnavailable, SecurityError):
			flash("Database unavailable")
			return redirect(url_for('index'))


@app.route('/record/generate_template', methods=['POST'])
def generate_template():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		record_form = RecordForm.update()
		location_form = LocationForm.update(optional=True)
		try:
			if all([
				record_form.validate_on_submit(),
				location_form.validate_on_submit()
			]):
				record_type = request.form['record_type'] if request.form['record_type'] != '' else None
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
				features_dict = {}
				for feature in selected_features:
					features_dict[feature] = request.form[feature]
				record_data = {
					'record_type': record_type,
					'item_level': item_level,
					'country': country,
					'region': region,
					'farm': farm,
					'field_uid': field_uid,
					'block_uid': block_uid,
					'tree_id_list': tree_id_list,
					'sample_id_list': sample_id_list,
					'selected_features': selected_features,
					'features_dict': features_dict
				}
				download_object = Download(session['username'])
				if download_object.record_form_to_template(record_data):
					file_list = download_object.file_list
					file_list_html = download_object.get_file_list_html()
					if request.form.get('email_checkbox'):
						recipients = [User(session['username']).find('')['email']]
						subject = "BreedCAFS files requested"
						body = (
								" You recently requested a template from the BreedCAFS database. "
								" A spreadsheet file (.xlsx) is attached containing a 'Template' sheet "
								" for data recording. Enter data into this spreadsheet and and upload "
								" the file on the 'Upload' page to record data corresponding to the listed items. "
								" The template file is available at the following address: "
								+ file_list_html
						)
						html = render_template(
							'emails/generate_files.html',
							file_list=[i['url'] for i in file_list]
						)
						send_email(
							subject,
							app.config['ADMINS'][0],
							recipients,
							body,
							html
						)
						return jsonify(
							{
								'submitted': (
										' Your template is available for download. '
										' A link to this file has been sent to your email address:'
										+ file_list_html
								)
							}
						)
					return jsonify(
						{
							'submitted': (
									' Your template is available'
									' for download. '
									+ file_list_html
							)
						}
					)
				else:
					return jsonify({
						'submitted': 'No items found that match your selection'
					})
			else:
				return jsonify({
					'errors': [record_form.errors, location_form.errors]
				})
		except (ServiceUnavailable, SecurityError):
			flash("Database unavailable")
			return redirect(url_for('index'))


@app.route('/record/submit_records', methods=['POST'])
def submit_records():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		record_form = RecordForm.update(web_form=True)
		location_form = LocationForm.update(optional=True)
		try:
			if all([
				record_form.validate_on_submit(),
				location_form.validate_on_submit()
			]):
				record_type = request.form['record_type'] if request.form['record_type'] != '' else None
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
				record_time = int(
					(
						datetime.strptime(request.form['record_time'], '%Y-%m-%d') +
						timedelta(hours=12) -
						datetime(1970, 1, 1)
					).total_seconds() * 1000
				) if request.form['record_time'] != '' else None
				start_time = int(
					(datetime.strptime(request.form['record_start'], '%Y-%m-%d') - datetime(1970, 1, 1)).total_seconds()
					* 1000
				) if request.form['record_start'] != '' else None
				# end time defaults to last minute of the end date
				end_time = int(
					(
						datetime.strptime(request.form['record_end'], '%Y-%m-%d') +
						timedelta(days=1) -
						datetime(1970, 1, 1)
					).total_seconds() * 1000
				) - 60000 if request.form['record_end'] != '' else None
				if all([record_type == 'condition', end_time, start_time >= end_time]):
					return jsonify({
						'submitted': 'Please make sure the start date is before the end date'
					})
				if 'select_features' in request.form:
					selected_features = request.form.getlist('select_features')
				else:
					selected_features = None
				features_dict = {}
				for feature in selected_features:
					features_dict[feature] = request.form[feature]
				record_data = {
					'record_type': record_type,
					'item_level': item_level,
					'country': country,
					'region': region,
					'farm': farm,
					'field_uid': field_uid,
					'block_uid': block_uid,
					'tree_id_list': tree_id_list,
					'sample_id_list': sample_id_list,
					'record_time': record_time,
					'start_time': start_time,
					'end_time': end_time,
					'selected_features': selected_features,
					'features_dict': features_dict
				}
				result = Record(session['username']).submit_records(record_data)
				return result
			else:
				errors = jsonify({
					'errors': [record_form.errors, location_form.errors]
				})
				return errors
		except (ServiceUnavailable, SecurityError):
			flash("Database unavailable")
			return redirect(url_for('index'))
