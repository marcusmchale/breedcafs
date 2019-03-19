from app import (
	app,
	ServiceUnavailable,
	SecurityError
)

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
	Correct,
	Parsers,
	Download
)

from app.forms import (
	LocationForm,
	CorrectForm
)

from datetime import datetime, timedelta


@app.route('/correct', methods=['GET'])
def correct():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		correct_form = CorrectForm.update()
		location_form = LocationForm.update(optional=True)
		return render_template(
			'correct.html',
			title='Correct',
			correct_form=correct_form,
			location_form=location_form
		)


@app.route('/correct/list_records', methods=['POST'])
def delete_records():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			correct_form = CorrectForm.update()
			location_form = LocationForm.update(optional=True)
			if all([
				correct_form.validate_on_submit(),
				location_form.validate_on_submit()
			]):
				username = session['username']
				access = session['access']
				record_type = request.form['record_type'] if request.form['record_type'] != '' else None
				data_format = 'db'
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
					submission_start and submission_start >= submission_end,
					record_start and record_start >= record_end
				]):
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
					'selected_features': selected_features
				}
				correct_object = Correct(username, access)
				records = correct_object.collect_records(parameters)
				download_object = Download(username)
				return jsonify(download_object.records_to_file(records, data_format, file_type=file_type))
			else:
				errors = jsonify({
					'errors':[location_form.errors, correct_form.errors]
				})
				return errors
		except (ServiceUnavailable, SecurityError):
			flash("Database unavailable")
			return redirect(url_for('index'))