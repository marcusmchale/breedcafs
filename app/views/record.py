from app import (
	app,
	ServiceUnavailable,
	AuthError
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
	FeaturesList,
	Parsers
)

from flask.views import MethodView

from datetime import datetime, timedelta


class ListFeatureGroups(MethodView):
	def get(self, data_type, level):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				feature_groups = SelectionList.get_feature_groups(data_type, level)
				response = make_response(jsonify(feature_groups))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable, AuthError):
				flash("Database unavailable")
				return redirect(url_for('index'))


class ListFeaturesDetails(MethodView):
	def get(self, data_type, level, group):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				features_details = FeaturesList.get_features_details(data_type, level, group)
				response = make_response(jsonify(features_details))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable, AuthError):
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
		except (ServiceUnavailable, AuthError):
			flash("Database unavailable")
			return redirect(url_for('index'))


@app.route('/record/submit_records', methods=['POST'])
def submit_records():
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
				data_type = request.form['data_type'] if request.form['data_type'] != '' else None
				level = request.form['level'] if request.form['level'] != '' else None
				country = request.form['country'] if request.form['country'] != '' else None
				region = request.form['region'] if request.form['region'] != '' else None
				farm = request.form['farm'] if request.form['farm'] != '' else None
				field_uid = int(request.form['field']) if request.form['field'].isdigit() else None
				block_uid = request.form['block'] if request.form['block'] != '' else None
				tree_id_list = (
					Parsers.parse_range_list(request.form['tree_id_list']) if request.form['tree_id_list'] != '' else None
				)
				record_time = int(
					(datetime.strptime(request.form['record_time'], '%Y-%m-%d') - datetime(1970, 1, 1)).total_seconds()
					* 1000
				) if request.form['record_time'] != '' else None
				start_time = int(
					(datetime.strptime(request.form['record_start'], '%Y-%m-%d') - datetime(1970, 1, 1)).total_seconds()
					* 1000
				) if request.form['record_start'] != '' else None
				# end time is the last millisecond of the end date
				end_time = int(
					(
						datetime.strptime(request.form['record_end'], '%Y-%m-%d') +
						timedelta(days=1) -
						datetime(1970, 1, 1)
					).total_seconds() * 1000
				) - 1 if request.form['record_end'] != '' else None
				if all([data_type == 'condition', end_time, start_time >= end_time]):
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
					'data_type': data_type,
					'level': level,
					'country': country,
					'region': region,
					'farm': farm,
					'field_uid': field_uid,
					'block_uid': block_uid,
					'tree_id_list': tree_id_list,
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
		except (ServiceUnavailable, AuthError):
			flash("Database unavailable")
			return redirect(url_for('index'))
