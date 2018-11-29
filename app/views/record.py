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
	#WeatherForm,
	#ControlledEnvironmentForm,
	#@TreatmentForm
)

from app.models import (
	SelectionList,
	Record,
	ConditionsList
)

from flask.views import MethodView

from datetime import datetime


class ListConditionGroups(MethodView):
	def get(self, level):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				condition_groups = SelectionList.get_condition_groups(level)
				response = make_response(jsonify(condition_groups))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable, AuthError):
				flash("Database unavailable")
				return redirect(url_for('index'))


class ListConditionsDetails(MethodView):
	def get(self, level, group):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				conditions_details = ConditionsList.get_conditions_details(level, group)
				response = make_response(jsonify(conditions_details))
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
			record_form = RecordForm.update()
			location_form = LocationForm.update()
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
		location_form = LocationForm.update()
		try:
			if all([
				record_form.validate_on_submit(),
				location_form.validate_on_submit()
			]):
				print('start of submit endpoint just after validation')
				import pdb;
				pdb.set_trace()
				field_uid = int(request.form['field']) if request.form['field'].isdigit() else None
				block_uid = request.form['block'] if request.form['block'] != '' else None
				trees_start = int(
					request.form['trees_start']
					) if request.form['trees_start'].isdigit() else None
				trees_end = int(
					request.form['trees_end']
					) if request.form['trees_end'].isdigit() else None


				treatment_name = request.form['treatment_name']
				treatment_category = request.form['treatment_category']


				result = Record(session['username']).submit_records(
					field_uid,
					block_uid,
					trees_start,
					trees_end,


					treatment_name,
					treatment_category
				)
				return result
			else:
				errors = jsonify([record_form.errors, location_form.errors])
				return errors
		except (ServiceUnavailable, AuthError):
			flash("Database unavailable")
			return redirect(url_for('index'))


#@app.route('/record/weather', methods=['POST'])
#def weather():
#	if 'username' not in session:
#		flash('Please log in')
#		return redirect(url_for('login'))
#	else:
#		try:
#			record_form = RecordForm()
#			location_form = LocationForm.update()
#			weather_form = WeatherForm()
#			# need to allow other levels of optional, at farm level for example
#			if all([
#				record_form.validate_on_submit(),
#				weather_form.validate_on_submit(),
#				location_form.validate_on_submit(),
#			]):
#				field_uid = int(request.form['field']) if request.form['field'].isdigit() else None
#				start_time = (
#					int((datetime.strptime(request.form['weather_start'], '%Y-%m-%d')
#						- datetime(1970, 1, 1)).total_seconds() * 1000)
#					if request.form['weather_start'] != '' else None
#				)
#				# end time is the last millisecond of the end date
#				end_time = (
#					int((datetime.strptime(request.form['weather_end'], '%Y-%m-%d')
#						- datetime(1969, 12, 31)).total_seconds() * 1000) - 1
#					if request.form['weather_end'] != '' else None
#				)
#				wind_speed_max = (
#					float(request.form['wind_speed_max'])
#					if request.form['wind_speed_max'].replace('.', '', 1).isdigit() else None
#				)
#				wind_direction = (
#					request.form['wind_direction']
#					if request.form['wind_direction'] != '' else None
#				)
#				temperature_min = (
#					float(request.form['temperature_min'])
#					if request.form['temperature_min'].replace('.', '', 1).isdigit() else None
#				)
#				temperature_max = (
#					float(request.form['temperature_max'])
#					if request.form['temperature_max'].replace('.', '', 1).isdigit() else None
#				)
#				solar_radiation = (
#					float(request.form['solar_radiation'])
#					if request.form['solar_radiation'].replace('.', '', 1).isdigit() else None
#				)
#				rainfall = (
#					float(request.form['rainfall'])
#					if request.form['rainfall'].replace('.', '', 1).isdigit() else None
#				)
#				humidity = (
#					float(request.form['humidity'])
#					if request.form['humidity'].replace('.', '', 1).isdigit() else None
#				)
#				if start_time >= end_time:
#					return jsonify({
#						'submitted': 'Please make sure the start date is before the end date'
#					})
#				result = Record(session['username']).weather(
#					field_uid,
#					start_time,
#					end_time,
#					wind_speed_max,
#					wind_direction,
#					temperature_min,
#					temperature_max,
#					solar_radiation,
#					rainfall,
#					humidity
#				)
#				return result
#			else:
#				errors = jsonify([record_form.errors, weather_form.errors, location_form.errors])
#				return errors
#		except (ServiceUnavailable, AuthError):
#			flash("Database unavailable")
#			return redirect(url_for('index'))
#
#
#@app.route('/record/controlled_environment', methods=['POST'])
#def controlled_environment():
#	if 'username' not in session:
#		flash('Please log in')
#		return redirect(url_for('login'))
#	else:
#		try:
#			record_form = RecordForm()
#			location_form = LocationForm.update()
#			controlled_environment_form = ControlledEnvironmentForm()
#			# need to allow other levels of optional, at farm level for example
#			if all([
#				record_form.validate_on_submit(),
#				controlled_environment_form.validate_on_submit(),
#				location_form.validate_on_submit(),
#			]):
#				field_uid = int(request.form['field']) if request.form['field'].isdigit() else None
#				block_uid = request.form['block'] if request.form['block'] != '' else None
#				start_time = (
#					int((datetime.strptime(request.form['controlled_environment_start'], '%Y-%m-%d')
#						- datetime(1970, 1, 1)).total_seconds() * 1000)
#					if request.form['controlled_environment_start'] != '' else None
#				)
#				# end time is the last millisecond of the end date.
#				end_time = (
#					int((datetime.strptime(request.form['controlled_environment_end'], '%Y-%m-%d')
#						- datetime(1969, 12, 31)).total_seconds() * 1000) - 1
#					if request.form['controlled_environment_end'] != '' else None
#				)
#				day_length = (
#					float(request.form['day_length'])
#					if request.form['day_length'].replace('.', '', 1).isdigit() else None
#				)
#				night_length = (
#					float(request.form['night_length'])
#					if request.form['night_length'].replace('.', '', 1).isdigit() else None
#				)
#				temperature_day = (
#					float(request.form['temperature_day'])
#					if request.form['temperature_night'].replace('.', '', 1).isdigit() else None
#				)
#				temperature_night = (
#					float(request.form['temperature_night'])
#					if request.form['temperature_night'].replace('.', '', 1).isdigit() else None
#				)
#				humidity = (
#					float(request.form['humidity'])
#					if request.form['humidity'].replace('.', '', 1).isdigit() else None
#				)
#				par = (
#					float(request.form['par'])
#					if request.form['par'].replace('.', '', 1).isdigit() else None
#				)
#				carbon_dioxide = (
#					float(request.form['carbon_dioxide'])
#					if request.form['carbon_dioxide'].replace('.', '', 1).isdigit() else None
#				)
#				if start_time >= end_time:
#					return jsonify({
#						'submitted': 'Please make sure the start date is before the end date'
#					})
#				result = Record(session['username']).controlled_environment(
#					field_uid,
#					block_uid,
#					start_time,
#					end_time,
#					day_length,
#					night_length,
#					temperature_day,
#					temperature_night,
#					humidity,
#					par,
#					carbon_dioxide
#				)
#				return result
#			else:
#				errors = jsonify([record_form.errors, controlled_environment_form.errors, location_form.errors])
#				return errors
#		except (ServiceUnavailable, AuthError):
#			flash("Database unavailable")
#			return redirect(url_for('index'))
