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
	jsonify
)

from app.forms import (
	LocationForm,
	RecordForm,
	WeatherForm
)


@app.route('/record', methods=['GET', 'POST'])
def record():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			record_form = RecordForm.update()
			location_form = LocationForm.update()
			weather_form = WeatherForm()
			return render_template(
				'record.html',
				title='Record',
				record_form=record_form,
				location_form=location_form,
				weather_form=weather_form
			)
		except (ServiceUnavailable, AuthError):
			flash("Database unavailable")
			return redirect(url_for('index'))


@app.route('/record/weather', methods=['POST'])
def weather():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			record_form = RecordForm.update()
			location_form = LocationForm.update(optional=True)
			weather_form = WeatherForm()
			# need to allow other levels of optional, at farm level for example
			if all([record_form.validate_on_submit(), weather_form.validate_on_submit(), location_form.validate_on_submit()]):

				return jsonify({'submitted': 'nothing actually done'})
			else:
				errors = jsonify([record_form.errors, weather_form.errors, location_form.errors])
				return errors
		except (ServiceUnavailable, AuthError):
			flash("Database unavailable")
			return redirect(url_for('index'))