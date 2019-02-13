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
	TrialForm,
	AddTrial
)


@app.route('/trials', methods=['GET', 'POST'])
def trials():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			location_form = LocationForm.update()
			trial_form = TrialForm.update()
			add_trial_form = AddTrial()
			return render_template(
				'trials.html',
				title='Trials',
				location_form=location_form,
				trial_form=trial_form,
				add_trial_form=add_trial_form
			)
		except (ServiceUnavailable, SecurityError):
			flash("Database unavailable")
			return redirect(url_for('index'))
