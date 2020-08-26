from dbtools import app, logging

from flask import (
	redirect,
	url_for,
	render_template,
	flash
)

from itsdangerous import URLSafeTimedSerializer

from dbtools.models.forms import (
	RegistrationForm
)


from dbtools.views.emails import send_email

from .custom_decorators import redis_required, neo4j_required


@app.route('/register', methods=['GET', 'POST'])
@neo4j_required
@redis_required
def register():
	try:
		form = RegistrationForm.update()
		if form.validate_on_submit():
			username = form.username.data
			password = form.password.data
			email = form.email.data.lower()
			name = form.name.data
			partner = form.partner.data

			# get list of allowed emails


			allowed_emails = get_all_allowed_emails()
			if email not in allowed_emails:
				flash('This email is not registered, please contact your BreedCAFS partner administrator.')
				return redirect(url_for('register'))
			existing_user = get_user_properties(User(username).username)
			if existing_user:
				if existing_user['confirmed']:
					flash('That username is already registered and confirmed')
					return redirect(url_for('register'))
				if existing_user['time'] - time.time()  > 8.64e+7 :
					flash('That username is already registered and confirmed')
					return redirect(url_for('register'))
				else:
					flash('Username was previously registered but is not yet confirmed.')
					if User(username).delete_user(email):
						flash('Un-confirmed user successfully removed')
					else:
						flash('Existing user cannot be removed, please contact an administrator')
						return redirect(url_for('register'))
			try:
				ts = URLSafeTimedSerializer(app.config['SECRET_KEY'])
				register(User(username).username, password, email, name, partner)
				token = ts.dumps(email, salt=app.config['CONFIRM_EMAIL_SALT'])
				subject = "BreedCAFS database confirmation."
				recipients = [email]
				confirm_url = url_for('confirm', token=token, _external = True)
				body = (
					"Your account on the BreedCAFS server was successfully created. Your username is" + username +
					" Please visit the following link to activate your account:" + confirm_url
				)
				html = render_template('emails/activate.html', confirm_url=confirm_url, username=username)
				send_email(subject, app.config['ADMINS'][0], recipients, body, html)
				flash('Registration successful. Please check your email to confirm.')
				return redirect(url_for('login'))
			except Exception as e:
				logging.info('Error registering user:' + str(e))
				flash('Error with registration please contact an administrator')
		return render_template('register.html', form=form, title='Register')
	except (ServiceUnavailable, SecurityError):
		flash("Database unavailable")
		return redirect(url_for('index'))

	@app.route('/confirm/<token>', methods=['GET', 'POST'])
	def confirm(token):
		try:
			try:
				ts = URLSafeTimedSerializer(app.config['SECRET_KEY'])
				email = ts.loads(token, salt=app.config['CONFIRM_EMAIL_SALT'], max_age=86400)
				User.set_user_confirmed(email)
				flash('Email confirmed')
				return redirect(url_for('login'))
			except Exception as e:
				logging.info('Error with user confirmation (email): ' + str(e))
				flash('Please register again and confirm within 24hrs')
				return redirect(url_for('register'))
		except (ServiceUnavailable, SecurityError):
			flash("Database unavailable")
			return redirect(url_for('index'))

		@app.route('/password_reset', methods=['GET', 'POST'])
		def password_reset():
			try:
				# get list of allowed emails
				allowed_emails = get_all_allowed_emails()
				form = PasswordResetRequestForm()
				if form.validate_on_submit():
					email = form.email.data.lower()
					if email not in allowed_emails:
						flash('This email is not registered')
						return redirect(url_for('password_reset'))
					user = User("").find(email)
					if user:
						if user['confirmed']:
							name = user['name']
							username = user['username']
							ts = URLSafeTimedSerializer(app.config['SECRET_KEY'])
							token = ts.dumps(email, salt=app.config["PASSWORD_RESET_SALT"])
							subject = "BreedCAFS database password reset request"
							recipients = [email]
							confirm_url = url_for('confirm_password_reset', token=token, _external=True)
							body = (
									"Hi " + name +
									". Someone recently requested to reset the password "
									"for your user account for the BreedCAFS database."
									" If you would like to proceed then please visit the following address: " + confirm_url +
									" As a reminder, your username for this account is " + username
							)
							html = render_template(
								'emails/password_reset.html',
								confirm_url=confirm_url,
								username=username,
								name=name
							)
							send_email(subject, app.config['ADMINS'][0], recipients, body, html)
							flash('Please check your email to confirm password reset')
							return redirect(url_for('login'))
						else:
							flash('User is not confirmed - please register and confirm')
					else:
						flash('User is not registered')
				return render_template('password_reset.html', form=form, title='Password reset')
			except (ServiceUnavailable, SecurityError):
				flash("Database unavailable")
				return redirect(url_for('index'))

		@app.route('/confirm_password_reset/<token>', methods=['GET', 'POST'])
		def confirm_password_reset(token):
			try:
				try:
					ts = URLSafeTimedSerializer(app.config['SECRET_KEY'])
					email = ts.loads(token, salt=app.config["PASSWORD_RESET_SALT"], max_age=86400)
				except Exception as e:
					logging.info("Error in confirm password reset:" + str(e))
					flash('Please submit a new request and confirm within 24hrs')
					return redirect(url_for('password_reset'))
				form = PasswordResetForm()
				if form.validate_on_submit():
					User.set_password(email, form.password.data)
					flash('Your password has been reset')
					return redirect(url_for('login'))
				return render_template(
					'confirm_password_reset.html',
					form=form,
					token=token
				)
			except (ServiceUnavailable, SecurityError):
				flash("Database unavailable")
				return redirect(url_for('index'))