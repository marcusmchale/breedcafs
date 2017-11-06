from app import app, ServiceUnavailable
from flask import request, session, redirect, url_for, render_template, flash
from itsdangerous import URLSafeTimedSerializer
from app.models import User
from app.forms import RegistrationForm, LoginForm, PasswordResetRequestForm, PasswordResetForm
from app.emails import send_email

#token generation
@app.route('/register', methods=['GET','POST'])
def register():
	try:
		form=RegistrationForm().update()
		if form.validate_on_submit():
			username = form.username.data.lower()
			password = form.password.data
			email = form.email.data.lower()
			name = form.name.data
			partner = form.partner.data
			if not email in app.config['ALLOWED_EMAILS']:
				flash('This email is not registered with BreedCAFS.')
				return redirect(url_for('register'))
			if User(username).find(email):
				if User(username).check_confirmed(email):
					flash('That username and/or email is already registered and confirmed')
					return redirect(url_for('register'))
				else:
					flash('Username and/or email was previously registered but not confirmed.')
					if User(username).remove(email):
						flash('Un-confirmed user successfully removed')
					else: 
						flash('Existing user cannot be removed, please contact an administrator')
						return redirect(url_for('register'))
			try:
				ts = URLSafeTimedSerializer(app.config['SECRET_KEY'])
				User(username).register(password, email, name, partner)
				token = ts.dumps(email, salt=app.config['CONFIRM_EMAIL_SALT'])
				subject = "BreedCAFS database confirmation."
				recipients = [email]
				confirm_url = url_for('confirm',token=token,_external=True)
				body = ("Your account on the BreedCAFS server was successfully created. Your username is" + username +
				" Please visit the following link to activate your account:" + confirm_url)
				html = render_template('emails/activate.html', confirm_url=confirm_url, username = username)
				send_email(subject, app.config['ADMINS'][0], recipients, body, html )
				flash('Registration successful. Please check your email to confirm.')
				return redirect(url_for('login'))
			except:
				flash('Error with registration please contact an administrator')
		return render_template('register.html', form=form, title='Register') 
	except (ServiceUnavailable):
		flash("Database unavailable")
		return redirect(url_for('index'))

@app.route('/confirm/<token>', methods=['GET', 'POST'])
def confirm(token):
	try:
		try:
			ts = URLSafeTimedSerializer(app.config['SECRET_KEY'])
			email = ts.loads(token,  salt=app.config['CONFIRM_EMAIL_SALT'], max_age=86400)
			User.confirm_email(email)
			flash('Email confirmed')
			return redirect(url_for('login'))
		except:
			flash('Please register again and confirm within 24hrs')
			return redirect(url_for('register'))
	except (ServiceUnavailable):
		flash("Database unavailable")
		return redirect(url_for('index'))

@app.route('/login', methods=['GET', 'POST'])
def login():
	try: 
		form = LoginForm()
		if form.validate_on_submit():
			username = form.username.data.lower()
			password = form.password.data
	# username 'start' is created when initialiseDB is run, just hiding it here.
			if username == 'start':
				flash ('Username not registered')
				return redirect(url_for('index')) 
			if not User(username).find(''):
				flash ('Username not registered')
				return redirect(url_for('index'))
			if not User(username).check_confirmed(''):
				flash ('Please check your email to confirm registration')
				return redirect(url_for('login'))
			if not User(username).verify_password(password):
				flash('Please check your password')
				return redirect(url_for('login'))
			else:
				session['username'] = username
				flash('Logged in.')
				return redirect(url_for('index'))
		return render_template('login.html', form=form, title='Login')
	except (ServiceUnavailable):
		flash("Database unavailable")
		return redirect(url_for('index'))

@app.route('/logout', methods=['GET'])
def logout():
	session.pop('username', None)
	flash('Logged out.')
	return redirect(url_for('index'))

@app.route('/password_reset', methods=['GET', 'POST'])
def password_reset():
	try:
		form=PasswordResetRequestForm()
		if form.validate_on_submit():
			email = form.email.data.lower()
			if not email in app.config['ALLOWED_EMAILS']:
				flash('This email is not registered')
				return redirect(url_for('password_reset'))
			user = User("").find(email)
			if user['confirmed'] == True:
				name = user['name']
				username = user['username']
				ts = URLSafeTimedSerializer(app.config['SECRET_KEY'])
				token = ts.dumps(email, salt=app.config["PASSWORD_RESET_SALT"])
				subject = "BreedCAFS database password reset request"
				recipients = [email]
				confirm_url = url_for('confirm_password_reset', token=token,_external=True)
				body = ("Hi " + name + ". Someone recently requested to reset the password for your user account for the BreedCAFS database."
				" If you would like to proceed then please visit the following address: " + confirm_url +
				" As a reminder, your username for this account is " + username)
				html = render_template('emails/password_reset.html', 
					confirm_url=confirm_url, 
					username=username,
					name=name)
				send_email(subject, app.config['ADMINS'][0], recipients, body, html )
				flash('Please check your email to confirm password reset')
				return redirect(url_for('login'))
			else:
				flash('User is not confirmed - please register')
		return render_template('password_reset.html', form=form, title='Password reset')
	except (ServiceUnavailable):
		flash("Database unavailable")
		return redirect(url_for('index'))

@app.route('/confirm_password_reset/<token>', methods=['GET', 'POST'])
def confirm_password_reset(token):
	try:
		try:
			ts = URLSafeTimedSerializer(app.config['SECRET_KEY'])
			email = ts.loads(token, salt=app.config["PASSWORD_RESET_SALT"], max_age=86400)
		except:
			flash('Please submit a new request and confirm within 24hrs')
			return redirect(url_for('password_reset'))
		form = PasswordResetForm()
		if form.validate_on_submit():
			User.password_reset(email, form.password.data)
			flash('Your password has been reset')
			return redirect(url_for('login'))
		return render_template('confirm_password_reset.html', 
			form=form, 
			token=token)
	except (ServiceUnavailable):
		flash("Database unavailable")
		return redirect(url_for('index'))