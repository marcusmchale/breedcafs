import os
from flask import Flask, request, session, redirect, url_for, render_template, flash, send_file
from app import app
from datetime import datetime
from werkzeug.utils import secure_filename
from itsdangerous import URLSafeTimedSerializer
from .models import User, Upload, List
from .forms import *
from .emails import send_email

#token generation for email confirmation
ts = URLSafeTimedSerializer(app.config['SECRET_KEY'], salt='email-confirm-key')

#
@app.route('/')
@app.route('/index')
def index():
	return render_template('index.html', title='Home')

@app.route('/register', methods=['GET','POST'])
def register():
	form=RegistrationForm()
	if request.method == 'POST' and form.validate_on_submit():
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
			User(username).register(password, email, name, partner)
			flash('New registration successful')
			token = ts.dumps(email)
			subject = "BreedCAFS database confirmation."
			recipients = [email]
			confirm_url = url_for(
				'confirm',
				token=token,
				_external=True)
			html = render_template('emails/activate.html', confirm_url=confirm_url)
			send_email(subject, app.config['ADMINS'][0], recipients, "confirmation", html )
			flash('Please check your email to confirm.')
			return redirect(url_for('login'))
		except:
			flash('Error with registration please contact an administrator')
	return render_template('register.html', form=form, title='Register') 

@app.route('/confirm/<token>', methods=['GET', 'POST'])
def confirm(token):
	try:
		email = ts.loads(token)
		User.confirm_email(email)
		flash('Email confirmed')
		return redirect(url_for('login'))
	except:
		flash('Please register again and confirm within 24hrs')
		return redirect(url_for('register'))

@app.route('/login', methods=['GET', 'POST'])
def login():
	form = LoginForm()
	if request.method == 'POST' and form.validate_on_submit():
		username = form.username.data.lower()
		password = form.password.data
#added the bit about username 'start' just to tidy the interface, the user is created when initialiseDB is run
		if username == 'start':
			flash ('Username not registered')
			return redirect(url_for('login')) 
		if not User(username).find(''):
			flash ('Username not registered')
			return redirect(url_for('login'))
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

@app.route('/logout', methods=['GET'])
def logout():
	session.pop('username', None)
	flash('Logged out.')
	return redirect(url_for('index'))

@app.route('/upload', methods=['GET', 'POST'])
def upload():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		form = UploadForm()
		if request.method == 'POST' and form.validate_on_submit():
			uploads=app.config['UPLOAD_FOLDER']
			username=session['username']
			time=datetime.now().strftime('_%Y%m%d-%H%M%S_')
			file = form.file.data
			subtype=form.submission_type.data
#If a user is deleted/username changed (while they are still logged in)
#they will be booted and asked to login/register when they try to submit
			if not User(username).find(''):
				session.pop('username', None)
				flash('Error: User not found - please login/register')
				return redirect(url_for('index'))
			if not file:
				flash('No file selected')
				return redirect(url_for('upload'))
			if file.filename == '':
				flash('No file selected')
				return redirect(url_for('upload'))
			if Upload(username, file.filename).allowed_file():
				filename = username+time+secure_filename(file.filename)
				file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
				if Upload(username, os.path.join(uploads, filename)).submit(subtype):
					flash('Data submitted to server')
				else: flash('Problem with submission. Please check data type')
			else:
				flash('Please submit CSV file')
				return redirect(url_for('upload'))
			return redirect(url_for('index'))
	return render_template('upload.html', form=form, title='Upload')

@app.route('/help', methods=['GET'])
def help():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		return render_template('help.html', title='Help')

@app.route('/create', methods=['GET'])
def create():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		return render_template('create.html', title='Create')

@app.route('/create_trt', methods=['GET', 'POST'])
def create_trt():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		form = CreateTraits()
		if request.method == 'POST' and form.validate_on_submit():
			selection = form.traits.data
			trt=List('Trait').create_trt(selection, 'name')
			return send_file(trt,
				attachment_filename='BreedCAFS_traits.trt', 
				as_attachment=True,
				mimetype=('txt/csv'))
	return render_template('create_trt.html', form=form, title='Create traits.trt')

@app.route('/create_fields', methods=['GET', 'POST'])
def create_fields():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		form = CreateFields.update()
		if request.method == 'POST' and form.validate_on_submit():
			pass
	return render_template('create_fields.html', form=form,	title='Create fields.csv')
