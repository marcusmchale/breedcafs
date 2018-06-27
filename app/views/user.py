from app import app, ServiceUnavailable, logging
from flask import (request, 
	session, 
	redirect, 
	url_for,
	render_template, 
	flash, 
	jsonify,
	json)
from itsdangerous import URLSafeTimedSerializer
from app.models import User, Lists
from app.forms import (RegistrationForm, 
	UserAdminForm,
	AffiliationForm,
	PartnerAdminForm,
	AddUserEmailForm,
	RemoveUserEmailForm,
	LoginForm, 
	PasswordResetRequestForm, 
	PasswordResetForm)
from app.emails import send_email
from collections import defaultdict

#token generation
@app.route('/register', methods=['GET','POST'])
def register():
	try:
		form = RegistrationForm.update()
		if form.validate_on_submit():
			username = form.username.data
			password = form.password.data
			email = form.email.data.lower()
			name = form.name.data
			partner = form.partner.data
			#get list of allowed emails
			allowed_emails = User.get_allowed_emails()
			if not email in allowed_emails:
				flash('This email is not registered, please contact your BreedCAFS partner administrator.')
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
			except Exception as e:
				logging.info('Error registering user:' + str(e))
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
#this view handles service unavailable at the model level (in User.login)
	form = LoginForm()
	if form.validate_on_submit():
		username = form.username.data.lower()
		password = form.password.data
		ip_address = request.remote_addr
		#login:
		login_response = User(username).login(password, ip_address)
		if 'success' in login_response:
			session['username'] = username
			session['access'] = login_response['access']
			flash('Logged in.')
			return redirect(url_for('index'))
		else:
			flash (login_response['error'])
			return redirect(url_for('login'))
	return render_template('login.html', form=form, title='Login')


@app.route('/logout', methods=['GET'])
def logout():
	session.pop('username', None)
	session.pop('access', None)
	flash('Logged out.')
	return redirect(url_for('index'))

@app.route('/password_reset', methods=['GET', 'POST'])
def password_reset():
	try:
		#get list of allowed emails
		allowed_emails = User.get_allowed_emails()
		form = PasswordResetRequestForm()
		if form.validate_on_submit():
			email = form.email.data.lower()
			if not email in allowed_emails:
				flash('This email is not registered')
				return redirect(url_for('password_reset'))
			user = User("").find(email)
			import pdb; pdb.set_trace()
			if user:
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

@app.route('/user', methods=['GET', 'POST'])
def user():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		affiliation_form = AffiliationForm.update(session['username'])
		return render_template('user.html', 
			affiliation_form = affiliation_form)

@app.route('/user/get_affiliations', methods=['GET'])
def get_affiliations():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		affiliations = User(session['username']).get_user_affiliations()
		confirmed = set(affiliations['confirmed'])
		pending = set(affiliations['pending'])
		other = (set(Lists('Partner').create_list_tup('name', 'fullname')) - confirmed - pending
		#also remove the one that had an asterix concatenated
			- set([(i[0],i[1][:-2]) for i in pending if i[1].endswith(' *')])
			- set([(i[0],i[1][:-2]) for i in confirmed if i[1].endswith(' *')]))
		return jsonify({'confirmed':sorted(tuple(confirmed), key=lambda tup: tup[1]),
			'pending':sorted(tuple(pending), key=lambda tup: tup[1]),
			'other':sorted(tuple(other), key=lambda tup: tup[1])})

@app.route('/user/add_affiliations', methods=['POST'])
def add_affiliations():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		affiliation_form = AffiliationForm.update(session['username'])
		if affiliation_form.validate_on_submit():
			to_remove = request.form.getlist('pending')
			if to_remove:
				remove_result = User(session['username']).remove_affiliations(to_remove)
			else:
				remove_result = []
			to_add = request.form.getlist('other')
			if to_add:
				add_result = User(session['username']).add_affiliations(to_add)
			else:
				add_result = []
			return jsonify({'success':'Updated affiliations',
				'remove_result':remove_result,
				'add_result':add_result
				})

#catchall admin route, redirects for global vs partner admins
@app.route('/admin', methods=['GET'])
def admin():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		if 'global_admin' in session['access']:
			return render_template('admin_choice.html')
		elif 'partner_admin' in session['access']:
			return redirect(url_for('user_admin'))
		else:
			flash('You attempted to access a restricted page')
			return redirect(url_for('index'))

#a route for the user admin available to partner_admins (and global_admins)
@app.route('/admin/user_admin', methods=['GET', 'POST'])
def user_admin():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			if 'global_admin' in session['access']:
				user_admin_form = UserAdminForm.update(session['username'],'global_admin')
			elif 'partner_admin' in session['access']:
				user_admin_form = UserAdminForm.update(session['username'],'partner_admin')
			else:
				flash ('You attempted to access a restricted page')
				return redirect(url_for('index'))
			add_user_email_form = AddUserEmailForm()
			remove_user_email_form  = RemoveUserEmailForm().update(session['username'])
			return render_template('user_admin.html',
				user_admin_form = user_admin_form,
				add_user_email_form = add_user_email_form,
				remove_user_email_form = remove_user_email_form)
		except (ServiceUnavailable):
			flash("Database unavailable")
			return redirect(url_for('index'))

#route to add new allowed users, the email is added to the current users own list
@app.route('/admin/add_allowed_email', methods=['POST'])
def add_allowed_user():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			if not set(session['access']).intersection(set(['global_admin','partner_admin'])):
				flash ('You attempted to access a restricted page')
				return redirect(url_for('index'))
			else: 
				add_user_email_form = AddUserEmailForm()
				if add_user_email_form.validate_on_submit():
					username = session['username']
					email = request.form['user_email'].lower()
					result = User(username).add_allowed_email(email)
					if result:
						return jsonify({'success':result})
					else:
						return jsonify({'error':'This email address is already allowed '})
				else:
					return jsonify({'error':add_user_email_form.errors["user_email"]})
		except (ServiceUnavailable):
			flash ("Database unavailable")
			return redirect(url_for('index'))

#route to remove allowed users from a partner_admin's own list
@app.route('/admin/remove_allowed_email', methods=['POST'])
def remove_allowed_user():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:		
		try:
			if not set(session['access']).intersection(set(['global_admin','partner_admin'])):
				flash ('You attempted to access a restricted page')
				return redirect(url_for('index'))
			else:
				username = session['username']
				remove_user_email_form = RemoveUserEmailForm.update(username)
				if remove_user_email_form.validate_on_submit():
					emails = request.form.getlist('emails_list')
					result = User(username).remove_allowed_email(emails)
					return jsonify({'success':result})
				else:
					return jsonify({'error':add_user_email_form.errors["user_email"]})
		except (ServiceUnavailable):
			flash ("Database unavailable")
			return redirect(url_for('index'))


#a route to get the list of allowed emails a partner_admin has added
@app.route('/admin/get_user_allowed_emails')
def get_user_allowed_emails():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try: 
			if not set(session['access']).intersection(set(['global_admin','partner_admin'])):
				flash ('You attempted to access a restricted page')
				return redirect(url_for('index'))
			else: 
				return jsonify(User(session['username']).get_user_allowed_emails())
		except (ServiceUnavailable):
			flash ("Database unavailable")
			return redirect(url_for('index'))


#a route for partner admin only available to global admins
@app.route('/admin/partner_admin', methods=['GET', 'POST'])
def partner_admin():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		if 'global_admin' in session['access']:
			try:
				form = PartnerAdminForm.update()
				return render_template('partner_admin.html', form=form)
			except (ServiceUnavailable):
				flash("Database unavailable")
				return redirect(url_for('index'))
		else:
			flash ('You attempted to access a restricted page')
			return redirect(url_for('index'))

#endpoint to get lists of users in html table format to populate the form on user_admin page
@app.route('/admin/users', methods = ['GET'])
def admin_users():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		if 'global_admin' in session['access']:
			users = [record[0] for record in User(session['username']).get_users_for_admin('global_admin')]
		elif 'partner_admin' in session['access']:
			users = [record[0] for record in User(session['username']).get_users_for_admin('partner_admin')]
		else:
			flash('User access to this page is restricted to administrators')
			return redirect(url_for('index'))
		user_lists = defaultdict(list)
		for user in enumerate(users):
			if user[1]['Confirmed'] == True:
				row = ('<tr><th><label for="confirmed_users-' 
					+ str(user[0]) + '"></label></th><td>'
					+ user[1]['Name'] + '</td><td>'
					+ user[1]['Partner'] + '</td><td><input id="confirmed_users-'
					+ str(user[0]) + '" name="confirmed_users" value=\'{"username":"' + user[1]['Username'] 
					+ '", "partner":"' + user[1]["Partner"]
					+ '"}\' type="checkbox"></td></tr>')
				user_lists['confirmed'].append(row)
			if user[1]['Confirmed'] == False:
				row = ('<tr><th><label for="unconfirmed_users-' 
					+ str(user[0]) + '"></label></th><td>'
					+ user[1]['Name'] + '</td><td>'
					+ user[1]['Partner'] + '</td><td><input id="unconfirmed_users-'
					+ str(user[0]) + '" name="unconfirmed_users" value=\'{"username":"' 
					+ user[1]['Username'] 
					+ '", "partner":"' 
					+ user[1]["Partner"]
					+ '"}\' type="checkbox"></td></tr>')
				user_lists['unconfirmed'].append(row)
		return jsonify(user_lists)

#endpoint to flip confirm attribute on affiliated relationship for selected users
@app.route('/admin/confirm_users', methods = ['POST'])
def confirm_users():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		if set(session['access']).intersection(set(['global_admin','partner_admin'])):
			try:
				if 'global_admin' in session['access']:
					form = UserAdminForm().update(session['username'],'global_admin')
				elif 'partner_admin' in session['access']:
					form = UserAdminForm().update(session['username'],'partner_admin')
				#make list of both, the function toggles TRUE/FALSE for confirmed 
				if form.validate_on_submit():
					confirm_list = request.form.getlist('unconfirmed_users') + request.form.getlist('confirmed_users')
					if confirm_list:
						for i, item in enumerate(confirm_list):
							confirm_list[i] = json.loads(item)
						users = User.admin_confirm_users(session['username'], session['access'], confirm_list)
						return jsonify({'success':[record[0] for record in users]})
					else:
						return jsonify({'error':'No users selected'})
				else:
					return jsonify({'error':'Unexpected form values'})
			except (ServiceUnavailable):
				flash("Database unavailable")
				return redirect(url_for('admin'))
		else:
			flash('You attempted to access a restricted page')
			return redirect(url_for('index'))


#endpoint to get list of current partner_admins (relationship ADMIN_FOR exists with property confirmed= true)
@app.route('/admin/partner_admins', methods = ['GET'])
def admin_partner_admins():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		if 'global_admin' in session['access']:
			users = [record[0] for record in User.admin_get_partner_admins()]
		else:
			flash('You attempted to access a restricted page')
			return redirect(url_for('index'))
		user_lists = defaultdict(list)
		for user in enumerate(users):
			if user[1]['Confirmed'] == True:
				row = ('<tr><th><label for="partner_admins-' 
					+ str(user[0]) + '"></label></th><td>'
					+ user[1]['Name'] + '</td><td>'
					+ user[1]['Partner'] + '</td><td><input id="partner_admins-'
					+ str(user[0]) + '" name="partner_admins" value=\'{"username":"' + user[1]['Username'] 
					+ '", "partner":"' + user[1]["Partner"]
					+ '"}\' type="checkbox"></td></tr>')
				user_lists['partner_admins'].append(row)
			if user[1]['Confirmed'] == False:
				row = ('<tr><th><label for="not_partner_admins-' 
					+ str(user[0]) + '"></label></th><td>'
					+ user[1]['Name'] + '</td><td>'
					+ user[1]['Partner'] + '</td><td><input id="not_partner_admins-'
					+ str(user[0]) + '" name="not_partner_admins" value=\'{"username":"' 
					+ user[1]['Username'] 
					+ '", "partner":"' 
					+ user[1]["Partner"]
					+ '"}\' type="checkbox"></td></tr>')
				user_lists['not_partner_admins'].append(row)
		return jsonify(user_lists)

#endpoint to select partner_admins (where relationship ADMIN_FOR exists flip property confirmed = true/false)
@app.route('/admin/confirm_partner_admins', methods = ['POST'])
def confirm_partner_admins():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		if 'global_admin' in session['access']:
			try:
				form = PartnerAdminForm.update()
				#make list of both, the function toggles TRUE/FALSE for confirmed 
				if form.validate_on_submit():
					admins = request.form.getlist('partner_admins') + request.form.getlist('not_partner_admins')
					if admins:
						for i, item in enumerate(admins):
							admins[i] = json.loads(item)
						users = User.admin_confirm_admins(admins)
						return jsonify({'success':[record[0] for record in users]})
					else:
						return jsonify({'error':'No users selected'})
			except (ServiceUnavailable):
				flash("Database unavailable")
				return redirect(url_for('admin'))
		else:
			flash('User access to this page is restricted to administrators')
			return redirect(url_for('index'))