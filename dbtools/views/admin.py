



# catchall admin route, redirects for global vs partner admins
@app.route('/admin', methods=['GET'])
def admin():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		if 'global_admin' in session['access'] or 'partner_admin' in session['access']:
			return render_template('admin_choice.html')
		else:
			flash('You attempted to access a restricted page')
			return redirect(url_for('index'))





# a route for the user admin available to partner_admins (and global_admins)
@app.route('/admin/user_admin', methods=['GET', 'POST'])
def user_admin():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			if 'global_admin' in session['access']:
				user_admin_form = UserAdminForm.update(session['username'], 'global_admin')
			elif 'partner_admin' in session['access']:
				user_admin_form = UserAdminForm.update(session['username'], 'partner_admin')
			else:
				flash('You attempted to access a restricted page')
				return redirect(url_for('index'))
			add_user_email_form = AddUserEmailForm()
			remove_user_email_form = RemoveUserEmailForm().update(session['username'])
			return render_template(
				'user_admin.html',
				user_admin_form = user_admin_form,
				add_user_email_form = add_user_email_form,
				remove_user_email_form = remove_user_email_form
			)
		except (ServiceUnavailable, SecurityError):
			flash("Database unavailable")
			return redirect(url_for('index'))


# route to add new allowed users, the email is added to the current users own list
@app.route('/admin/add_allowed_email', methods=['POST'])
def add_allowed_user():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			if not set(session['access']).intersection({'global_admin', 'partner_admin'}):
				flash('You attempted to access a restricted page')
				return redirect(url_for('index'))
			else:
				add_user_email_form = AddUserEmailForm()
				if add_user_email_form.validate_on_submit():
					username = session['username']
					email = request.form['user_email'].lower()
					result = add_allowed_email(User(username).username, email)
					if result:
						return jsonify({'success': result})
					else:
						return jsonify({'error': 'This email address is already allowed '})
				else:
					return jsonify({'error': add_user_email_form.errors["user_email"]})
		except (ServiceUnavailable, SecurityError):
			flash("Database unavailable")
			return redirect(url_for('index'))


# route to remove allowed users from a partner_admin's own list
@app.route('/admin/remove_allowed_email', methods=['POST'])
def remove_allowed_user():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			if not set(session['access']).intersection({'global_admin', 'partner_admin'}):
				flash('You attempted to access a restricted page')
				return redirect(url_for('index'))
			else:
				username = session['username']
				remove_user_email_form = RemoveUserEmailForm.update(username)
				if remove_user_email_form.validate_on_submit():
					emails = request.form.getlist('emails_list')
					result = remove_allowed_email(User(username).username, emails)
					return jsonify({'success': result})
				else:
					return jsonify({'error': remove_user_email_form.errors["user_email"]})
		except (ServiceUnavailable, SecurityError):
			flash("Database unavailable")
			return redirect(url_for('index'))


# a route to get the list of allowed emails a partner_admin has added
@app.route('/admin/get_user_allowed_emails')
def get_user_allowed_emails():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			if not set(session['access']).intersection({'global_admin', 'partner_admin'}):
				flash('You attempted to access a restricted page')
				return redirect(url_for('index'))
			else:
				return jsonify(get_unregistered_emails(User(session['username']).username))
		except (ServiceUnavailable, SecurityError):
			flash("Database unavailable")
			return redirect(url_for('index'))



# endpoint to get lists of users in html table format to populate the form on user_admin page
@app.route('/admin/users', methods=['GET'])
def admin_users():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		if not any([i in session['access'] for i in ['global_admin', 'partner_admin']]):
			flash('User access to this page is restricted to administrators')
			return redirect(url_for('index'))

		# todo i should just be handing over the session access property to this function
		users = [record[0] for record in User(session['username']).get_users_affiliations(session['access'])]
		user_lists = defaultdict(list)
		for user in enumerate(users):
			if user[1]['Confirmed']:
				row = (
					'<tr><label for="confirmed_users-'
					+ str(user[0]) + '"></label><td>'
					+ user[1]['Name'] + '</td><td>'
					+ user[1]['Partner'] + '</td><td><input id="confirmed_users-'
					+ str(user[0]) + '" name="confirmed_users" value=\'{"username":"' + user[1]['Username']
					+ '", "partner":"' + user[1]["Partner"]
					+ '"}\' type="checkbox"></td></tr>'
				)
				user_lists['confirmed'].append(row)
			else:
				row = (
					'<tr><label for="unconfirmed_users-'
					+ str(user[0]) + '"></th><td>'
					+ user[1]['Name'] + '</td><td>'
					+ user[1]['Partner'] + '</td><td><input id="unconfirmed_users-'
					+ str(user[0]) + '" name="unconfirmed_users" value=\'{"username":"'
					+ user[1]['Username']
					+ '", "partner":"'
					+ user[1]["Partner"]
					+ '"}\' type="checkbox"></td></tr>'
				)
				user_lists['unconfirmed'].append(row)
		return jsonify(user_lists)


# endpoint to flip confirm attribute on affiliated relationship for selected users
@app.route('/admin/confirm_users', methods = ['POST'])
def confirm_users():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		if set(session['access']).intersection({'global_admin', 'partner_admin'}):
			try:
				if 'global_admin' in session['access']:
					form = UserAdminForm().update(session['username'], 'global_admin')
				elif 'partner_admin' in session['access']:
					form = UserAdminForm().update(session['username'], 'partner_admin')
				else:
					flash('You attempted to access a restricted page')
					return redirect(url_for('index'))
				# make list of both, the function toggles TRUE/FALSE for confirmed
				if form.validate_on_submit():
					confirm_list = request.form.getlist('unconfirmed_users') + request.form.getlist('confirmed_users')
					if confirm_list:
						for i, item in enumerate(confirm_list):
							confirm_list[i] = json.loads(item)
						users = User.admin_confirm_users(session['username'], session['access'], confirm_list)
						return jsonify({'success': [record[0] for record in users]})
					else:
						return jsonify({'error': 'No users selected'})
				else:
					return jsonify({'error': 'Unexpected form values'})
			except (ServiceUnavailable, SecurityError):
				flash("Database unavailable")
				return redirect(url_for('admin'))
		else:
			flash('You attempted to access a restricted page')
			return redirect(url_for('index'))


# endpoint to get list of current partner_admins (relationship ADMIN_FOR exists with property confirmed= true)
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
			if user[1]['Confirmed']:
				row = (
					'<tr><label for="partner_admins-%s"></label><td>%s</td><td>%s</td><td><input id="partner_admins-'
					'%s" name="partner_admins" value=\'{"username":"%s", "partner":"%s"}\' type="checkbox"></td></tr>'
					% (
						str(user[0]), user[1]['Name'], user[1]['Partner'],  str(user[0]), user[1]['Username'],
						user[1]["Partner"]
					)
				)
				user_lists['partner_admins'].append(row)
			else:
				row = (
					'<tr><label for="not_partner_admins-%s"></label><td>%s</td><td>%s</td><td><input id="not_partner_admins-'
					'%s" name="not_partner_admins" value=\'{"username":"%s", "partner":"%s"}\' type="checkbox"></td></tr>'
					% (
						str(user[0]), user[1]['Name'], user[1]['Partner'], str(user[0]), user[1]['Username'],
						user[1]["Partner"]
					)
				)
				user_lists['not_partner_admins'].append(row)
		return jsonify(user_lists)
