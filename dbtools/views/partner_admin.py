
# a route for partner admin only available to global admins
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
			except (ServiceUnavailable, SecurityError):
				flash("Database unavailable")
				return redirect(url_for('index'))
		else:
			flash('You attempted to access a restricted page')
			return redirect(url_for('index'))


# endpoint to select partner_admins (where relationship ADMIN_FOR exists flip property confirmed = true/false)
@app.route('/admin/confirm_partner_admins', methods = ['POST'])
def confirm_partner_admins():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		if 'global_admin' in session['access']:
			try:
				form = PartnerAdminForm.update()
				# make list of both, the function toggles TRUE/FALSE for confirmed
				if form.validate_on_submit():
					admins = request.form.getlist('partner_admins') + request.form.getlist('not_partner_admins')
					if admins:
						for i, item in enumerate(admins):
							admins[i] = json.loads(item)
						users = User.admin_confirm_admins(admins)
						return jsonify({'success': [record[0] for record in users]})
					else:
						return jsonify({'error': 'No users selected'})
			except (ServiceUnavailable, SecurityError):
				flash("Database unavailable")
				return redirect(url_for('admin'))
		else:
			flash('User access to this page is restricted to administrators')
			return redirect(url_for('index'))
