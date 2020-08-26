
@app.route('/user/get_affiliations', methods=['GET'])
def get_affiliations():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		affiliations = User(session['username']).get_user_affiliations()
		confirmed = set(affiliations['confirmed'])
		pending = set(affiliations['pending'])
		other = (
			set(SelectionList.get_partners()) - confirmed - pending
			# also remove the one that had an asterix concatenated
			- set([(i[0], i[1][:-2]) for i in pending if i[1].endswith(' *')])
			- set([(i[0], i[1][:-2]) for i in confirmed if i[1].endswith(' *')])
		)
		return jsonify({
			'confirmed': sorted(tuple(confirmed), key=lambda tup: tup[1]),
			'pending': sorted(tuple(pending), key=lambda tup: tup[1]),
			'other': sorted(tuple(other), key=lambda tup: tup[1])
		})


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
				remove_result = remove_user_affiliations(User(session['username']).username, to_remove)
			else:
				remove_result = []
			to_add = request.form.getlist('other')
			if to_add:
				add_result = add_user_affiliations(User(session['username']).username, to_add)
			else:
				add_result = []
			return jsonify({
				'success': 'Updated affiliations',
				'remove_result': remove_result,
				'add_result': add_result
			})

