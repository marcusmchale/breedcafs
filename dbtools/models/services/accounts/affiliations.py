from dbtools.models.cypher.accounts import queries


def add_user_affiliations(tx, username, partners):
	tx.run(queries['add_user_affiliations.cypher'], username=username, partners=partners)


def remove_user_affiliations(tx, username, partners):
	tx.run(queries['remove_user_affiliations.cypher'], username=username, partners=partners)


def get_user_affiliations(tx, username):
	result = tx.run(queries['get_user_affiliations'], username=username)
	confirmed = []
	pending = []
	for record in result:
		if record['data_shared']:
			fullname = record['p.fullname'] + " *"
		else:
			fullname = record['p.fullname']
		if record['confirmed']:
			confirmed.append((record['p.name'], fullname))
		else:
			if record['admin_emails']:
				fullname = fullname + ' <br>Contact: ' + ','.join(record['admin_emails'])
			pending.append((record['p.name'], fullname))
	return {'confirmed': confirmed, 'pending': pending}


def get_users_affiliations(tx, username, access):
	# if have global admin, retrieves all user accounts
	if 'global_admin' in access:
		query = cypher['get_affiliations_global_admin']
	# else if have partner_admin retrieves just those users that have "data_shared" with a partner
	# for which the current user is an admin
	elif 'partner_admin' in access:
		query = cypher['get_affiliations_partner_admin']
	else:
		return None
	user_affiliations = tx.run(query, username=username)
	return user_affiliations


def toggle_users_affiliations(tx, username, access, username_partner_list):
	if 'global_admin' in access:
		query = cypher['toggle_affiliations_global_admin']
	else:
		query = cypher['toggle_affiliations']
	tx.run(query, username=username, username_partner_list=username_partner_list)