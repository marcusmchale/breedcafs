from dbtools.cypher import account_admin as cypher


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


def get_partner_admins(tx):
	return tx.run(cypher['get_admins'])


def toggle_admins(tx, admins):
	tx.run(cypher['toggle_admins'], admins=admins)
