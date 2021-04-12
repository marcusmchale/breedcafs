


def get_unregistered_emails_partner_admin(username, tx):
	return tx.run(
		cypher_dict['get_unregistered_emails_partner_admin'],
		username=username
	)


def add_allowed_email(username, tx, email):
	tx.run(
		cypher_dict['add_allowed_email'],
		username=username,
		email=email
	)


def remove_allowed_email(username, tx, email):
	tx.run(
		cypher_dict['remove_allowed_email'],
		username=username,
		email=email
	)