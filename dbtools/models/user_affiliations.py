from dbtools.cypher import cypher_queries


def add_user_affiliations(tx, username, partners):
	tx.run(cypher_queries['add_user_affiliations.cypher'], username=username, partners=partners)


def remove_user_affiliations(tx, username, partners):
	tx.run(cypher_queries['remove_user_affiliations.cypher'], username=username, partners=partners)


def get_user_affiliations(tx, username):
	result = tx.run(cypher_queries['get_user_affiliations'], username=username)
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