def get_partner_admins(tx):
	return tx.run(cypher['get_admins'])


def toggle_admins(tx, admins):
	tx.run(cypher['toggle_admins'], admins=admins)
