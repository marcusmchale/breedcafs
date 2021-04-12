from neo4j import Transaction
from dbtools.models.entities.accounts import RegistrationUser
from dbtools.models.cypher.accounts import queries


def register_user(
		tx: Transaction,
		user: RegistrationUser,
	):
	tx.run(
		query=queries['register_user'],
		username=user.username,
		password=user.password,
		email=user.email,
		name=user.name,
		partner=user.partner.name
	)


def get_allowed_emails(tx):
	return tx.run(cypher_dict['get_allowed_emails'])


def record_bad_registration(self, username, ip_address):
	logging.info('bad login: %s %s' % (ip_address, username))
	redis_store.zadd('bad_logins_all', '%s:%s' % (ip_address, username), self.time)
	redis_store.zadd('bad_logins_user_%s' % username, username, self.time)
	redis_store.zadd('bad_logins_ip_%s' % ip_address, ip_address, self.time)



def delete_user(tx, email):
	tx.run(
		cypher_dict['delete_user'],
		email=email
	)


def set_user_confirmed(tx, email):
	tx.run(
		cypher_dict['set_user_confirmed'],
		email=email
	)


def set_password(tx, email, password):
	password = Bcrypt(app).generate_password_hash(password)
	tx.run(
		cypher_dict['set_password'], email=email, password=password
	)
