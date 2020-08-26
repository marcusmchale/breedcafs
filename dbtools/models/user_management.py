from flask_bcrypt import Bcrypt
from dbtools.cypher import cypher_dict
from dbtools import app




def record_bad_registration(self, username, ip_address):
	logging.info('bad login: %s %s' % (ip_address, username))
	redis_store.zadd('bad_logins_all', '%s:%s' % (ip_address, username), self.time)
	redis_store.zadd('bad_logins_user_%s' % username, username, self.time)
	redis_store.zadd('bad_logins_ip_%s' % ip_address, ip_address, self.time)

def register_user(tx, username, password, email, name, partner):
	tx.run(
		query=cypher_dict['register_user'],
		username=username,
		password=Bcrypt(app).generate_password_hash(password),
		email=email,
		name=name,
		partner=partner
	)


def get_allowed_emails(tx):
	return tx.run(cypher_dict['get_allowed_emails'])


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
