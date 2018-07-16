from app import (
	# app,
	logging,
	# celery,
	redis_store,
	redis_exceptions,
	ServiceUnavailable,
	AuthError
)
from app.cypher import Cypher
from passlib.hash import bcrypt
from neo4j_driver import get_driver
from time import time
# from datetime import datetime


class User:
	def __init__(self, username):
		self.username = username
		self.email = None
		self.password = None
		self.partner = None
		self.name = None

	def find(self, email):
		self.email = email
		with get_driver().session() as neo4j_session:
			return neo4j_session.read_transaction(self._find)

	def _find(self, tx):
		for record in tx.run(Cypher.user_find, username=self.username, email=self.email):
			return record['user']

	def get_user_affiliations(self):
		with get_driver().session() as neo4j_session:
			confirmed = []
			pending = []
			for record in neo4j_session.read_transaction(self._get_user_affiliations):
				if record['data_shared']:
					fullname = record['p.fullname'] + " *"
				else:
					fullname = record['p.fullname']
				if record['confirmed']:
					confirmed.append((record['p.name'], fullname))
				elif record['confirmed']:
					if record['admin_email']:
						fullname = fullname + ' <br>Contact: ' + record['admin_email']
					pending.append((record['p.name'], fullname))
			return {'confirmed': confirmed, 'pending': pending}

	def _get_user_affiliations(self, tx):
		return tx.run(Cypher.user_affiliations, username = self.username)

	def add_affiliations(self, partners):
		with get_driver().session() as neo4j_session:
			return [record['p.name'] for record in neo4j_session.write_transaction(self._add_affiliations, partners)]

	def _add_affiliations(self, tx, partners):
		return tx.run(
			Cypher.add_affiliations,
			username = self.username, 
			partners = partners
		)

	def remove_affiliations(self, partners):
		with get_driver().session() as neo4j_session:
			return [record['p.name'] for record in neo4j_session.write_transaction(self._remove_affiliations, partners)]

	def _remove_affiliations(self, tx, partners):
		return tx.run(Cypher.remove_affiliations, username = self.username, partners = partners)

	@staticmethod
	def get_allowed_emails():
		try:
			with get_driver().session() as neo4j_session:
				result = neo4j_session.read_transaction(User._get_allowed_emails)
				return set(
					[item for sublist in [i['e.allowed'] for i in result] for item in sublist]
				)
		except (ServiceUnavailable, AuthError):
			logging.error('Get allowed emails failed due to service unavailable')
			return {'error': 'The database is unavailable, please try again later.'}

	@staticmethod
	def _get_allowed_emails(tx):
		return tx.run(Cypher.allowed_emails)

	def get_user_allowed_emails(self):
		try:
			with get_driver().session() as neo4j_session:
				result = neo4j_session.read_transaction(self._get_user_allowed_emails)
				return [record[0] for record in result][0]
		except (ServiceUnavailable, AuthError):
			logging.error('Get user allowed email failed due to service unavailable')
			return {'error': 'The database is unavailable, please try again later.'}

	def _get_user_allowed_emails(self, tx):
		return tx.run(Cypher.user_allowed_emails, username = self.username)

	def add_allowed_email(self, email):
		self.email = email
		try:
			with get_driver().session() as neo4j_session:
				result = neo4j_session.write_transaction(self._add_allowed_email)
				return [record[0] for record in result]
		except (ServiceUnavailable, AuthError):
			logging.error('Add allowed email failed due to service unavailable')
			return {'error': 'The database is unavailable, please try again later.'}

	def _add_allowed_email(self, tx):
		return tx.run(
			Cypher.add_allowed_email,
			username = self.username, 
			email = self.email
		)

	def remove_allowed_email(self, email):
		self.email = email
		try:
			with get_driver().session() as neo4j_session:
				result = neo4j_session.write_transaction(self._remove_allowed_email)
				return [record[0] for record in result]
		except (ServiceUnavailable, AuthError):
			logging.error('Remove allowed email failed due to service unavailable')
			return {'error': 'The database is unavailable, please try again later.'}

	def _remove_allowed_email(self, tx):
		return tx.run(
			Cypher.remove_allowed_email,
			username = self.username, 
			email = self.email
		)

	def register(self, password, email, name, partner):
		self.password = password
		self.email = email
		self.name = name
		self.partner = partner
		with get_driver().session() as neo4j_session:
			neo4j_session.write_transaction(self._register)

	def _register(self, tx):
		tx.run(
			Cypher.user_register,
			username=self.username,
			password = bcrypt.encrypt(self.password), 
			email = self.email, 
			name = self.name, 
			partner = self.partner
		)

	def remove(self, email):
		self.email = email
		try:
			with get_driver().session() as neo4j_session:
				neo4j_session.write_transaction(self._remove)
			return True
		except (ServiceUnavailable, AuthError):
			return False

	def _remove(self, tx):
		tx.run(Cypher.user_del,	email=self.email)

	def check_confirmed(self, email):
		user = self.find(email)
		if user['confirmed']:
			return True

	# single transaction for login
	def login(self, password, ip_address):
		# Need to handle brute force attacks.
		# allow 5 bad login attempts per 5 minute window per username or IP
		now = time()
		expires = now - 300
		# first we check if there is an unusual amount of bad login attempts across all users in the last 5 minutes,
		# this would signal a bot-net attack and should trigger a temporary system wide lock-down of login attempts
		try:
			if redis_store.exists('bad_logins_all'):
				redis_store.zremrangebyscore('bad_logins_all', '-inf', expires)
				bad_logins_all = redis_store.zcard('bad_logins_all')
				if bad_logins_all >= 100:
					logging.warning('A high volume of bad logins is occurring - consider this may be a bot-net attack')
					return {
						'error': (
							'We are currently experiencing a high frequency of failed login attempts to the site. '
							'To protect your data, login is temporarily disabled. Please try again later'
						)}
			# next check against username
			elif redis_store.exists('bad_logins_user_' + self.username):
				redis_store.zremrangebyscore('bad_logins_user_' + self.username, '-inf', expires)
				bad_logins_user = redis_store.zcard('bad_logins_user_' + self.username)
				if bad_logins_user >= 5:
					logging.warning('More than 5 bad login attempts in 5 minute window from user:' + self.username)
					return {
						'error': (
							'A maximum of 5 failed login attempts are allowed per username in a 5 minute window.'
							' Please try again later.'
						)}
			# next check against IP
			elif redis_store.exists('bad_logins_ip_' + ip_address):
				redis_store.zremrangebyscore('bad_logins_ip_' + ip_address, '-inf', expires)
				bad_logins_ip = redis_store.zcard('bad_logins_ip_' + ip_address)
				if bad_logins_ip >= 5:
					logging.warning('More than 5 bad login attempts in 5 minute window from IP:' + ip_address)
					return {
						'error': (
							'A maximum of 5 failed login attempts are allowed per IP address in a 5 minute window. '
							'Please try again later.'
						)}
		except Exception as e:
			logging.error('Checks against bad login counts has failed. Redis may be unavailable' + str(e))
			return {'error': 'An error as occurred, please try again.'}
		# now connect to neo4j retrieve user by username
		try:
			user = self.find('')
			if user:
				if user['confirmed']:
					if bcrypt.verify(password, user['password']):
						return {'success': 'Logged in', 'access': user['access']}
					else:
						# handle bad login attempts here,
						redis_store.zadd('bad_logins_user_' + self.username, now, now)
						redis_store.zadd('bad_logins_ip_' + ip_address, now, now)
						logging.info('bad login: ' + ip_address + ' ' + self.username)
						return {'error': 'Please check your password'}
				else:
					return {'error': 'Username is not confirmed. Please check your email to confirm'}
			else:
				return {'error': 'Username is not registered'}
		except (ServiceUnavailable, AuthError):
			logging.error('Neo4j database is unavailable')
			return {'error': 'The database is unavailable, please try again later.'}

	# These are static as they don't need username
	@staticmethod
	def confirm_email(email):
		with get_driver().session() as neo4j_session:
			neo4j_session.write_transaction(User._confirm_email, email = email)

	@staticmethod
	def _confirm_email(tx, email):
		tx.run(Cypher.confirm_email, email = email)

	@staticmethod
	def password_reset(email, password):
		with get_driver().session() as neo4j_session:
			neo4j_session.write_transaction(User._password_reset, email=email, password=password)

	@staticmethod
	def _password_reset(tx, email, password):
		tx.run(Cypher.password_reset, email=email, password=bcrypt.encrypt(password))

	# this retrieves the list of users the input user is able to confirm
	def get_users_for_admin(self, access):
		with get_driver().session() as neo4j_session:
			return neo4j_session.read_transaction(self._get_users_for_admin, access)

	def _get_users_for_admin(self, tx, access):
		# if have global admin, retrieves all user accounts
		if 'global_admin' in access:
			users = tx.run(Cypher.global_admin_users)
		# else if have partner_admin retrieves just those users that registered with a partner
		# for which the current user has "admin" in relationship with
		elif 'partner_admin' in access:
			users = tx.run(Cypher.partner_admin_users, username = self.username)
		else:
			users = None
		return users

	# this confirms/unconfirms lists of users
	@staticmethod
	def admin_confirm_users(username, access, confirm_list):
		with get_driver().session() as neo4j_session:
			return neo4j_session.write_transaction(User._admin_confirm_users, username, access, confirm_list)

	@staticmethod
	def _admin_confirm_users(tx, username, access, confirm_list):
		if 'global_admin' in access:
			query = Cypher.global_confirm_users
		elif 'partner_admin' in access:
			query = Cypher.partner_confirm_users
		else:
			query = None
		return tx.run(query, username = username, confirm_list = confirm_list)

	# the below are global_admin only functions to manage partner_admins
	@staticmethod
	def admin_get_partner_admins():
		with get_driver().session() as neo4j_session:
			return neo4j_session.read_transaction(User._admin_get_partner_admins)

	@staticmethod
	def _admin_get_partner_admins(tx):
		return tx.run(Cypher.partner_admins)

	@staticmethod
	def admin_confirm_admins(admins):
		with get_driver().session() as neo4j_session:
			return neo4j_session.write_transaction(User._admin_confirm_admins, admins)

	@staticmethod
	def _admin_confirm_admins(tx, admins):
		return tx.run(Cypher.confirm_admins, admins = admins)
