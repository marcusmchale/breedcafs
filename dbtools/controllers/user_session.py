import time

from flask_bcrypt import Bcrypt

from flask import url_for

from dbtools import (
	app,
	logging,
	redis_client
)

from dbtools.cypher.account_admin import queries


class User:
	def __init__(self, username, password):
		self.username = username,
		self.password = password,
		self.access = None


class Guest(User):
	def __init__(self, username, password, ip_address):
		super().__init__(username, password)
		self.ip_address = ip_address


class RegisteredUser(User):
	def __init__(self, user_record):
		super().__init__(user_record['username'], user_record['password'])
		self.confirmed = user_record['confirmed']
		self.access = user_record['access']


class LoginHandler:
	def __init__(self, guest):
		self.errors = set()
		self.time = time.time()
		self.guest = guest
		self.registered_user = None

	def login(self, tx):
		if self.excessive_bad_logins() or not self.valid_credentials(tx):
			return False
		else:
			return True

	def valid_credentials(self, tx):
		user_record = tx.run(queries['get_user_properties'])
		self.registered_user = RegisteredUser(user_record)
		return self.password_matches() and self.email_confirmed

	def password_matches(self):
		if Bcrypt(app).check_password_hash(self.registered_user.password, self.guest.password):
			return True
		else:
			self.record_bad_login()
			self.errors.add(
				'Password does not match, please try again. \n '
				'You may want to <a href=%s>reset</a> your password' % url_for('password_reset')
			)
			return False

	def email_confirmed(self):
		if self.registered_user.confirmed:
			return True
		else:
			self.errors.add(
				'Username is not confirmed. \n'
				'Please check your email and follow the link provided following registration.'
			)
			return False

	def excessive_bad_logins(self):
		if any([
			self.excessive_global_bad_logins(),
			self.excessive_username_bad_logins(),
			self.excessive_ip_bad_logins()
		]):
			return True
		return False

	def excessive_global_bad_logins(self, max_bad_logins=100, expire_seconds=300):
		# Need to handle brute force attacks.
		# first we check if there is an unusual amount of bad login attempts across all users in the last 5 minutes,
		# this would signal a bot-net attack and should trigger a temporary system wide lock-down of login attempts
		if redis_client.exists('bad_logins_all'):
			# remove all except most recent 5 minutes of bad logins from key
			redis_client.zremrangebyscore('bad_logins_all', '-inf', self.time - expire_seconds)
			# get list of bad logins and if
			bad_logins_all = redis_client.zcard('bad_logins_all')
			if bad_logins_all >= max_bad_logins:
				logging.warning(
					'A high volume (%s in the last %s mins) of bad logins is occurring'
					% (bad_logins_all, round(expire_seconds / 60))
				)
				self.errors.add(
					'We are currently experiencing a high volume of failed login attempts to the site. '
					'To protect your data, all logins are temporarily disabled. Please try again later'
				)
				return True
		return False

	def excessive_username_bad_logins(self, max_bad_logins=10, expire_seconds=300):
		username = self.guest.username
		if redis_client.exists('bad_logins_user_%s' % username):
			redis_client.zremrangebyscore('bad_logins_user_%s' % username, '-inf', self.time - expire_seconds)
			bad_logins = redis_client.zcard('bad_logins_user_%s' % username)
			if bad_logins >= max_bad_logins:
				logging.warning('More than 5 bad login attempts in 5 minute window from user: %s' % username)
				self.errors.add(
					'A maximum of %s failed login attempts are allowed per username in a %s minute window.'
					' Please try again later.' % (max_bad_logins, round(expire_seconds / 60))
				)
				return True
		return False

	def excessive_ip_bad_logins(self, max_bad_logins=10, expire_seconds=300):
		ip_address = self.guest.ip_address
		if redis_client.exists('bad_logins_ip_%s' % ip_address):
			redis_client.zremrangebyscore('bad_logins_ip_%s' % ip_address, '-inf', self.time - expire_seconds)
			bad_logins = redis_client.zcard('bad_logins_ip_%s' % ip_address)
			if bad_logins >= max_bad_logins:
				logging.warning('More than 5 bad login attempts in 5 minute window from IP:%s' % ip_address)
				self.errors.add(
					'A maximum of %s failed login attempts are allowed per IP address in a %s minute window. '
					'Please try again later.' % (max_bad_logins, round(expire_seconds / 60))
				)
				return True
		return False

	def record_bad_login(self):
		ip_address = self.guest.ip_address
		username = self.guest.username
		logging.info('bad login: %s %s' % (ip_address, username))
		redis_client.zadd('bad_logins_all', '%s:%s' % (ip_address, username), self.time)
		redis_client.zadd('bad_logins_user_%s' % username, username, self.time)
		redis_client.zadd('bad_logins_ip_%s' % ip_address, ip_address, self.time)
