from app import app, logging, celery, redis_store, ServiceUnavailable
from flask import request
from app.cypher import Cypher
from passlib.hash import bcrypt
from neo4j_driver import get_driver
from time import time
from datetime import datetime

class User:
	def __init__(self, username):
		self.username=username
	def find(self, email):
		self.email=email
		with get_driver().session() as neo4j_session:
			return neo4j_session.read_transaction(self._find)
	def _find (self, tx):
		for record in tx.run(Cypher.user_find, username=self.username, email=self.email):
			return (record['user'])
	def register(self, password, email, name, partner):
		self.password=password
		self.email=email
		self.name=name
		self.partner=partner
		with get_driver().session() as neo4j_session:
			neo4j_session.write_transaction(self._register)
	def _register (self, tx):
		tx.run(Cypher.user_register, username=self.username,
			password = bcrypt.encrypt(self.password), 
			email = self.email, 
			name = self.name, 
			partner = self.partner)
	def remove(self, email):
		self.email=email
		try:
			with get_driver().session() as neo4j_session:
				neo4j_session.write_transaction(self._remove)
			return True
		except:
			return False
	def _remove (self, tx):
		tx.run(Cypher.user_del,	email=self.email)
	def check_confirmed(self, email):
		user = self.find(email)
		if user['confirmed'] == True:
			return True
	def verify_password(self, password):
		user = self.find('')
		if user:
			return bcrypt.verify(password, user['password'])
		else:
			return False
	#single transaction for login
	def login(self, password, ip_address):
		#Need to handle brute force attacks.
		#allow 5 bad logins per 5 minute window per username or IP
		now = time()
		expires = now - 300
		#first we also check if there is an unusual amount of bad logins across all users in the last 5 minutes, this would signal a bot-net attack and should trigger a system wide lock-down of logins while it is ongoing
		try:
			if redis_store.exists('bad_logins_all'):
				redis_store.zremrangebyscore('bad_logins_all', '-inf', expires)
				bad_logins_all = redis_store.zcard('bad_logins_all')
				if bad_logins_all >=100:
					logging.warning( +'A high volume of bad logins is occurring - consider this may be a bot-net attack')
					return {'error':'We are currently experiencing a high number of failed logins to the site. To protect your data, logins are temporarily disabled. Please try again later'}
			#next check against username
			elif redis_store.exists('bad_logins_user_' + self.username):
				redis_store.zremrangebyscore('bad_logins_user_' + self.username, '-inf', expires)
				bad_logins_user = redis_store.zcard('bad_logins_user_' + self.username)
				if bad_logins_user >= 5:
					logging.warning('More than 5 bad login attempts in 5 minute window from user:' + self.username)
					return {'error':'A maximum of 5 failed login attempts are allowed per username in a 5 minute window. Please try again later.'}
			#next check against IP
			elif redis_store.exists('bad_logins_ip_' + ip_address):
				redis_store.zremrangebyscore('bad_logins_ip_' + ip_address, '-inf', expires)
				bad_logins_ip = redis_store.zcard('bad_logins_ip_' + ip_address)
				if bad_logins_ip >= 5:
					logging.warning('More than 5 bad login attempts in 5 minute window from IP:' + ip_address)
					return {'error':'A maximum of 5 failed login attempts are allowed per IP address in a 5 minute window. Please try again later.'}
		except: 
			logging.error('Checks against bad login counts has failed. Redis may be unavailable')
			return {'error':'An error as occurred, please try again.'}
		#now connect to neo4j retrieve user by username
		try:
			user = self.find('')
			if user:
				if user['confirmed'] == True:
					if bcrypt.verify(password, user['password']):
						return {'success':'Logged in'}			
					else:
						#handle bad login attempts here, 
						redis_store.zadd('bad_logins_user_' + self.username, now, now)
						redis_store.zadd('bad_logins_ip_' + ip_address, now, now)
						logging.info('bad login: ' + ip_address + ' ' + self.username)
						return {'error':'Please check your password'}
				else:
					return {'error':'Username is not confirmed. Please check your email to confirm'}
			else:
				return {'error':'Username is not registered'}
		except (ServiceUnavailable):
			logging.error('Neo4j database is unavailable')
			return {'error':'The database is unavailable, please try again later.'}
		
#These are classmethods as they don't need a username
	@staticmethod
	def confirm_email(email):
		with get_driver().session() as neo4j_session:
			neo4j_session.write_transaction(User._confirm_email, email=email)
	@staticmethod
	def _confirm_email(tx, email):
		tx.run(Cypher.confirm_email, email=email)
	@staticmethod
	def password_reset(email, password):
		with get_driver().session() as neo4j_session:
			neo4j_session.write_transaction(User._password_reset, email=email, password=password)
	@staticmethod		
	def _password_reset(tx, email, password):
		tx.run(Cypher.password_reset, email=email, password=bcrypt.encrypt(password))
