from app import app
from app.cypher import Cypher
from passlib.hash import bcrypt
from config import uri, driver

class User:
	def __init__(self, username):
		self.username=username
	def find(self, email):
		self.email=email
		with driver.session() as session:
			return session.read_transaction(self._find)
	def _find (self, tx):
		for record in tx.run(Cypher.user_find, username=self.username, email=self.email):
			return (record['user'])
	def register(self, password, email, name, partner):
		self.password=password
		self.email=email
		self.name=name
		self.partner=partner
		with driver.session() as session:
			session.write_transaction(self._register)
	def _register (self, tx):
		tx.run(Cypher.user_register, username=self.username,
			password = bcrypt.encrypt(self.password), 
			email = self.email, 
			name = self.name, 
			partner = self.partner, 
			confirmed = 'False')
	def remove(self, email):
		self.email=email
		try:
			with driver.session() as session:
				session.write_transaction(self._remove)
			return True
		except:
			return False
	def _remove (self, tx):
		tx.run(Cypher.user_del, username=self.username, 
			email=self.email)
	def check_confirmed(self, email):
		user = self.find(email)
		if user['confirmed'] == u'True':
			return True
	def verify_password(self, password):
		user = self.find('')
		if user:
			return bcrypt.verify(password, user['password'])
		else:
			return False
#This is a classmethod so that it doesn't need username
	@classmethod
	def confirm_email(cls, email):
		with driver.session() as session:
			with session.begin_transaction() as tx:
				tx.run(Cypher.confirm_email, email=email)
#This is a classmethod so that it doesn't need username
	@classmethod
	def password_reset(cls, email, password):
		with driver.session() as session:
			with session.begin_transaction() as tx:
				tx.run(Cypher.password_reset, email=email, password=bcrypt.encrypt(password))
