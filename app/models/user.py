from app import app
from app.cypher import Cypher
from passlib.hash import bcrypt
from neo4j_driver import get_driver

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
#These are classmethods as they don't need a username
	@classmethod
	def confirm_email(cls, email):
		with get_driver().session() as neo4j_session:
			neo4j_session.write_transaction(cls._confirm_email, email=email)
	@classmethod
	def _confirm_email(cls, tx, email):
		tx.run(Cypher.confirm_email, email=email)
	@classmethod
	def password_reset(cls, email, password):
		with get_driver().session() as neo4j_session:
			neo4j_session.write_transaction(cls._password_reset, email=email, password=password)
	@classmethod		
	def _password_reset(cls, tx, email, password):
		tx.run(Cypher.password_reset, email=email, password=bcrypt.encrypt(password))
