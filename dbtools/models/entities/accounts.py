from dbtools import app
from flask_bcrypt import Bcrypt


class Partner:
	def __init__(self, name, fullname):
		self.name = name
		self.fullname = fullname


class User:
	def __init__(self, username):
		self.username = username


class SessionUser(User):
	def __init__(self, username, ip_address):
		super().__init__(username)
		self.ip_address = ip_address


class RegistrationUser(SessionUser):
	def __init__(
			self,
			username,
			ip_address,
			password,
			email,
			name,
			partner: Partner
	):
		super().__init__(username, ip_address)
		self.password = Bcrypt(app).generate_password_hash(password)
		self.email = email
		self.name = name
		self.partner = Partner


class AccountUser(User):
	def __init__(self, username, password, confirmed, access):
		super().__init__(username)
		self.password = password
		self.confirmed = confirmed
		self.access = access

