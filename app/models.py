import os, csv, cStringIO
from datetime import datetime
from flask import session
from app import app
from passlib.hash	import bcrypt
from config import uri, driver, ALLOWED_EXTENSIONS
from cypher import Cypher

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

#User class related (all uploads are tied to a user) yet more specifically regarding uploads
class Upload(User):
	def __init__(self, username, filename):
		self.username=username
		self.filename=filename
	def allowed_file(self):
		return '.' in self.filename and \
			self.filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
	def submit(self, submission_type):
		self.submission_type=submission_type
		if self.submission_type == 'FB':
			with driver.session() as session:
				session.write_transaction(self._submit)
			return True
		else:
			return False
	def _submit(self, tx):
			tx.run(Cypher.upload_submit, username=self.username,
				filename='file://' + self.filename,
				submission_time=str(datetime.now()),
				submission_type=self.submission_type)

#Get dicts of values matching a node in the database then generate list for forms
class List:
	def __init__(self, node_label):
		self.node_label=node_label
	#get lists of all nodes with properties as dict
	def get_nodes(self):
		with driver.session() as session:
			return session.read_transaction(self._node_properties)
	def _node_properties(self, tx):
		node_label=self.node_label
		Cypher_node_list='MATCH (n: ' + node_label +') RETURN properties (n)'
		result = tx.run(Cypher_node_list, node_label=self.node_label)
		return [record[0] for record in result]
	#create tuple for forms: for given key pair (e.g. name, fullname)
	#use set to only get unique values	
	def create_list(self, key1, key2):
		#node_label=self.node_label
		#properties_dict = self.get_nodes()
		return [(node[key1], node[key2]) for node in self.get_nodes()]
	#gets nodes (defined by key:value of a property) connected by a relationship (rel)
	def get_connected(self, key, value, rel):
		self.key=key 
		self.rel=rel
		self.value=value
		with driver.session() as session:
			return session.read_transaction(self._get_connected)
	def _get_connected(self, tx):
		key=self.key
		rel=self.rel
		node_label=self.node_label
		Cypher_get_connected='MATCH (n: ' + node_label +' {' + key + ':{value}}) <- [:' \
			+ rel + '] - (r) RETURN properties (r)'
		result = tx.run(Cypher_get_connected, value=self.value)
		dict_result= [record[0] for record in result]
		return [(node['name'], node['name']) for node in dict_result]
	#get selected nodes (forms)
	def get_selected(self):
		all_nodes = self.get_nodes()
		selection = self.selection
		keyby=self.keyby
		selected_nodes = [node for node in all_nodes if node[keyby] in selection]
		return selected_nodes
	#creates the traits.trt file for import to Field-Book
	#may need to replace 'name' with 'trait' in header but doesn't seem to affect
	def create_trt(self, selection, keyby):
		#key by is the value from the form to search for a match with a key in the dict
		#this is originally in the tuple generated for the form
		self.keyby=keyby
		self.selection=selection
		selected_traits = self.get_selected()
		fieldnames = ['name',
			'format',
			'defaultValue',
			'minimum',
			'maximum',
			'details',
			'categories',
			'isVisible',
			'realPosition']
		trt = cStringIO.StringIO()
		writer = csv.DictWriter(trt, 
			fieldnames=fieldnames, 
			quoting=csv.QUOTE_ALL)
		writer.writeheader()
		for i, trait in enumerate(selected_traits):
			trait['realPosition'] = str(i+1)
			trait['isVisible'] ='True'
			writer.writerow(trait)
		trt.seek(0)
		return trt
