from app import app
from app.cypher import Cypher
from config import uri, driver
from flask import session

class Samples:
	def __init__ (self, tissue):
		self.tissue = tissue
	def find_tissue(self):
		with driver.session() as session:
			return session.read_transaction(self._find_tissue)
	def _find_tissue(self, tx):
		for record in tx.run(Cypher.tissue_find,
			tissue = self.tissue):
			return (record['tissue'])
	def add_tissue(self):
		with driver.session() as session:
			session.write_transaction(self._add_tissue)
	def _add_tissue(self, tx):
		tx.run(Cypher.tissue_add,
			tissue = self.tissue,
			username = session['username'])