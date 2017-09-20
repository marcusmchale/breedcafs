from app import app
from app.cypher import Cypher
from config import uri, driver
from flask import session

class FieldDetails:
	def __init__(self):
		pass
	def add_soil(self, soil):
		self.soil = soil
		with driver.session() as session:
			session.write_transaction(self._add_soil)
	def _add_soil(self, tx):
		tx.run(Cypher.soil_add,
			soil = self.soil,
			username = session['username'])
	def add_shade_tree(self, shade_tree):
		self.shade_tree = shade_tree
		with driver.session() as session:
			session.write_transaction(self._add_shade_tree)
	def _add_shade_tree(self, tx):
		tx.run(Cypher.shade_tree_add,
			shade_tree = self.shade_tree,
			username = session['username'])
	def update(self, plotID, soil, shade_trees):
		self.plotID = plotID
		self.soil = soil
		self.shade_trees = shade_trees
		with driver.session() as session:
			session.write_transaction(self._update)
	def _update(self, tx):
		tx.run(Cypher.field_details_update,
			plotID = self.plotID,
			soil = self.soil,
			shade_trees = self.shade_trees,
			username = session['username'])