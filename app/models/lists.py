import csv, cStringIO
from app import app
from app.cypher import Cypher
from passlib.hash	import bcrypt
from config import uri, driver
#Get dicts of values matching a node in the database then generate list for forms
class Lists:
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
	#may need to replace 'name' with 'trait' in header but doesn't seem to affect Field-Book
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
