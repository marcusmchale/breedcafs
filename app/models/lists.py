import os
import unicodecsv as csv 
import cStringIO
from app import app
from app.cypher import Cypher
from passlib.hash	import bcrypt
from config import uri, driver
from datetime import datetime
#Get dicts of values matching a node in the database then generate list for forms
#careful with any node_label functions to prevent injection - don't allow user input assigned node_labels
class Lists:
	def __init__(self, node_label):
		self.node_label=node_label
	def find_node(self, name):
		self.name = name
		with driver.session() as session:
			return session.read_transaction(self._find_node)
	def _find_node(self, tx):
		node_label = self.node_label
		name = self.name
		Cypher_node_find='MATCH (n: ' + node_label +' {name:$name}) RETURN (n)'
		for record in tx.run(Cypher_node_find, name=self.name):
			return record
	#get lists of all nodes with properties as dict
	def get_nodes(self):
		with driver.session() as session:
			return session.read_transaction(self._node_properties)
	def _node_properties(self, tx):
		node_label=self.node_label
		Cypher_node_properties='MATCH (n: ' + node_label +') RETURN properties (n)'
		result = tx.run(Cypher_node_properties)
		return [record[0] for record in result]
	def create_list(self, key):
		return [(node[key]) for node in self.get_nodes()]
	#lists of tups for forms
	def create_list_tup(self, key1, key2):
		return [(node[key1], node[key2]) for node in self.get_nodes()]
	#Finds node (defined by key:value of a property) and gets 'name' from nodes connected by a relationship with label 'rel'
	def get_connected(self, key, value, rel):
		self.key=key 
		self.rel=rel
		self.value=value
		with driver.session() as session:
			result = session.read_transaction(self._get_connected)
			return [(record[0]['name']) for record in result]
	def _get_connected(self, tx):
		key = self.key
		rel = self.rel
		value = self.value
		node_label=self.node_label
		Cypher_get_connected=('MATCH (n: ' + node_label +' {' + key + ':$value}) <- [:' 
			+ rel + '] - (r) RETURN properties (r)')
		return tx.run(Cypher_get_connected, value=self.value)
	#get selected nodes (forms)
	def get_selected(self):
		all_nodes = self.get_nodes()
		selection = self.selection
		keyby=self.keyby
		selected_nodes = [node for node in all_nodes if node[keyby] in selection]
		return selected_nodes
	#creates the traits.trt file for import to Field-Book
	#may need to replace 'name' with 'trait' in header but doesn't seem to affect Field-Book
	def create_trt(self, username, selection, keyby, level):
		#get the data
		#key by is the value from the form to search for a match with a key in the dict
		#this is originally in the tuple generated for the form
		self.keyby=keyby
		self.selection=selection
		selected_traits = self.get_selected()
		#create user download path if not found
		if not os.path.isdir(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], username)):
			os.mkdir(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], username))
		#set variables for file creation
		fieldnames = ['name',
			'format',
			'defaultValue',
			'minimum',
			'maximum',
			'details',
			'categories',
			'isVisible',
			'realPosition']
		time = datetime.now().strftime('%Y%m%d-%H%M%S')
		filename = time + '_' + level + '.trt'
		file_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], username, filename)
		#make the file
		with open (file_path, 'w') as file:
			writer = csv.DictWriter(file, 
				fieldnames=fieldnames, 
				quoting=csv.QUOTE_ALL,
				extrasaction='ignore')
			writer.writeheader()
			for i, trait in enumerate(selected_traits):
				trait['realPosition'] = str(i+1)
				trait['isVisible'] ='True'
				writer.writerow(trait)
			file_size = file.tell()
		return {"filename":filename, "file_path":file_path, "file_size":file_size}
