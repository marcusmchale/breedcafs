from neo4j_driver import get_driver, neo4j_query


# Get dicts of values matching a node in the database then generate list for forms
# careful with any node_label functions to prevent injection - don't allow user input assigned node_labels
class Lists:
	def __init__(self, node_label):
		self.node_label = node_label

	def find_node(self, name):
		self.name = name
		with get_driver().session() as neo4j_session:
			return neo4j_session.read_transaction(self._find_node)

	def _find_node(self, tx):
		node_label = self.node_label
		name = self.name
		Cypher_node_find='MATCH (n: ' + node_label +' {name_lower:toLower($name)}) RETURN (n)'
		for record in tx.run(Cypher_node_find, name=self.name):
			return record
	# get lists of all nodes with properties as dict

	def get_nodes(self):
		with get_driver().session() as neo4j_session:
			return neo4j_session.read_transaction(self._node_properties)

	def _node_properties(self, tx):
		node_label=self.node_label
		Cypher_node_properties='MATCH (n: ' + node_label +') RETURN properties (n)'
		result = tx.run(Cypher_node_properties)
		return [record[0] for record in result]

	def create_list(self, key):
		return [(node[key]) for node in self.get_nodes()]

	# lists of tups for forms
	def create_list_tup(self, key1, key2):
		return [(node[key1].lower(), node[key2]) for node in self.get_nodes()]

	# Finds node (defined by key:value of a property) and gets 'name'
	# from nodes connected by a relationship with label 'rel'
	def get_connected(self, key, value, rel):
		query =  (
			'MATCH (n: ' + self.node_label + ' {' + key + '_lower:toLower($value)}) <- [:'
			+ rel + '] - (r) RETURN properties (r)'
		)
		parameters = {
				'value': value
			}
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(neo4j_query, query, parameters)
			names_list = []
			for record in result:
				if 'name' in record[0]:
					names_list.append(record[0]['name'])
				else:
					pass
		return names_list

	# get selected nodes (forms)
	def get_selected(self, selection, keyby):
		all_nodes = self.get_nodes()
		selected_nodes = [node for node in all_nodes if node[keyby] in selection]
		return selected_nodes
