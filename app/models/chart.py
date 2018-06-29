from app import app, ServiceUnavailable, AuthError
from app.cypher import Cypher
from neo4j_driver import get_driver
from datetime import datetime
from flask import jsonify
#Get dicts of values matching a node in the database then generate list for forms

class Chart:
	#def __init__(self):
	#	pass
	new = 'test'
	def _get_submissions_range(self, tx):
		result = [record for record in tx.run(Cypher.get_submissions_range, 
			username=self.username, 
			starttime=self.starttime, 
			endtime=self.endtime)]
		return result
	#get lists of submitted nodes (rels and directly linked nodes too) in json format
	def get_submissions_range(self, username, startdate, enddate):
		try:
			self.username=username
			epoch = datetime.utcfromtimestamp(0)
			self.starttime =  ((datetime.strptime(startdate, '%Y-%m-%d')-epoch).total_seconds())*1000
			self.endtime =  ((datetime.strptime(enddate, '%Y-%m-%d')-epoch).total_seconds())*1000
			with get_driver().session() as neo4j_session:
				records = neo4j_session.read_transaction(self._get_submissions_range)
			#collect all nodes/rels from records into lists of dicts
			nodes=[]
			rels=[]
			for record in records:
				nodes.extend(({'id':record['d_id'],'label':record['d_label'],'name':record['d_name']},
					{'id':record['n_id'],'label':record['n_label'],'name':record['n_name']}))
				rels.append({'id':record['r_id'],'source':record['r_start'],'type':record['r_type'],'target':record['r_end']})
			#connect counters to block/plot
			for node in nodes:
				if str(node['id']).endswith('_count_node'):
					rels.append({'source':node['id'],
						'type':'FROM_COUNTER',
						'id':(node['id'] + '_' + node['id'].split('_')[0]),
						'target': int(node['id'].split('_')[0])})
			#then uniquify
			nodes={node['id']:node for node in nodes}.values()
			rels={rel['id']:rel for rel in rels}.values()
			#and create the d3 input
			return jsonify({"nodes":nodes, "links":rels})
		except (ServiceUnavailable, AuthError):
			return jsonify({"status":"Database unavailable"})
	def _get_plots_treecount(self, tx):
		return [record for record in tx.run(Cypher.get_plots_treecount)]
	#get lists of submitted nodes (rels and directly linked nodes too) in json format
	def get_plots_treecount(self):
		with get_driver().session() as neo4j_session:
			records = neo4j_session.read_transaction(self._get_plots_treecount)
		#collect all nodes/rels from records into nested dict
		nested = {'name':'nodes','label':'root_node','children':[record[0] for record in records]}
		return jsonify(nested)