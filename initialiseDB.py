#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import csv
# get the preliminary list of allowed emails from instance/config
from instance import config
# import shutil
from neo4j.v1 import GraphDatabase
# for grouper function
from itertools import izip_longest as zip_longest

# neo4j config
uri = "bolt://localhost:7687"
auth = (os.environ['NEO4J_USERNAME'], os.environ['NEO4J_PASSWORD'])
driver = GraphDatabase.driver(uri, auth=auth)

#inputs
allowed_emails = config.allowed_emails

partners = [
	{'OPERATES_IN':['France', 'Vietnam','Cameroon', 'Costa Rica', 'French Guiana', 'El Salvador'], 'BASED_IN':'France', 'name':'CIRAD', 'fullname':'Centre de Coopération Internationale en Recherche Agronomique pour le Développement'},
	{'OPERATES_IN':None, 'BASED_IN':'France', 'name':'Eurofins', 'fullname':'Eurofins Analytics'},
	{'OPERATES_IN':None, 'BASED_IN':'Portugal', 'name':'ISD', 'fullname':'Instituto Superior de Agronomia'},
	{'OPERATES_IN':None, 'BASED_IN':'Portugal', 'name':'NOVA ID FCT', 'fullname':'Associação para a Inovação e Desenvolvimento da FCT'},
	{'OPERATES_IN':None, 'BASED_IN':'Ireland', 'name':'NUIG', 'fullname':'National University of Ireland, Galway'},
	{'OPERATES_IN':None, 'BASED_IN':'Denmark', 'name':'UCPH', 'fullname':'Københavns Universitet'},
	{'OPERATES_IN':None, 'BASED_IN':'Italy', 'name':'IllyCaffe', 'fullname':'IllyCaffe S.P.A'},
	{'OPERATES_IN':None, 'BASED_IN':'Nicaragua', 'name':'NicaFrance', 'fullname':'Fundación Nicafrance'},
	{'OPERATES_IN':['Nicaragua'], 'BASED_IN':'Netherlands', 'name':'SNV', 'fullname':'Stichting SNV Nederlandse Ontwikkelingsorganisatie'},
	{'OPERATES_IN':None, 'BASED_IN':'France', 'name':'IRD', 'fullname':'Institut de Recherche pour le Développement'},
	{'OPERATES_IN':None, 'BASED_IN':'France', 'name':'UM', 'fullname':'Université de Montpellier'},
	{'OPERATES_IN':None, 'BASED_IN':'Germany', 'name':'MPG', 'fullname':'Max-Planck-Gesellschaft zur Förderung der Wissenschaften e. V.'},
	{'OPERATES_IN':None, 'BASED_IN':'Germany', 'name':'RWTH', 'fullname':'Rheinisch-Westfälische Technische Hochschule Aachen'},
	{'OPERATES_IN':None, 'BASED_IN':'United States of America', 'name':'WCR', 'fullname':'World Coffee Research'},
	{'OPERATES_IN':None, 'BASED_IN':'United States of America', 'name':'ABR', 'fullname':'Arizona State University'},
	{'OPERATES_IN':None, 'BASED_IN':'Cameroon', 'name':'IRAD', 'fullname':'Institut de Recherche Agricole pour le Développement'},
	{'OPERATES_IN':None, 'BASED_IN':'Vietnam', 'name':'ICRAF', 'fullname':'International Center for Research in AgroForestry'},
	{'OPERATES_IN':None, 'BASED_IN':'Vietnam', 'name':'NOMAFSI', 'fullname':'Northern Mountainous Agriculture and Forestry Science Institute'},
	{'OPERATES_IN':None, 'BASED_IN':'Sweden', 'name':'Arvid', 'fullname':'Arvid Nordquist HAB'},
	{'OPERATES_IN':None, 'BASED_IN':'Vietnam', 'name':'AGI', 'fullname':'Agricultural Genetics Institute'}
]

trial_varieties = config.varieties
el_frances_code = config.el_frances_code

# we store indexed traits as lower case for indexed matching e.g. name_lower = toLower(name)
# case insensitive indexed search isn't supported in base neo4j, at least up to v3
# it would be possible with manual indexing and Lucene query syntax

indexes = [
	#not needed if in unique constraints list
	#{'label': 'Country', 'property': 'name'},
	{'label': 'Region', 'property': 'name_lower'},
	{'label': 'Farm', 'property': 'name_lower'},
	{'label': 'Plot', 'property': 'name_lower'},
	{'label': 'Block', 'property': 'id'},
	{'label': 'Tree', 'property': 'id'},
	{'label': 'Branch', 'property': 'id'},
	{'label': 'Leaf', 'property': 'id'},
	{'label': 'Sample', 'property': 'id'},
	{'label': 'Data', 'property': 'time'}
]

# Node Key constraint requires Neo4j Enterprise Edition
# we work around this by using two labels on the trait
# one generic Trait label and one specific level+trait e.g. TreeTrait
# then create the constraint on that unique label

constraints = [
	{'node': 'User', 'property': 'username_lower', 'constraint': 'IS UNIQUE'},
	{'node': 'Partner', 'property': 'name_lower', 'constraint': 'IS UNIQUE'},
	{'node': 'FarmTrait','property': 'name_lower', 'constraint': 'IS UNIQUE'},
	{'node': 'PlotTrait', 'property': 'name_lower', 'constraint': 'IS UNIQUE'},
	{'node': 'BlockTrait', 'property': 'name_lower', 'constraint': 'IS UNIQUE'},
	{'node': 'TreeTrait', 'property': 'name_lower', 'constraint': 'IS UNIQUE'},
	{'node': 'BranchTrait', 'property': 'name_lower', 'constraint': 'IS UNIQUE'},
	{'node': 'LeafTrait', 'property': 'name_lower', 'constraint': 'IS UNIQUE'},
	{'node': 'SampleTrait', 'property': 'name_lower', 'constraint': 'IS UNIQUE'},
	{'node': 'Item', 'property': 'uid', 'constraint': 'IS UNIQUE'},
	{'node': 'Counter', 'property': 'uid', 'constraint': 'IS UNIQUE'},
	{'node': 'Country', 'property': 'name_lower', 'constraint': 'IS UNIQUE'},
	{'node': 'Storage', 'property': 'name_lower', 'constraint': 'IS UNIQUE'},
	{'node': 'Tissue', 'property': 'name_lower', 'constraint': 'IS UNIQUE'},
	{'node': 'Variety', 'property': 'name_lower', 'constraint': 'IS UNIQUE'}
]

#functions
#https://docs.python.org/3.1/library/itertools.html#recipes
def grouper(n, iterable, fillvalue=None):
	"grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
	args = [iter(iterable)] * n
	return zip_longest(*args, fillvalue=fillvalue)

def confirm(question):
	valid = {"yes": True, "y": True, "no": False, "n": False}
	prompt = " [y/n] "
	while True:
		sys.stdout.write(question + prompt)
		choice = raw_input().lower()
		if choice in valid:
			return valid[choice]
		else:
			sys.stdout.write("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")
#erase database 
def delete_database(tx):
#	shutil.rmtree("/var/lib/neo4j/data/databases/graph.db/")
	tx.run('MATCH (n) DETACH DELETE n')


class Create:
	def __init__ (self, username):
		self.username=username

	@staticmethod
	def indexes(tx, indexes):
		for item in indexes:
			tx.run('CREATE INDEX ON :' + item['label'] + '(' + item['property'] + ')')

	@staticmethod
	def constraints(tx, constraints):
		for constraint in constraints:
			node=constraint['node']
			prop=constraint['property']
			constraint=constraint['constraint']
			tx.run('CREATE CONSTRAINT ON (n:' + node + ') ASSERT (n.' + prop + ') ' + constraint)

	def user(self, tx):
		username=self.username
		user_create = tx.run(
			' MERGE (u:User {'
				' username_lower: toLower($username) '
			' }) '
			' ON CREATE SET '
				' u.username = $username, '
				' u.found = False, '
				' u.time = timestamp() '
			' ON MATCH SET '
				' u.found = True '
			' MERGE (u)-[:SUBMITTED]->(sub:Submissions) '
				' MERGE (sub)-[:SUBMITTED]->(counter:Counter {name: "plot", count: 0}) '
				' MERGE (sub)-[:SUBMITTED]->(:Emails {allowed : $allowed_emails})'
				' MERGE (sub)-[:SUBMITTED]->(:Partners) '
				' MERGE (sub)-[:SUBMITTED]->(:Trials) '
				' MERGE (sub)-[:SUBMITTED]->(locations:Locations) '
					' MERGE (locations)-[:SUBMITTED]->(:Countries) '
				' MERGE (sub)-[:SUBMITTED]->(traits:Traits) '
					' MERGE (traits)-[:SUBMITTED]->(:FarmTraits) '
					' MERGE (traits)-[:SUBMITTED]->(:PlotTraits) '
					' MERGE (traits)-[:SUBMITTED]->(:BlockTraits) '
					' MERGE (traits)-[:SUBMITTED]->(:TreeTraits) '
					' MERGE (traits)-[:SUBMITTED]->(:BranchTraits) '
					' MERGE (traits)-[:SUBMITTED]->(:LeafTraits) '
					' MERGE (traits)-[:SUBMITTED]->(:SampleTraits) '
				' MERGE (sub)-[:SUBMITTED]->(varieties : Varieties) '
					' MERGE (varieties)-[: SUBMITTED]->(:Inbreds) '
					' MERGE (varieties)-[: SUBMITTED]->(:Hybrids) '
					' MERGE (varieties)-[: SUBMITTED]->(:Grafts) ' 
				' MERGE (sub)-[:SUBMITTED]->(sd:SampleDescriptors) '
					' MERGE (sd)-[:SUBMITTED]->(:StorageMethods) '
					' MERGE (sd)-[:SUBMITTED]->(:Tissues) '
			' RETURN u.found ',
			username = username,
			allowed_emails = allowed_emails
		)
		result = [record[0] for record in user_create]
		if result[0]:
			print ('Found: User ' + username)
		else:
			print ('Created: User ' + username)

	def partners(self, tx, partner_list):
		for partner in partner_list:
			partner_create = tx.run(
				' MATCH (:User {'
						' username_lower : toLower($username) '
					' }) '
					' -[:SUBMITTED]->(sub:Submissions), '
						' (sub)-[:SUBMITTED]->(:Locations)-[:SUBMITTED]->(uc:Countries), '
						' (sub)-[:SUBMITTED]->(up:Partners) '
				' MERGE (up)-[s1:SUBMITTED]-(p:Partner { '
						' name_lower: toLower($name) '
					' }) '
					' ON CREATE SET '
						' p.name = $name, '
						' p.fullname = $fullname, '
						' p.found = False, '
						' s1.time = timestamp() '
					' ON MATCH SET '
						' p.found = True '
				' MERGE (uc)-[s2:SUBMITTED]-(c:Country {'
						' name_lower : toLower($based_in) '
					' }) '
					' ON CREATE SET '
						' c.name = $based_in, '
						' c.found = False, '
						' s2.time = timestamp() '
					' ON MATCH SET '
						' c.found = True '
				' MERGE (p)-[r:BASED_IN]->(c) '
					' ON CREATE SET '
						' r.found = False, '
						' r.time = timestamp() '
					' ON MATCH SET '
						' r.found = True '
				' RETURN p.found, c.found, r.found ',
				username = self.username,
				based_in = partner['BASED_IN'],
				operates_in = partner['OPERATES_IN'],
				fullname = partner['fullname'],
				name = partner['name'])
			for record in partner_create:
				if record['p.found'] and record['r.found'] and record['c.found']:
					print ('Found: ' + partner['name'] + ' BASED_IN ' + partner['BASED_IN'])
				elif record['p.found'] and not record['r.found'] and record['c.found']:
					print ('Created: ' + partner['name'] + ' BASED_IN ' + partner['BASED_IN'] + '(relationship only)')
				elif record['p.found'] and not record['r.found'] and not record['c.found']:
					print ('Created: ' + partner['name'] + ' BASED_IN ' + partner['BASED_IN'] + '(country and relationship only)')
				elif not record['p.found'] and not record['r.found'] and record['c.found']:
					print ('Created: ' + partner['name'] + ' BASED_IN ' + partner['BASED_IN'] + '(partner and relationship only)')
				elif not record['p.found'] and not record['r.found'] and not record['c.found']:
					print ('Created: ' + partner['name'] + ' BASED_IN ' + partner['BASED_IN'])
				else:
					print ('Error with merger of partner ' + partner['name'] + ' and/or BASED_IN relationship')
			#separated operates_in call because list of operates_in relationships makes output confusing
			operates_in = tx.run (
				' MATCH '
					' (:User {username_lower: toLower($username)}) '
						' -[:SUBMITTED]->(sub:Submissions), '
							' (sub)-[:SUBMITTED]->(:Locations)-[:SUBMITTED]->(uc:Countries), '
							' (sub)-[:SUBMITTED]->(:Partners)-[:SUBMITTED]-(p:Partner {name : $name}) '
				' UNWIND $operates_in AS x '
					' MERGE (uc)-[s:SUBMITTED]->(c:Country { '
							' name_lower:toLower(x)'
						' }) '
						' ON MATCH SET c.found = True '
						' ON CREATE SET '
							' c.name = x, '
							' c.found = False, '
							' s.time = timestamp() '
					' MERGE (p)-[r:OPERATES_IN]->(c) '
						' ON MATCH SET r.found = True '
						' ON CREATE SET r.found = False, r.time = timestamp() '
					' return p.name, c.name, c.found, r.found ',
				username = 'start',
				name = partner['name'],
				operates_in = partner['OPERATES_IN']
			)
			for record in operates_in:
				if record['c.found'] and record['r.found']:
					print ('Found: ' + record['p.name'] + ' OPERATES_IN ' + record['c.name'])
				elif record['c.found'] and not record['r.found']:
					print ('Created: '  + record['p.name'] + ' OPERATES_IN ' + record['c.name'] + '(relationship only)')
				elif not record['c.found'] and not record['r.found']:
					print ('Created: '  + record['p.name'] + ' OPERATES_IN ' + record['c.name'])
				else:
					print ('Error with merger of relationship OPERATES_IN for ' + record['p.name'])

	def traits(self, tx, traits_file, level):
		with open (traits_file, 'rb') as traits_csv:
			reader = csv.DictReader(traits_csv, delimiter = ',', quotechar = '"')
			for trait in reader: 
				trait_create = tx.run(
					'MATCH (u : User {username_lower : toLower(trim($username))}) '
						' -[:SUBMITTED]->(:Submissions) '
						' -[:SUBMITTED]->(uts:Traits) '
						' -[s1:SUBMITTED]-(ut:' + level + 'Traits) '
					' MERGE '
					#creating with both generic "trait" label, but also the specific level+trait attribute 
						' (ut)-[s2:SUBMITTED]->(t:Trait:' + level + 'Trait { '
							' name_lower: toLower(trim($trait))'
						' }) '
						' ON CREATE SET '
							' t.level = toLower(trim($level)), '
							' t.group = toLower(trim($group)), '
							' t.name = $trait, '
							' t.format = toLower(trim($format)), '
							' t.default_value = CASE '
							'	WHEN size(trim($default_value)) = 0 '
							'	THEN Null '
							'	ELSE $default_value '
							'	END, '
							' t.minimum = CASE '
							'	WHEN size(trim($minimum)) = 0 '
							'	THEN Null '
							'	ELSE $minimum '
							'	END, '
							' t.maximum = CASE '
							'	WHEN size(trim($maximum)) = 0 '
							'	THEN Null '
							'	ELSE $maximum '
							'	END, '
							' t.details = $details, '
							' t.categories_fb = CASE '
							'	WHEN size(trim($categories)) = 0 '
							'	THEN Null '
							' 	Else $categories '
							'	END, '
							' t.category_list  = CASE '
							'	WHEN size(trim($categories)) = 0 '
							'	THEN Null '
							' 	Else split($categories, "/") '
							'	END, '
							' t.found = False, '
							' s2.time = timestamp() '
						' ON MATCH SET t.found = True '
					' RETURN t.found ',
						username = self.username,
						group = trait['group'],
						level = level,
						trait = trait['name'],
						format = trait['format'],
						default_value = trait['defaultValue'],
						minimum = trait['minimum'],
						maximum = trait['maximum'],
						details = trait['details'],
						categories = trait['categories'])
				for record in trait_create:
					if record['t.found']:
						print ('Found: ' + level + 'Trait ' + trait['name'])
					elif not record['t.found']:
						print ('Created: '+ level + 'Trait ' + trait['name'])
					else:
						print ('Error with merger of ' + level +'Trait ' + trait['name'])
	def varieties(self, tx, varieties):
		#first build the trials connected country
		trials = {"props":[]}
		for wp in trial_varieties:
			for trial in wp['trials']:
				trials['props'].append({
					'number':trial['number'],
					'name':trial['name'],
					'wp':wp['WP'],
					'country':trial['country']
					})
		trial_create = tx.run(
			' MATCH (u:User {username_lower: toLower($username)}) '
				' -[:SUBMITTED]->(:Submissions) '
				' -[:SUBMITTED]->(ut:Trials)  ' 
				' WITH ut '
				' UNWIND $trials as trial '
					' MATCH (country: Country {name_lower: toLower(trim(trial.country))}) '
					' MERGE (ut) '
						' -[s:SUBMITTED]->(n : Trial {'
								' name_lower : toLower(trim(trial.name)) '
							'}) '
						' -[:PERFORMED_IN]->(country) '
						' ON MATCH SET'
							' n.found = True '
						' ON CREATE SET '
							' n.name = trial.name, '
							' n.number = toInteger(trial.number), '
							' n.work_package = toInteger(trial.wp), '				
							' s.time = timestamp(), n.found = False '
					' RETURN n.number, n.name, country.name, n.found ',
			username = self.username,
			trials = trials['props']
		)
		print 'Creating trials:'
		for record in trial_create:
			if record['n.found']:
				print("Trial already registered: " + str([record[0], record[1], record[2]]))
			else : print record[0], record[1], record[2]
		#build a list of varieties connected to trial and group
		varieties = list()
		for wp in trial_varieties:
			for trial in wp['trials']:
				for type in trial['varieties']:
					for variety in trial['varieties'][type]:
						varieties.append({
							'name': variety,
							'trial': trial['number'],
							'type': type.lower()
						})
		variety_create = tx.run(
			' MATCH '
				' (u:User {username_lower: toLower($username)}) '
					' -[:SUBMITTED]->(:Submissions) '
					' -[:SUBMITTED]->(varieties:Varieties) '
			' UNWIND $varieties as variety '
				' MATCH (trial:Trial {number: variety.trial}) '
				' MATCH (varieties)-[:SUBMITTED]->(type) '
					' WHERE toLower(labels(type)[0]) contains toLower(variety.type) '
				' MERGE '
					' (var : Variety { '
						' name_lower: toLower(variety.name), '
						' type : toLower(variety.type) '
						' }'
					' ) '
					' ON CREATE SET '
						' var.name = variety.name, '
						' var.found = False '
					' ON MATCH SET '
						' var.found = True '
				' MERGE '
					' (var)-[r1: GROWN_IN]->(trial) '
					' ON CREATE SET r1.time = timestamp() '
				' MERGE '
					' (type)-[r2: SUBMITTED]->(var) '
					' ON CREATE SET r2.time = timestamp() '
			' RETURN var.name, var.found ',
			username = self.username,
			varieties = varieties
		)
		print ("Creating varieties:")
		for record in variety_create:
			print ("Created" if record[1] else "Found"), record[0]
		##then build in the relationships for hybrids to their parents
		inbreds = []
		hybrids = []
		grafts = []
		for variety in varieties:
			if variety['type'] == 'inbred':
				inbreds.append(variety['name'])
			elif variety['type'] == 'hybrid':
				hybrids.append(variety['name'])
			elif variety['type'] == 'graft':
				grafts.append(variety['name'])
			else:
				pass
		for variety in hybrids:
			parents = variety.split(' x ')
			if len(parents) > 1:
				maternal = parents[0]
				paternal = parents[1]
				link_hybrid =  tx.run(
					' MATCH (var:Variety {name_lower :toLower($variety)}) '
					' MERGE (mat:Variety {name_lower:toLower($maternal)}) '
						' ON CREATE SET '
							' mat.name = $maternal, '
							' mat.found = False '
						' ON MATCH SET mat.found = True '
					' MERGE (pat: Variety {name_lower: toLower($paternal)}) '
						' ON CREATE SET '
							' pat.name = $paternal, '
							' pat.found = False '
						' ON MATCH SET pat.found = True '
					' MERGE (var)-[m:MATERNAL_DONOR]->(mat) '
						' ON CREATE SET m.found = False '
						' ON MATCH SET m.found = True '
					' MERGE (var)-[p:PATERNAL_DONOR]->(pat) '
						' ON CREATE SET p.found = False '
						' ON MATCH SET p.found = True '
					' RETURN var.name, mat.name, mat.found, pat.name, pat.found, m.found, p.found ',
						variety = variety,
						maternal = maternal,
						paternal = paternal)
				for record in link_hybrid:
					if record['mat.found']:
						print ('Maternal donor variety is already registered: ' + record['mat.name'] )
					elif not record['mat.found'] :
						print ('Maternal donor variety created: ' + record['mat.name'] )
					if record['pat.found']:
						print ('Paternal donor variety is already registered: ' + record['pat.name'] )
					elif not record['pat.found']:
						print ('Paternal donor variety created: ' + record['pat.name'] )
					if record['m.found']:
						print('Maternal donor relationship already established between ' + record['var.name'] + " and " + record['mat.name'] )
					if not record['m.found']:
						print('Maternal donor relationship created between ' + record['var.name'] + " and " + record['mat.name'] )
					if record['p.found']:
						print('Paternal donor relationship already established between ' + record['var.name'] + " and " + record['pat.name'] )
					if not record['p.found']:
						print('paternal donor relationship created between ' + record['var.name'] + " and " + record['pat.name'] )
		##and same for grafts
		for variety in grafts:
			grafted = variety.split(' \xe2\x81\x84 ')
			if len(grafted) > 1:
				scion = grafted[0]
				rootstock = grafted[1]
				link_graft =  tx.run(
					' MATCH (var: Variety {name_lower:toLower($variety)}) '
					' MERGE (scion: Variety {name_lower: toLower($scion)}) '
						' ON CREATE SET '
							' scion.name = $scion, '
							' scion.found = False '
						' ON MATCH SET scion.found = True '
					' MERGE (rootstock:Variety {name_lower: toLower($rootstock)}) '
						' ON CREATE SET '
							' rootstock.name = $name, '
							' rootstock.found = False '
						' ON MATCH SET rootstock.found = True '
					' MERGE (var)-[s:SCION]->(scion) '
						' ON CREATE SET s.found = False '
						' ON MATCH SET s.found = True '
					' MERGE (var)-[r:ROOTSTOCK]->(rootstock) '
						' ON CREATE SET r.found = False '
						' ON MATCH SET r.found = True '
					' RETURN var.name, scion.name, scion.found, rootstock.name, rootstock.found, s.found, r.found ',
						variety = variety,
						scion = scion,
						rootstock = rootstock)
				for record in link_graft:
					if record['scion.found']:
						print ('Scion variety is already registered: ' + record['scion.name'] )
					elif not record['scion.found']:
						print ('Scion variety created: ' + record['scion.name'] )
					if record['rootstock.found']:
						print ('Rootstock variety is already registered: ' + record['rootstock.name'] )
					elif not record['rootstock.found']:
						print ('Rootstock variety created: ' + record['rootstock.name'] )
					if record['s.found']:
						print('Scion relationship already established between ' + record['var.name'] + " and " + record['scion.name'] )
					if not record['s.found']:
						print('Scion donor relationship created between ' + record['var.name'] + " and " + record['scion.name'] )
					if record['r.found']:
						print('Rootstock relationship already established between ' + record['var.name'] + " and " + record['rootstock.name'] )
					if not record['r.found']:
						print('Rootstock relationship created between ' + record['var.name'] + " and " + record['rootstock.name'] )
		# # Assign variety as a text field but with list of categories still and later check for categories when uploading
		variety_list = []
		for i in sorted(inbreds):
			if i not in variety_list:
				variety_list.append(i)
		for i in sorted(hybrids):
			if i not in variety_list:
				variety_list.append(i)
		for i in sorted(grafts):
			if i not in variety_list:
				variety_list.append(i)
		variety_trait_create = tx.run(
			'MATCH (u:User {username_lower: toLower(trim($username))}) '
				' -[:SUBMITTED]->(: Submissions) '
				' -[:SUBMITTED]->(: Traits) '
				' -[:SUBMITTED]-(ut: TreeTraits) '
			' WITH ut '
			' MERGE (ut)-[s2: SUBMITTED]->(t: Trait: TreeTrait { '
					' name_lower: "variety name (text)" '
				' }) '
				' ON CREATE SET '
					' t.name = "Variety name (text)", '
					' t.level = "tree", '
					' t.group = "general", '
					' t.format = "text", '
					' t.category_list = $varieties, ' 				
					' t.details = "Variety name as text", '
					' t.found = False, '
					' s2.time = timestamp() '
				' ON MATCH SET t.found = True '
			' RETURN t.name, t.found, t.category_list',
			username = self.username,
			varieties = variety_list
		)
		for record in variety_trait_create:
			print ("Created trait: " if not record[1] else "Found:"), record[0]
	def el_frances_code (self, tx, el_frances_code):
		for item in el_frances_code:
			if item[1].lower() == item[2].lower():
				result = tx.run(
					' MERGE (var:Variety { name_lower: toLower($variety) } ) '
						' ON CREATE SET '
							' var.name = $variety, '
							' var.found = False '
						' ON MATCH SET '
							' var.found = True '
					' SET var.el_frances_code = $code '
					' SET var.el_frances_code_lower = toLower($code) '
					' RETURN var.found ',
					code = str(item[0]).lower(),
					variety = str(item[1]).lower()
				)
			else:
				result = tx.run (
					' MERGE (mat:Variety {name_lower: toLower($maternal)}) '
						' ON CREATE SET '
							' mat.name = $maternal '
					' MERGE (pat:Variety {name_lower: toLower($paternal)}) '
						' ON CREATE SET '
							' pat.name = $paternal '
					' MERGE '
						' (mat)<-[:MATERNAL_DONOR]-(var:Variety) '
							' -[:PATERNAL_DONOR]->(pat) '
					' ON CREATE SET '
						' var.name = ($maternal + " x " + $paternal), '
						' var.found = False '
					' ON MATCH SET '
						' var.found = True '
					' SET '
						' var.el_frances_code = $code, '
						' var.el_frances_code_lower = toLower($code) '
					' RETURN var.found ',
					code = str(item[0]).lower(),
					maternal = str(item[1]).lower(),
					paternal = str(item[2]).lower()
				)
			print str(item[0]).lower(), str(item[1]).lower(), str(item[2]).lower()
			for record in result:
				print "Merging el Frances codes"
				if record[0] == True:
					print "Existing variety, code added"
				else:
					print "New variety created"
		# now create the TreeTrait
		code_list = list(set([str(i[0]).lower() for i in el_frances_code]))
		el_frances_code_trait_create = tx.run(
			'MATCH (u:User {username_lower:toLower(trim($username))}) '
				' -[:SUBMITTED]->(:Submissions) '
				' -[:SUBMITTED]->(:Traits) '
				' -[:SUBMITTED]-(ut:TreeTraits) '
				' WITH ut '
				' MERGE (ut)-[s2 : SUBMITTED]->(t : Trait: TreeTrait { '
						' name_lower: "el frances code (text)" '
					' }) '
					' ON CREATE SET '
						' t.level = "tree", '
						' t.group = "general", '
						' t.name = "El Frances Code (text)", '
						' t.format = "text", '
						' t.category_list = $code_list, '
						' t.details = "El Frances variety code", '
						' t.found = False, '
						' s2.time = timestamp() '
					' ON MATCH SET t.found = True '
				' RETURN t.name, t.found, t.category_list',
				username = self.username,
				code_list = code_list
		)
		for record in el_frances_code_trait_create:
				print ("Created trait: " if not record[1] else "Found:"), record[0]

if not confirm('Are you sure you want to proceed? This is should probably only be run when setting up the database'):
	sys.exit()
else:
	if confirm('Do you want to a wipe existing data, rebuild the constraints and reset the indexes?'):
		print('Performing a full reset of database')
		with driver.session() as session:
			session.write_transaction(delete_database)
			session.write_transaction(Create.constraints, constraints)
			session.write_transaction(Create.indexes, indexes)
	else: print('Attempting to create the following while retaining existing data:\n'
		'  * user:start \n'
		'  * partners \n'
		'  * traits')
	with driver.session() as session:
		session.write_transaction(Create('start').user)
		session.write_transaction(Create('start').partners, partners)
		session.write_transaction(Create('start').traits, 'traits/farm_traits.csv', 'Farm')
		session.write_transaction(Create('start').traits, 'traits/plot_traits.csv', 'Plot')
		session.write_transaction(Create('start').traits, 'traits/block_traits.csv', 'Block')
		session.write_transaction(Create('start').traits, 'traits/tree_traits.csv', 'Tree')
		session.write_transaction(Create('start').traits, 'traits/branch_traits.csv', 'Branch')
		session.write_transaction(Create('start').traits, 'traits/leaf_traits.csv', 'Leaf')
		session.write_transaction(Create('start').traits, 'traits/sample_traits.csv', 'Sample')
		session.write_transaction(Create('start').varieties, trial_varieties)
		session.write_transaction(Create('start').el_frances_code, el_frances_code)
	print ('Complete')