#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import csv
#get the preliminary list of allowed emails from instance/config
from instance import config
#import shutil
from neo4j.v1 import GraphDatabase
#for grouper function
from itertools import izip_longest as zip_longest

#neo4j config
uri = "bolt://localhost:7687"
auth = (os.environ['NEO4J_USERNAME'], os.environ['NEO4J_PASSWORD'])
driver = GraphDatabase.driver(uri, auth=auth)

#inputs
ALLOWED_EMAILS = config.ALLOWED_EMAILS

PARTNERS = ({'OPERATES_IN':['France', 'Vietnam','Cameroon', 'Costa Rica', 'French Guiana', 'El Salvador'], 'BASED_IN':'France', 'name':'CIRAD', 'fullname':'Centre de Coopération Internationale en Recherche Agronomique pour le Développement'},
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
{'OPERATES_IN':None, 'BASED_IN':'Vietnam', 'name':'AGI', 'fullname':'Agricultural Genetics Institute'})

VARIETIES = config.VARIETIES

CONSTRAINTS = ({'node':'User', 'property':'username', 'constraint':'IS UNIQUE'},
	{'node':'Partner', 'property':'name', 'constraint':'IS UNIQUE'},
	{'node':'SampleTrait', 'property':'name', 'constraint':'IS UNIQUE'},
	{'node':'TreeTrait', 'property':'name', 'constraint':'IS UNIQUE'},
	{'node':'BlockTrait', 'property':'name', 'constraint':'IS UNIQUE'},
	{'node':'PlotTrait', 'property':'name', 'constraint':'IS UNIQUE'},
	{'node':'Plot', 'property':'uid', 'constraint':'IS UNIQUE'},
	{'node':'Block', 'property':'uid', 'constraint':'IS UNIQUE'},
	{'node':'Tree', 'property':'uid', 'constraint':'IS UNIQUE'},
	{'node':'Sample', 'property':'uid', 'constraint':'IS UNIQUE'},
	{'node':'Counter', 'property':'uid', 'constraint':'IS UNIQUE'},
	{'node':'Country', 'property':'name', 'constraint':'IS UNIQUE'},
	{'node':'Storage', 'property':'name', 'constraint':'IS UNIQUE'},
	{'node':'Tissue', 'property':'name', 'constraint':'IS UNIQUE'},
	{'node':'Variety', 'property':'name', 'constraint':'IS UNIQUE'})


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
	tx.run('MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r')




class Create:
	def __init__ (self, username):
		self.username=username
	@staticmethod
	def constraints(tx, constraints):
		for constraint in constraints:
			node=constraint['node']
			prop=constraint['property']
			constraint=constraint['constraint']
			tx.run('CREATE CONSTRAINT ON (n:' + node + ') ASSERT (n.' + prop + ') ' + constraint)
	def user(self, tx):
		username=self.username
		user_create = tx.run(' MERGE (u:User {username:$username}) '
			' ON MATCH SET u.found="TRUE" '
			' ON CREATE SET u.found="FALSE", u.time=timestamp() '
			' MERGE (u)-[:SUBMITTED]->(sub:Submissions) '
				' -[:SUBMITTED]->(locations:Locations) '
				' -[:SUBMITTED]->(:Countries) '
			' MERGE (sub)-[:SUBMITTED]->(:Partners) '
			' MERGE (sub)-[:SUBMITTED]->(:Traits) '
			' MERGE (sub)-[:SUBMITTED]->(:Trials) '
			' MERGE (sub)-[:SUBMITTED]->(:Varieties) '
			' MERGE (sub)-[:SUBMITTED]->(:Emails {allowed : $allowed_emails})'
			' RETURN u.found', 
			username=username,
			allowed_emails = ALLOWED_EMAILS)
		for record in user_create:
			if record['u.found'] == 'TRUE' :
				print ('Found: User ' + username)
			if record['u.found'] == 'FALSE' :
				print ('Created: User ' + username)
	def partners(self, tx, partners):
		for partner in partners: 
			partner_create = tx.run(' MATCH (:User {username : $username}) '
					' -[:SUBMITTED]->(sub:Submissions), '
				' (sub)-[:SUBMITTED]->(:Locations)-[:SUBMITTED]->(uc:Countries), '
				' (sub)-[:SUBMITTED]->(up:Partners) '
				' MERGE (up)-[s1:SUBMITTED]-(p:Partner {name:$name, fullname:$fullname}) '
					' ON MATCH SET p.found="TRUE" '
					' ON CREATE SET p.found="FALSE", s1.time= timestamp() '
				' MERGE (uc)-[s2:SUBMITTED]-(c:Country {name : $based_in}) '
					' ON MATCH SET c.found ="TRUE"'
					' ON CREATE SET c.found ="FALSE", s2.time = timestamp() '
				' MERGE (p)-[r:BASED_IN]->(c)'
					' ON MATCH SET r.found="TRUE"'
					' ON CREATE SET r.found="FALSE", r.time = timestamp()'
				' RETURN p.found, c.found, r.found ',
				username=self.username,
				based_in=partner['BASED_IN'],
				operates_in=partner['OPERATES_IN'],
				fullname=partner['fullname'],
				name=partner['name'])
			for record in partner_create:
				if record['p.found'] == 'TRUE' and record['r.found'] == 'TRUE' and record['c.found'] == 'TRUE':
					print ('Found: ' + partner['name'] + ' BASED_IN ' + partner['BASED_IN'])
				elif record['p.found'] == 'TRUE' and record['r.found'] == 'FALSE'  and record['c.found'] == 'TRUE':
					print ('Created: ' + partner['name'] + ' BASED_IN ' + partner['BASED_IN'] + '(relationship only)')
				elif record['p.found'] == 'TRUE' and record['r.found'] == 'FALSE' and record['c.found'] == 'FALSE':
					print ('Created: ' + partner['name'] + ' BASED_IN ' + partner['BASED_IN'] + '(country and relationship only)')
				elif record['p.found'] == 'FALSE' and record['r.found'] == 'FALSE' and record['c.found'] == 'TRUE':
					print ('Created: ' + partner['name'] + ' BASED_IN ' + partner['BASED_IN'] + '(partner and relationship only)')
				elif record['p.found'] == 'FALSE' and record['r.found'] == 'FALSE' and record['c.found'] == 'FALSE':
					print ('Created: ' + partner['name'] + ' BASED_IN ' + partner['BASED_IN'])
				else:
					print ('Error with merger of partner ' + partner['name'] + 'and/or BASED_IN relationship' )
			#separated operates_in call because list of operates_in relationships makes output confusing
			operates_in = tx.run (' MATCH (:User {username: $username}) '
				' -[:SUBMITTED]->(sub:Submissions), '
				' (sub)-[:SUBMITTED]->(:Locations)-[:SUBMITTED]->(uc:Countries), '
				' (sub)-[:SUBMITTED]->(:Partners)-[:SUBMITTED]-(p:Partner {name : $name}) '
				' UNWIND $operates_in AS x '
				' MERGE (uc)-[s:SUBMITTED]->(c:Country {name:x}) '
					' ON MATCH SET c.found= "TRUE" '
					' ON CREATE SET c.found= "FALSE", s.time = timestamp() '
				' MERGE (p)-[r:OPERATES_IN]->(c) '
					' ON MATCH SET r.found = "TRUE" '
					' ON CREATE SET r.found = "FALSE", r.time = timestamp() '
				' return p.name, c.name, c.found, r.found ',
				username='start',
				name=partner['name'],
				operates_in=partner['OPERATES_IN'])
			for record in operates_in:
				if record['c.found'] == 'TRUE' and record['r.found'] == 'TRUE':
					print ('Found: ' + record['p.name'] + ' OPERATES_IN ' + record['c.name'])
				elif record['c.found'] == 'TRUE' and record['r.found'] == 'FALSE':
					print ('Created: '  + record['p.name'] + ' OPERATES_IN ' + record['c.name'] + '(relationship only)')
				elif record['c.found'] == 'FALSE' and record['r.found'] == 'FALSE':
					print ('Created: '  + record['p.name'] + ' OPERATES_IN ' + record['c.name'])
				else:
					print ('Error with merger of relationship OPERATES_IN for ' + record['p.name'])
	def traits(self, tx, traits_file, level):
		with open (traits_file, 'rb') as traits_csv:
			reader = csv.DictReader(traits_csv, delimiter=',', quotechar='"')
			for trait in reader: 
				trait_create = tx.run('MATCH (u:User {username:$username}) '
					' -[:SUBMITTED]->(:Submissions)-[:SUBMITTED]->(uts:Traits) '
					' MERGE (uts)-[s1:SUBMITTED]-(ut:' + level + 'Traits) '
						' ON CREATE SET s1.time = timestamp() '
					' MERGE (ut)-[s2:SUBMITTED]->(t:' + level +'Trait {group: $group, '
						' name: $trait, '
						' format: $format, '
						' defaultValue: $defaultValue, '
						' minimum: $minimum, '
						' maximum: $maximum,'
						' details: $details, '
						' categories: $categories}) '
					' ON MATCH SET t.found="TRUE" '
					' ON CREATE SET t.found="FALSE", s2.time=timestamp() '
					' RETURN t.found',
						username=self.username,
						group=trait['group'],
						trait=trait['name'],
						format=trait['format'],
						defaultValue=trait['defaultValue'],
						minimum=trait['minimum'],
						maximum=trait['maximum'],
						details=trait['details'],
						categories=trait['categories'])
				for record in trait_create:
					if record['t.found']=='TRUE':
						print ('Found: ' + level + 'Trait ' + trait['name'])
					elif record['t.found]'=='FALSE']:
						print ('Created: '+ level + 'Trait ' + trait['name'])
					else:
						print ('Error with merger of ' + level +'Trait ' + trait['name'])
	def varieties(self, tx, varieties):
		#first build the trials connected country
		trials = {"props":[]}
		for wp in VARIETIES:
			for trial in wp['trials']:
				trials['props'].append({
					'number':trial['number'],
					'name':trial['name'],
					'wp':wp['WP'],
					'country':trial['country']
					})
		trial_create = tx.run(' MATCH (u:User {username:$username}) '
				' -[:SUBMITTED]->(:Submissions) '
				' -[:SUBMITTED]->(ut:Trials)  ' 
				' WITH ut '
				' UNWIND $trials as trial'
				' MATCH (country:Country {name: trial.country})'
				' CREATE (ut) '
					' -[s:SUBMITTED {time:timestamp()}]->(n:Trial) '
					' -[:PERFORMED_IN]->(country) '
				' SET n = trial '
				' RETURN n.number, n.name, country.name',
			username = self.username,
			trials = trials['props'])
		print 'Creating trials:'
		for record in trial_create:
			print record[0], record[1], record[2]
		#then merge varieties, connect to trial and group (inbred/hybrid/)
		varieties = {"props":[]}
		for wp in VARIETIES:
			for trial in wp['trials']:
				for group in trial['varieties']:
					for variety in trial['varieties'][group]:
						varieties['props'].append({
							'name':variety,
							'trial':trial['number'],
							'group':group
							})
		#then create a TreeTrait per trial to store the varieties.
		#This splits it up to make it more relevant per user
		#also long lists of multicats don't work in fieldbook, the last ones are hidden.
		variety_trait_create = tx.run( 'MATCH (u:User {username:$username}) '
				' -[:SUBMITTED]->(:Submissions) '
				' -[:SUBMITTED]->(uts:Traits) '
				' MERGE (uts)-[s1:SUBMITTED]-(ut:TreeTraits) '
					' ON CREATE SET s1.time = timestamp() '
				' WITH ut '
				' MATCH (trial:Trial) '
				' MERGE (ut)-[s2:SUBMITTED]->(t:TreeTrait {group: "general", '
						' name: ("Varieties (Trial " + trial.number + ")"),  '
						' format: "multicat", '
						' defaultValue: "", '
						' minimum: "", '
						' maximum: "",'
						' details: ("Varieties (Trial #" + trial.number + ", " + trial.name + ")"), '
						' categories: ""}) '
					' -[:TRAIT_FOR]->(trial) '	
				' ON MATCH SET t.found="TRUE" '
				' ON CREATE SET t.found="FALSE", s2.time=timestamp() '
				' RETURN t.name, t.found',
			username = self.username,
			)
		for record in variety_trait_create:
			print ("Created: " if record[1] else "Found:"), record[0]
		#now add the specific list of categories to each trait
		#the order is important here, 
		#and the height of the row in fieldbook is set by the last button
		#so this should be the longest string
		for wp in VARIETIES:
			for trial in wp['trials']:
				varieties = []
				##creating list with inbred first and grafted last
				for group in ['inbred','hybrid','grafted']:
					if group in trial['varieties']:
						varieties.extend(trial['varieties'][group])
				varieties3 = []
				for var3 in grouper(3, varieties):
					var3 = filter(None, list(var3))
					var3.sort(key = lambda s: len(s))
					varieties3.extend(var3)
				categories = str("/".join(varieties3))
				trait_categories_create = tx.run( ' MATCH (tt:TreeTrait '
					' {name: ("Varieties (Trial " + $trial + ")")})  '
					' SET tt.categories = $categories '
					' RETURN tt.name, tt.categories ',
					trial = trial['number'],
					categories = categories)
				for record in trait_categories_create:
					print ("Set " + record[0] + " categories as " + record[1])



#sorted(varieties[group])
		#then create a TreeTrait per 


##then group these into 3 and sorted by length
#category_list_grouped = []
#for group_3 in grouper(3, category_list):
#	print(group_3)
#	group_3 = filter(None, list(group_3))
#	print(group_3)
#	group_3.sort(key = lambda s: len(s))
#	print(group_3)
#	category_list_grouped.extend(group_3)
#categories = str("/".join(category_list_grouped))
#		create_categories = tx.run (' MATCH (tt:TreeTrait {name:"variety"}) '
#			' SET tt.categories = $categories '
#			' RETURN tt.categories ',
#			categories = categories)
#		for record in create_categories:
#			print ('Created categories list on TreeTrait (variety): ' + record['tt.categories'])




###		variety_create = tx.run(
###				'MATCH (u:User {username:$username}) '
###						' -[:SUBMITTED]->(:Submissions) '
###						' -[:SUBMITTED]->(uv:Varieties), ' 
###					' (tt:TreeTrait {name:"variety"}) '
###				' UNWIND $varieties AS variety '
###				' MERGE (uv) '
###					'-[s1:SUBMITTED]->(var:Variety {name:variety.name}) '
###					' ON CREATE SET s1.time=timestamp(), var.found = "FALSE" '
###					' ON MATCH SET var.found = "TRUE" '
###				' MERGE (group:VarietyGroup {name:variety.group}) '
###				' MERGE (var)-[r:OF_CATEGORY]->(group) '
###					' ON CREATE SET r.time = timestamp(), r.found = "FALSE" '
###					' ON MATCH SET r.found = "TRUE" '
###				' RETURN var.name, var.found ',
###					username = self.username,
###					varieties = varieties['props'])
###		import pdb; pdb.set_trace()
###		print ("Creating varieties:")
###		for record in variety_create:
###			print ("Created" if record[1] else "Found"), record[0]


#		##then build in the relationships for hybrids to their parents
#		for variety in varieties['hybrid']:
#			parents = variety.split(' x ')
#			if len(parents) > 1:
#				maternal = parents[0]
#				paternal = parents[1]
#				link_hybrid =  tx.run( ' MATCH (var:Variety {name:$variety}) '
#					' MERGE (mat:Variety {name:$maternal}) '
#						' ON CREATE SET mat.found = "FALSE" '
#						' ON MATCH SET mat.found = "TRUE" '
#					' MERGE (pat:Variety {name:$paternal}) '
#						' ON CREATE SET pat.found = "FALSE" '
#						' ON MATCH SET pat.found = "TRUE" '
#					' MERGE (var)-[m:MATERNAL_DONOR]->(mat) '
#						' ON CREATE SET m.found = "FALSE" '
#						' ON MATCH SET m.found = "TRUE" '
#					' MERGE (var)-[p:PATERNAL_DONOR]->(pat) '
#						' ON CREATE SET p.found = "FALSE" '
#						' ON MATCH SET p.found = "TRUE" '
#					' RETURN var.name, mat.name, mat.found, pat.name, pat.found, m.found, p.found ',
#						variety = variety,
#						maternal = maternal,
#						paternal = paternal)
#				for record in link_hybrid:
#					if record['mat.found'] == "TRUE":
#						print ('Maternal donor variety is already registered: ' + record['mat.name'] )
#					elif record['mat.found'] == "FALSE":
#						print ('Maternal donor variety created: ' + record['mat.name'] )
#					if record['pat.found'] == "TRUE":
#						print ('Paternal donor variety is already registered: ' + record['pat.name'] )
#					elif record['pat.found'] == "FALSE":
#						print ('Paternal donor variety created: ' + record['pat.name'] )
#					if record['m.found'] == "TRUE" :
#						print('Maternal donor relationship already established between ' + record['var.name'] + " and " + record['mat.name'] )
#					if record['m.found'] == "FALSE" :
#						print('Maternal donor relationship created between ' + record['var.name'] + " and " + record['mat.name'] )
#					if record['p.found'] == "TRUE" :
#						print('Paternal donor relationship already established between ' + record['var.name'] + " and " + record['pat.name'] )
#					if record['p.found'] == "FALSE" :
#						print('paternal donor relationship created between ' + record['var.name'] + " and " + record['pat.name'] )
#		##tand same for grafts
#		for variety in varieties['grafted']:
#			grafted = variety.split(' ⁄ ')
#			if len(grafted) > 1:
#				scion = grafted[0]
#				rootstock = grafted[1]
#				link_graft =  tx.run( ' MATCH (var:Variety {name:$variety}) '
#					' MERGE (scion:Variety {name:$scion}) '
#						' ON CREATE SET scion.found = "FALSE" '
#						' ON MATCH SET scion.found = "TRUE" '
#					' MERGE (rootstock:Variety {name:$rootstock}) '
#						' ON CREATE SET rootstock.found = "FALSE" '
#						' ON MATCH SET rootstock.found = "TRUE" '
#					' MERGE (var)-[s:SCION]->(scion) '
#						' ON CREATE SET s.found = "FALSE" '
#						' ON MATCH SET s.found = "TRUE" '
#					' MERGE (var)-[r:ROOTSTOCK]->(rootstock) '
#						' ON CREATE SET r.found = "FALSE" '
#						' ON MATCH SET r.found = "TRUE" '
#					' RETURN var.name, scion.name, scion.found, rootstock.name, rootstock.found, s.found, r.found ',
#						variety = variety,
#						scion = scion,
#						rootstock = rootstock)
#				for record in link_graft:
#					if record['scion.found'] == "TRUE":
#						print ('Scion variety is already registered: ' + record['scion.name'] )
#					elif record['scion.found'] == "FALSE":
#						print ('Scion variety created: ' + record['scion.name'] )
#					if record['rootstock.found'] == "TRUE":
#						print ('Rootstock variety is already registered: ' + record['rootstock.name'] )
#					elif record['rootstock.found'] == "FALSE":
#						print ('Rootstock variety created: ' + record['rootstock.name'] )
#					if record['s.found'] == "TRUE" :
#						print('Scion relationship already established between ' + record['var.name'] + " and " + record['scion.name'] )
#					if record['s.found'] == "FALSE" :
#						print('Scion donor relationship created between ' + record['var.name'] + " and " + record['scion.name'] )
#					if record['r.found'] == "TRUE" :
#						print('Rootstock relationship already established between ' + record['var.name'] + " and " + record['rootstock.name'] )
#					if record['r.found'] == "FALSE" :
#						print('Rootstock relationship created between ' + record['var.name'] + " and " + record['rootstock.name'] )						





if not confirm('Are you sure you want to proceed? This is should probably only be run when setting up the database'):
	sys.exit()
else:
	if confirm('Do you want to a wipe existing data and rebuild the constraints?'):
		print('Performing a full reset of database')
		with driver.session() as session:
			session.write_transaction(delete_database)
			session.write_transaction(Create.constraints, CONSTRAINTS)
	else: print('Attempting to create the following while retaining existing data:\n'
		'  * user:start \n'
		'  * partners \n'
		'  * traits')
	with driver.session() as session:
		session.write_transaction(Create('start').user)
		session.write_transaction(Create('start').partners, PARTNERS)
		session.write_transaction(Create('start').traits, 'traits/plot_traits.csv', 'Plot')
		session.write_transaction(Create('start').traits, 'traits/block_traits.csv', 'Block')
		session.write_transaction(Create('start').traits, 'traits/tree_traits.csv', 'Tree')
		session.write_transaction(Create('start').traits, 'traits/branch_traits.csv', 'Branch')
		session.write_transaction(Create('start').traits, 'traits/leaf_traits.csv', 'Leaf')
		session.write_transaction(Create('start').traits, 'traits/sample_traits.csv', 'Sample')
		session.write_transaction(Create('start').varieties, VARIETIES)
	print ('Complete')
