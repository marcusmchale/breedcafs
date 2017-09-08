#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import csv
#import shutil
from neo4j.v1 import GraphDatabase

#neo4j config
uri = "bolt://localhost:7687"
auth = (os.environ['NEO4J_USERNAME'], os.environ['NEO4J_PASSWORD'])
driver = GraphDatabase.driver(uri, auth=auth)

#inputs

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

CONSTRAINTS = ({'node':'User', 'property':'username', 'constraint':'IS UNIQUE'},
	{'node':'Partner', 'property':'name', 'constraint':'IS UNIQUE'},
	{'node':'Trait', 'property':'name', 'constraint':'IS UNIQUE'},
	{'node':'PlotID', 'property':'name', 'constraint':'IS UNIQUE'},
	{'node':'TreeID', 'property':'plotID', 'constraint':'IS UNIQUE'},
	{'node':'Plot', 'property':'uid', 'constraint':'IS UNIQUE'},
	{'node':'Tree', 'property':'uid', 'constraint':'IS UNIQUE'})

#functions
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
	@classmethod
	def constraints(self, tx, constraints):
		for constraint in constraints:
			node=constraint['node']
			prop=constraint['property']
			constraint=constraint['constraint']
			tx.run('CREATE CONSTRAINT ON (n:' + node + ') ASSERT (n.' + prop + ') ' + constraint)
	def user(self, tx):
		username=self.username
		user_create = tx.run(' MERGE (u:User {username:$username}) '
			'ON MATCH SET u.found="TRUE" '
			'ON CREATE SET u.found="FALSE" '
			'RETURN u.found', 
			username=username)
		for record in user_create:
			if record['u.found'] == 'TRUE' :
				print ('Found: User ' + username)
			if record['u.found'] == 'FALSE' :
				print ('Created: User ' + username)
	def partners(self, tx, partners):
		for partner in partners: 
			partner_create = tx.run('MATCH (u:User {username : $username}) '
				' MERGE (p:Partner {name:$name, fullname:$fullname}) '
				' ON MATCH SET p.found="TRUE" '
				' ON CREATE SET p.found="FALSE"'
				' MERGE (u)-[s:SUBMITTED]->(p) '
				' ON MATCH SET s.timeInt = timestamp() '
				' ON CREATE SET s.timeInt = timestamp() '
				' MERGE (c:Country {name : $based_in}) '
				' ON MATCH SET c.found ="TRUE"'
				' ON CREATE SET c.found ="FALSE"'
				' MERGE (u)-[s2:SUBMITTED]->(c)'
				' ON MATCH SET s2.timeInt = timestamp() '
				' ON CREATE SET s2.timeInt = timestamp() '
				' MERGE (p)-[r:BASED_IN]->(c)'
				' ON MATCH SET r.found="TRUE"'
				' ON CREATE SET r.found="FALSE"'
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
			operates_in = tx.run (' MATCH (p:Partner {name : $name}), '
				' (u:User {username: $username})'
				' UNWIND $operates_in AS x'
				' MERGE (c:Country {name:x}) '
				' ON MATCH SET c.found="TRUE" '
				' ON CREATE SET c.found="FALSE" '
				' MERGE (c)<-[s:SUBMITTED]-(u) '
				' ON MATCH SET s.timeInt = timestamp() '
				' ON CREATE SET s.timeInt = timestamp() '
				' MERGE (c)<-[r:OPERATES_IN]-(p) '
				' ON MATCH SET r.found="TRUE" '
				' ON CREATE SET r.found="FALSE" '
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
	def traits(self, tx):
		with open ('traits.csv', 'rb') as traits_csv:
			reader = csv.DictReader(traits_csv, delimiter=',', quotechar='"')
			for trait in reader: 
				trait_create = tx.run('MATCH (u:User {username:$username}) '
				' MERGE (t:Trait {group: $group, '
					' name: $trait, '
					' format: $format, '
					' defaultValue: $defaultValue, '
					' minimum: $minimum, '
					' maximum: $maximum,'
					' details: $details, '
					' categories: $categories}) '
				' ON MATCH SET t.found="TRUE" '
				' ON CREATE SET t.found="FALSE" '
				' MERGE (t)<-[s:SUBMITTED]-(u) '
				' ON MATCH SET s.timeInt = timestamp() '
				' ON CREATE SET s.timeInt = timestamp() '
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
						print ('Found: trait ' + trait['name'])
					elif record['t.found]'=='FALSE']:
						print ('Created: trait ' + trait['name'])
					else:
						print ('Error with merger of trait ' + trait['name'])
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
		session.write_transaction(Create('start').traits)
	print ('Complete')
