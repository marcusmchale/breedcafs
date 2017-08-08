#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
from datetime import datetime
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

#tuple{trait, input type {for Field-Book), description)
TRAITS= ({'trait':'height', 'format':'numeric', 'details':'Height'},
	{'trait':'diameter', 'format':'numeric', 'details':'Diameter (cm) of main stem at 20cm above the ground'},
	{'trait':'branches', 'format':'counter', 'details':'Branch count'},
	{'trait':'branches_fruiting', 'format':'counter', 'details':'Fruiting branch count'},
	{'trait':'nodes_fruiting', 'format':'counter', 'details':'Fruiting nodes per branch'},
	{'trait':'fruit_load', 'format':'numeric', 'details':'Fruit load (g) per branch/tree'},
	{'trait':'berry_yield', 'format':'numeric', 'details':'Berry Yield'},
	{'trait':'borer_incidence', 'format':'multicat', 'details':'Borer incidence'},
	{'trait':'rust_score', 'format':'rust rating', 'details':'Rust score'},
	{'trait':'location', 'format':'location', 'details':'Location (device positioning)'})

#Mock data to use in development - need to create the tool for users to generate this data (and down to the plant level)
#include genotype etc.
TREES = ({'country':'Nicaragua', 'region':'Highlands', 'farm':'yellow', 'plot':'1'},
	{'country':'Nicaragua', 'region':'Lowlands', 'farm':'blue', 'plot':'1'},
	{'country':'Costa Rica', 'region':'Highlands', 'farm':'red', 'plot':'11'},
	{'country':'Cameroon', 'region':'Lowlands', 'farm':'orange', 'plot':'111'},
	{'country':'France', 'region':'Highlands', 'farm':'yellow', 'plot':'11'})

CONSTRAINTS = ({'node':'User', 'property':'username', 'constraint':'IS UNIQUE'},
	{'node':'Partner', 'property':'name', 'constraint':'IS UNIQUE'},
	{'node':'Trait', 'property':'name', 'constraint':'IS UNIQUE'},
	{'node':'Plot', 'property':'uid', 'constraint':'IS UNIQUE'},
	{'node':'Tree', 'property':'id', 'constraint':'IS UNIQUE'})


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
	tx.run('MATCH (n)-[r]-() DELETE n , r')
	tx.run('MATCH (n) DELETE n')

class Create:
	def __init__ (self, username):
		self.username=username
	@classmethod
	def constraints(self, tx, constraints):
		for constraint in constraints:
			node=constraint['node']
			prop=constraint['property']
			constraint=constraint['constraint']
			tx.run('CREATE CONSTRAINT ON (n:' + node + ') ASSERT n.' + prop + ' ' + constraint)
	def user(self, tx):
		username=self.username
		tx.run(' CREATE (u:User {username:{username}})', username=username)
	def partners(self, tx, partners):
		for partner in partners: 
			tx.run('MATCH (u:User {username : {username}}) '
			' CREATE (u)-[s:SUBMITTED { submission_time : {submission_time} } ] '
			'->	(p:Partner {name:{name}, fullname:{fullname}}) '
			' MERGE (b:Country {name : {based_in}}) '
			' CREATE (p)-[:BASED_IN]->(b)'
			' WITH p'
			' MATCH (o:Country) WHERE o.name IN {operates_in} '
			' MERGE (p)-[:OPERATES_IN]-> (o)',
				username=self.username,
				submission_time=str(datetime.now()),
				based_in=partner['BASED_IN'],
				operates_in=partner['OPERATES_IN'],
				fullname=partner['fullname'],
				name=partner['name'])
	def traits(self, tx, traits):
		for trait in traits: 
			tx.run('MATCH (u:User {username:{username}}) '
			' CREATE (u)-[s:SUBMITTED { submission_time : {submission_time} } ] '
			'->	(t:Trait {name:{trait}, format:{format}, details:{details}})',
				username=self.username,
				submission_time=str(datetime.now()),
				trait=trait['trait'],
				format=trait['format'],
				details=trait['details'])
#https://stackoverflow.com/questions/32040409/reliable-autoincrementing-identifiers-for-all-nodes-relationships-in-neo4j
#use counter stored in Neo4j node to establish IDs
	def trees(self, tx, trees):
		for tree in trees:
			tx.run('MATCH (u:User {username:{username}}) '
				' MERGE (idP:UniqueId{name:"Plots"})'
				' MERGE (idT:UniqueId{name:"Trees"})'
				' ON CREATE SET idP.count=1'
				' ON CREATE SET idT.count=1'
				' ON MATCH SET idP.count=idP.count+1'
				' ON MATCH SET idT.count=idT.count+1'
				' MERGE (c:Country {name: {country}}) '
				' MERGE (r:Region {name: {region}}) - [:IS_IN] -> (c) '
				' MERGE (fa:Farm {name: {farm}}) - [:IS_IN] -> (r) '
				' MERGE (p:Plot {name: {plot}, uid:idP.count}) - [:IS_IN] -> (fa) '
				' MERGE (t:Tree {uid:idT.count}) - [:IS_IN] -> (p) '
				' MERGE (u) - [:SUBMITTED] -> (f) ',
				username=self.username,
				plot=tree['plot'],
				farm=tree['farm'],
				region=tree['region'],
				country=tree['country'])

if confirm('Do you want to reset the database from scratch?'):
	print('Full reset of database initiated')
	with driver.session() as session:
		session.write_transaction(delete_database)
else: print('Attempting to write over and retain existing data')

with driver.session() as session:
	session.write_transaction(Create.constraints, CONSTRAINTS)
	session.write_transaction(Create('start').user)
	session.write_transaction(Create('start').partners, PARTNERS)
	session.write_transaction(Create('start').traits, TRAITS)
	session.write_transaction(Create('start').trees, TREES)