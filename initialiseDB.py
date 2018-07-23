#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import csv
from instance import config
from neo4j.v1 import GraphDatabase

# neo4j config
uri = "bolt://localhost:7687"
auth = (os.environ['NEO4J_USERNAME'], os.environ['NEO4J_PASSWORD'])
driver = GraphDatabase.driver(uri, auth=auth)

# Node Key constraint requires Neo4j Enterprise Edition
# we work around this by using two labels on the trait
# one generic Trait label and one specific level+trait e.g. TreeTrait
# then create the constraint on that unique label


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


# erase database
def delete_database(tx):
	tx.run('MATCH (n) DETACH DELETE n')
	tx.run('CALL apoc.schema.assert ({}, {})')


def create_indexes(tx, indexes):
	for item in indexes:
		tx.run(
			'CREATE INDEX ON :'
			+ item['label']
			+ '('
			+ item['property']
			+ ')'
		)


def create_constraints(tx, constraints_list):
	for constraint in constraints_list:
		tx.run(
			'CREATE CONSTRAINT ON (n:'
			+ constraint['node']
			+ ') ASSERT (n.'
			+ constraint['property']
			+ ') '
			+ constraint['constraint']
		)


def create_partners(tx, partner_list):
	for partner in partner_list:
		partner_create = tx.run(
			' MERGE '
			'	(p:Partner { '
			'		name_lower: toLower($name) '
			'	}) '
			'	ON CREATE SET '
			'		p.name = $name, '
			'		p.fullname = $fullname, '
			'		p.found = False, '
			'	ON MATCH SET '
			'		p.found = True '
			' MERGE '
			'	(c:Country {'
			'		name_lower : toLower($based_in) '
			'	}) '
			'	ON CREATE SET '
			'		c.name = $based_in, '
			'		c.found = False, '
			'	ON MATCH SET '
			'		c.found = True '
			' MERGE '
			'	(p)-[r:BASED_IN]->(c) '
			' 	ON CREATE SET '
			'		r.found = False, '
			'		r.time = timestamp() '
			'	ON MATCH SET '
			'		r.found = True '
			' RETURN p.found, c.found, r.found ',
			based_in = partner['BASED_IN'],
			fullname = partner['fullname'],
			name = partner['name'])
		for record in partner_create:
			if record['p.found'] and record['r.found'] and record['c.found']:
				print (
					'Found: '
					+ partner['name']
					+ ' BASED_IN '
					+ partner['BASED_IN']
				)
			elif record['p.found'] and not record['r.found'] and record['c.found']:
				print (
					'Created: '
					+ partner['name']
					+ ' BASED_IN '
					+ partner['BASED_IN']
					+ '(relationship only)'
				)
			elif record['p.found'] and not record['r.found'] and not record['c.found']:
				print (
					'Created: '
					+ partner['name']
					+ ' BASED_IN '
					+ partner['BASED_IN']
					+ '(country and relationship only)'
				)
			elif not record['p.found'] and not record['r.found'] and record['c.found']:
				print (
					'Created: '
					+ partner['name']
					+ ' BASED_IN '
					+ partner['BASED_IN']
					+ '(partner and relationship only)'
				)
			elif not record['p.found'] and not record['r.found'] and not record['c.found']:
				print (
					'Created: '
					+ partner['name']
					+ ' BASED_IN '
					+ partner['BASED_IN']
				)
			else:
				print (
					'Error with merger of partner '
					+ partner['name']
					+ ' and/or BASED_IN relationship'
				)


def create_traits(tx, traits_file, level):
	with open(traits_file, 'rb') as traits_csv:
		reader = csv.DictReader(traits_csv, delimiter = ',', quotechar = '"')
		for trait in reader:
			trait_create = tx.run(
				' MERGE '
				# creating with both generic "trait" label, but also the specific level + trait attribute 
				'	(t:Trait:' + level + 'Trait { '
				'			name_lower: toLower(trim($trait))'
				'	}) '
				'	ON CREATE SET '
				'		t.level = toLower(trim($level)), '
				'		t.group = toLower(trim($group)), '
				'		t.name = trim($trait), '
				'		t.format = toLower(trim($format)), '
				'		t.default_value = CASE '
				'			WHEN size(trim($default_value)) = 0 '
				'				THEN Null '
				'			ELSE $default_value '
				'			END, '
				'		t.minimum = CASE '
				'			WHEN size(trim($minimum)) = 0 '
				'				THEN Null '
				'			ELSE $minimum '
				'			END, '
				'		t.maximum = CASE '
				'			WHEN size(trim($maximum)) = 0 '
				'				THEN Null '
				'			ELSE $maximum '
				'			END, '
				'		t.details = $details, '
				'		t.categories = CASE '
				'			WHEN size(trim($categories)) = 0 '
				'				THEN Null '
				'			ELSE $categories '
				'			END, '
				'		t.category_list  = CASE '
				'			WHEN size(trim($categories)) = 0 '
				'				THEN Null '
				'			ELSE split($categories, "/") '
				'			END, '
				'		t.found = False, '
				'		s2.time = timestamp() '
				'	ON MATCH SET '
				'		t.found = True '
				' RETURN t.found ',
				group = trait['group'],
				level = level,
				trait = trait['name'],
				format = trait['format'],
				default_value = trait['defaultValue'],
				minimum = trait['minimum'],
				maximum = trait['maximum'],
				details = trait['details'],
				categories = trait['categories']
			)
			for record in trait_create:
				if record['t.found']:
					print ('Found: ' + level + 'Trait ' + trait['name'])
				elif not record['t.found']:
					print ('Created: ' + level + 'Trait ' + trait['name'])
				else:
					print ('Error with merger of ' + level + 'Trait ' + trait['name'])


def add_global_admin(tx):
	username = raw_input('Enter the username that is to receive global_admin privileges: ')
	print ('Adding global admin privilege to account: ' + username)
	result = tx.run(
		'MATCH (u:User {username: $username}) SET u.access = u.access + ["global_admin"]',
		username = username
	)
	return result


def create_trials(self, tx, trials):
	# register the established trials
	for trial in trials:
		# TODO major revision to handle the field trials that span multiple regions/farms.
		# TODO revise this to give feedback and prevent overwrite (merge instead of create on trial)
		# change dependency of trial on farm-> region-> country
		# allow trial to span multiple farms
		if trial['farm']:
			tx.run(
				' MERGE '
				'	(country: Country { '
				'		name: $country, '
				'		name_lower: toLower($country) '
				'	}) '
				' ON CREATE SET country.found = False '
				' ON MATCH SET country.found = True '
				' MERGE '
				'	(region: Region { '
				'		name: $region,'
				'		name_lower: toLower($region) '
				'	})-[:IS_IN]->(country) '
				' ON CREATE SET region.found = False '
				' ON MATCH SET region.found = True '
				' MERGE '
				'	(farm: Farm { '
				'		name: $farm,'
				'		name_lower: toLower($farm) '
				'	})-[:IS_IN]->(region) '
				' ON CREATE SET farm.found = False '
				' ON MATCH SET farm.found = True '
				' CREATE ',
				'	(trial: Trial { '
				'		uid: $uid'
				'		name: $name'
				'		name_lower: toLower($name) '
				'	})-[:IS_IN]->(farm) ',
				uid = trial['uid'],
				name = trial['name'],
				country = trial['country'],
				region = trial['region'],
				farm = trial['farm']
			)
		#
		else:
			if trial['region']:
				tx.run(
					' MERGE '
					'	(country: Country { '
					'		name: $country, '
					'		name_lower: toLower($country) '
					'	}) '
					' ON CREATE SET country.found = False '
					' ON MATCH SET country.found = True '
					' MERGE '
					'	(region: Region { '
					'		name: $region,'
					'		name_lower: toLower($region) '
					'	})-[:IS_IN]->(country) '
					' ON CREATE SET region.found = False '
					' ON MATCH SET region.found = True '
					' CREATE ',
					'	(trial: Trial { '
					'		uid: $uid'
					'		name: $name'
					'		name_lower: toLower($name) '
					'	})-[:IS_IN]->(farm: Farm) '
					'	-[:IS_IN]->(region) '
					'	-[:IS_IN]->(country) ',
					uid = trial['uid'],
					name = trial['name'],
					country = trial['country'],
					region = trial['region']
				)
			else:
				tx.run(
					' MERGE '
					'	(country: Country { '
					'		name: $country, '
					'		name_lower: toLower($country) '
					'	}) '
					' ON CREATE SET country.found = False '
					' ON MATCH SET country.found = True '
					' CREATE ',
					'	(trial: Trial { '
					'		uid: $uid '
					'		name: $name '
					'		name_lower: toLower($name) '
					'	})-[:IS_IN]->(farm: Farm) '
					'	-[:IS_IN]->(region: Region) '
					'	-[:IS_IN]->(country) ',
					uid = trial['uid'],
					name = trial['name'],
					country = trial['country']
				)
		# merge the trial_varieties_trait for this trial
		tx.run(
			' MATCH '
			'	(trial: Trial {uid:$uid}), '
			'	(trait: TreeTrait {name_lower: "variety name"}) '
			' MERGE '
			'	(trial) '
			'	<-[assessed_in: ASSESSED_IN]-(trait: Trait: TreeTrait { '
			'		name_lower: "variety name" '
			'	}) '
			'	ON CREATE SET '
			'		assessed_in.category_list = [], '
			'		trait.name = "Variety name" ',
			'		trait.level = "tree", '
			'		trait.details = ("Variety name (from  for trial " + trial.uid) '
			# this is frustrating but Field Book can't handle long lists of categories (more than 12)
			# so we have to handle these traits as text
			'		trait.format = "text" '
			'		trait.group = "variety" ',
			uid = trial['uid']
		)
		for variety_type in trial['varieties']:
			for variety in variety_type:
				variety_create = tx.run(
					' MATCH '
					'	(trial: Trial {'
					'		uid: $trial'
					'	})<-[:ASSESSED_IN]-(trait: TreeTrait {'
					'		name_lower: "variety name"'
					'	})'
					' MERGE '
					'	(variety: Variety { '
					'		name_lower: toLower($variety) '
					'	}) '
					'	ON CREATE SET '
					'		name = $variety, '
					'		type = $variety_type '
					'		found = False '
					'	ON MATCH SET '
					'		found = True '
					' RETURN '
					'	variety.name, variety.found ',
					variety = variety,
					trial = trial['uid'],
					variety_type = variety_type
				)
				for record in variety_create:
					print ("Created" if record[1] else "Found"), record[0]
	# build in any obvious relationships for hybrids to their parents
	tx.run(
		' MATCH '
		'	(variety: Variety {type: "hybrid") '
		' WITH '
		'	variety, '
		'	split(variety.name_lower, " x ") as parents '
		' WHERE size(parents) = 2 '
		' MATCH '
		'	(maternal: Variety {name_lower: parents[0]}), '
		'	(paternal: Variety {name_lower: parents[1]})'
		' MERGE '
		'	(variety)-[:MATERNAL_DONOR]->(maternal), '
		' MERGE '
		'	(variety)-[:PATERNAL_DONOR]->(paternal) '
	)
	# and now just a couple more that have non-obvious naming but for which I was provided details about parents
	for hybrid in config.hybrid_parents:
		tx.run(
			' MERGE '
			'	(variety: Variety { '
			'		type: "hybrid", '
			'		name_lower: toLower($hybrid) '
			'	}), '
			'	ON CREATE SET '
			'		variety.name = $hybrid '
			' MERGE '
			'	(maternal: Variety { '
			'		name_lower: toLower($maternal_donor) '
			'	}) '
			'	ON CREATE SET '
			'		maternal.name = $maternal_donor'
			' MERGE '
			'	(paternal: Variety { '
			'		name_lower: toLower($paternal_donor) '
			'	})'
			'	ON CREATE SET '
			'		paternal.name = $paternal_donor'
			' MERGE '
			'	(variety)-[:MATERNAL_DONOR]->(maternal), '
			' MERGE '
			'	(variety)-[:PATERNAL_DONOR]->(paternal) ',
			hybrid = hybrid[0],
			maternal_donor = hybrid[1],
			paternal_donor = hybrid[2]
		)
	# same for grafts (no additional lists here though)
	tx.run(
		' MATCH '
		'	(variety: Variety {type: "graft") '
		' WITH '
		'	variety, '
		'	split(variety.name_lower, " / ") as source_tissue '
		' WHERE size(source_tissue) = 2 '
		' MATCH '
		'	(scion: Variety {name_lower: source_tissue[0]}), '
		'	(rootstock: Variety {name_lower: source_tissue[1]})'
		' MERGE '
		'	(variety)-[:SCION]->(scion), '
		' MERGE '
		'	(variety)-[:ROOTSTOCK]->(rootstock) '
	)


def variety_code (self, tx, variety_code):
	for item in variety_code:
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
	code_list = []
	for i in el_frances_code:
		if str(i[0]).lower() not in code_list:
			code_list.append(str(i[0]).lower())
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
			session.write_transaction(create_constraints, config.constraints)
			session.write_transaction(create_indexes, config.indexes)
	else: print('Attempting to create the following while retaining existing data:\n'
		'  * user:start \n'
		'  * partners \n'
		'  * traits')
	with driver.session() as session:
		session.write_transaction(create_partners, config.partners)
		session.write_transaction(Create('start').traits, 'traits/trial_traits.csv', 'Trial')
		session.write_transaction(Create('start').traits, 'traits/block_traits.csv', 'Block')
		session.write_transaction(Create('start').traits, 'traits/tree_traits.csv', 'Tree')
		session.write_transaction(Create('start').traits, 'traits/branch_traits.csv', 'Branch')
		session.write_transaction(Create('start').traits, 'traits/leaf_traits.csv', 'Leaf')
		session.write_transaction(Create('start').traits, 'traits/sample_traits.csv', 'Sample')
		session.write_transaction(Create('start').el_frances_code, config.el_frances_code)
		session.write_transaction(create_trials, config.trials)
		session.write_transaction(add_global_admin)
	print ('Complete')
