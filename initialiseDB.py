#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import sys
import os
import csv
from flask import Flask
from neo4j.v1 import GraphDatabase
from instance import varieties, input_groups

app = Flask(__name__, instance_relative_config=True)
app.config.from_object('config')
app.config.from_pyfile('config.py')

# configure logging
logging.basicConfig(
	filename=app.config['BREEDCAFS_LOG'],
	level=logging.DEBUG,
	format='%(asctime)s %(levelname)-8s %(message)s',
	datefmt='%Y-%m-%d %H:%M:%S'
)

# Varieties are defined in a list of nested dicts structured around the "Trials"
# A bit complicated but this is how the partners initially provided me with details
# so it is easier to update it in this way during development as they provide updates
# The country, region, farm and varieties are all created where needed (IIRC)
# This is defined in the varieties module.
# e.g.
# trials = [
#	{
#		"uid": 1, # an integer for trial UID
#		"name": "A meaningful name for a trial",
#		"country": "Ireland",
#		"region": "Galway",
#		"farm": "NUIG",
#		"varieties": {
#			"inbred": [
#				"Marsellesa"
#			],
#			"hybrid": [
#				"Hybrid1"
#			],
#			"graft": [
#				"Variety 1 / Variety 2"
#			]
#		}
#	},
# variety codes are in current use in some regions, so these need to be defined
# in the varieties module which I place in the instance path as it isn't public yet
# It is a list of tuples,
# - first element is the code (string or integer)
# - second element is the maternal donor (mother plant)
# - third element is the paternal donor
# e.g:
# variety_codes = [
#	(1, "Mat 1", "Pat 1"),
#	(2, "Mat 1", "Pat 2")
# ]
# Additional information about the parents of each variety that I was provided,
# also in the varieties module which I place in the instance path as it isn't public yet
# - first element is the name of the variety
# - second element is the maternal donor (mother plant)
# - third element is the paternal donor
# e.g.:
# hybrid_parents = [
#	("variety 1 name", "mat 1", "pat 1"),
#	("variety 2 name", "mat 1", "pat 2")
# ]

# neo4j config
uri = app.config['BOLT_URI']
print 'Initialising DB:' + uri

auth = (os.environ['NEO4J_USERNAME'], os.environ['NEO4J_PASSWORD'])
driver = GraphDatabase.driver(uri, auth=auth)


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


def clear_schema(tx):
	tx.run('CALL apoc.schema.assert({}, {})')


def delete_items(tx):
	tx.run(
		' MATCH '
		'	(item:Item), '
		'	(counter:Counter) ' 
		' DETACH DELETE counter, item '
		' CREATE (:Counter {name: "field", count: 0})'
	)


def delete_data(tx):
	tx.run(
		' MATCH '
		'	(record:Record) ' 
		' DETACH DELETE record '
	)


def delete_inputs(tx):
	tx.run(
		' MATCH '
		'	(input:Input) ' 
		' DETACH DELETE input '
	)


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


def create_item_levels(tx, levels):
	tx.run(
		' UNWIND $levels as level'
		' CREATE (il: ItemLevel { '
		'	name_lower: toLower(trim(level)), '
		'	name: trim(level) '
		' }) ',
		levels=levels
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
			'		p.found = False '
			'	ON MATCH SET '
			'		p.found = True '
			' MERGE '
			'	(c:Country {'
			'		name_lower : toLower($based_in) '
			'	}) '
			'	ON CREATE SET '
			'		c.name = $based_in, '
			'		c.found = False '
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
			name = partner['name']
		)
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


def create_inputs(tx, inputs_file):
	with open(inputs_file, 'rb') as inputs_csv:
		reader = csv.DictReader(inputs_csv, delimiter=',', quotechar='"')
		for input_variable in reader:
			input_create = tx.run(
				' MERGE '
				'	(i:Input { '
				'		name_lower: toLower(trim($name)) '
				'	}) '
				'	ON CREATE SET '
				'		i.name = trim($name), '
				'		i.notes = trim($notes), '
				'		i.format = toLower(trim($format)), '
				'		i.minimum = CASE '
				'			WHEN size(trim($minimum)) = 0 '
				'				THEN Null '
				'			ELSE toInteger($minimum) '
				'			END, '
				'		i.maximum = CASE '
				'			WHEN size(trim($maximum)) = 0 '
				'				THEN Null '
				'			ELSE toInteger($maximum) '
				'			END, '
				'		i.details = $details, '
				'		i.categories = CASE '
				'			WHEN size(trim($categories)) = 0 '
				'				THEN Null '
				'			ELSE $categories '
				'			END, '
				'		i.category_list  = CASE '
				'			WHEN size(trim($categories)) = 0 '
				'				THEN Null '
				'			ELSE [i in split($categories, "/") | trim(i)] '
				'			END, '
				'		i.found = False '
				'	ON MATCH SET '
				'		i.found = True '
				' MERGE '
				' (record_type: RecordType {name_lower: toLower(trim($type))}) '
				'	ON CREATE SET record_type.name = trim($type) '
				' MERGE (i)-[:OF_TYPE]->(record_type) '
				' WITH i, record_type '
				' UNWIND [x in split($levels, "/") | trim(x)] as level '
				' MERGE '
				'	(item_level: ItemLevel { '
				'		name_lower: toLower(trim(level)) '
				'	}) '
				'	ON CREATE SET '
				'		item_level.name = trim(level) '
				' MERGE '
				'	(i)-[:AT_LEVEL]->(item_level) '
				' RETURN i.found ',
				type=input_variable['type'],
				levels=input_variable['levels'],
				name=input_variable['name'],
				format=input_variable['format'],
				minimum=input_variable['minimum'],
				maximum=input_variable['maximum'],
				details=input_variable['details'],
				categories=input_variable['categories'],
				notes=input_variable['notes']
			)
			for record in input_create:
				if record['i.found']:
					print ('Found input variable ' + input_variable['name'])
				elif not record['i.found']:
					print ('Created input variable: ' + input_variable['name'])
				else:
					print ('Error with merger of input variable: ' + input_variable['name'])


def create_input_groups(tx, groups):
	for group in groups:
		tx.run(
			' MERGE (ig: InputGroup {name_lower:toLower(trim($name))}) '
			'	ON CREATE SET ig.name = trim($name) '
			' WITH (ig) '
			'	MATCH (i:Input) '
			'	WHERE i.name_lower IN [x in $inputs | toLower(trim(x))] '
			'	MATCH (l:ItemLevel) '
			'	WHERE l.name_lower IN [x in $levels | toLower(trim(x))] '
			' MERGE '
			'	(i)-[:IN_GROUP]->(ig) '
			' MERGE '
			'	(ig)-[:AT_LEVEL]->(l) ',
			name=group['input_group'],
			levels=group['input_levels'],
			inputs=group['input_variables']
		)


def create_trials(tx, trials):
	# also creates the variety name input and list of varieties as its categories
	# as well as a number of known locations (from trial information)
	tx.run(
		' MATCH (input: Input {name_lower: "assign variety name"}) '
		' SET input.category_list = [] '
	)
	for trial in trials:
		# TODO major revision to handle the field trials that span multiple regions/farms.
		# for now allowing registering to Farm/Region without details - so that anchored for existing queries
		# then returned farm/region will just not have name property etc.
		# but need to change registering to separate trial from farm completely, and add farm to trial
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
				' MERGE '
				'	(trial: Trial { '
				'		uid: $uid, '
				'		name: $name, '
				'		name_lower: toLower($name) '
				'	}) '
				' -[: PERFORMED_IN]->(farm) ',
				uid=trial['uid'],
				name=trial['name'],
				country=trial['country'],
				region=trial['region'],
				farm=trial['farm']
			)
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
					' MERGE '
					'	(trial: Trial { '
					'		uid: $uid, '
					'		name: $name, '
					'		name_lower: toLower($name) '
					'	})-[: PERFORMED_IN]->(region) ',
					uid=trial['uid'],
					name=trial['name'],
					country=trial['country'],
					region=trial['region']
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
					' MERGE '
					'	(trial: Trial { '
					'		uid: $uid, '
					'		name: $name, '
					'		name_lower: toLower($name) '
					'	})-[:PERFORMED_IN]->(country) ',
					uid=trial['uid'],
					name=trial['name'],
					country=trial['country']
				)
		# add a relationship between the trial and the "variety name" input variable
		# make this relationship contain a category list
		# use this later with coalesce to obtain location dependent category lists
		tx.run(
			' MATCH '
			'	(trial: Trial {uid:$uid}), '
			'	(input: Input {name_lower: "assign variety name"}) '
			' MERGE '
			'	(trial)'
			'		<-[recorded_in: RECORDED_IN]-(input) '
			' SET recorded_in.category_list = [] ',
			uid=trial['uid']
		)
		# now merge the varieties into the recorded_in relationship as a category_list
		# but also collect all as categories on input category_list
		# will rely on a coalesce of assessed in and the input category list to return items
		# also create the varieties
		for variety_type in trial['varieties']:
			for variety in trial['varieties'][variety_type]:
				variety_create = tx.run(
					' MATCH '
					'	(trial: Trial { '
					'		uid: $trial '
					'	})<-[recorded_in:RECORDED_IN]-(input: Input { '
					'		name_lower: "assign variety name" '
					'	}) '
					' SET '
					'	recorded_in.category_list = CASE '
					'		WHEN $variety IN recorded_in.category_list '
					'		THEN recorded_in.category_list '
					'		ELSE recorded_in.category_list + $variety '
					'		END, '
					'	input.category_list = CASE '
					'		WHEN $variety IN input.category_list '
					'		THEN input.category_list '
					'		ELSE input.category_list + $variety '
					'		END '
					' MERGE '
					'	(variety: Variety { '
					'		name_lower: toLower($variety) '
					'	}) '
					'	ON CREATE SET '
					'		variety.name = $variety, '
					'		variety.type = $variety_type, '
					'		variety.found = False '
					'	ON MATCH SET '
					'		variety.found = True '
					' MERGE '
					'	(variety)-[:ASSESSED_IN]->(trial) '
					' RETURN '
					'	variety.name, variety.found ',
					variety=variety,
					trial=trial['uid'],
					variety_type=variety_type
				)
				for record in variety_create:
					print ("Created" if record[1] else "Found"), record[0]
	# build in any obvious relationships for hybrids to their parents
	tx.run(
		' MATCH '
		'	(variety: Variety {type: "hybrid"}) '
		' WITH '
		'	variety, '
		'	split(variety.name_lower, " x ") as parents '
		' WHERE size(parents) = 2 '
		' MATCH '
		'	(maternal: Variety {name_lower: parents[0]}), '
		'	(paternal: Variety {name_lower: parents[1]})'
		' MERGE '
		'	(variety)-[:MATERNAL_DONOR]->(maternal) '
		' MERGE '
		'	(variety)-[:PATERNAL_DONOR]->(paternal) '
	)
	# and now just a couple more that have non-obvious naming but for which I was provided details about parents
	for hybrid in varieties.hybrid_parents:
		tx.run(
			' MERGE '
			'	(variety: Variety { '
			'		type: "hybrid", '
			'		name_lower: toLower($hybrid) '
			'	}) '
			'	ON CREATE SET '
			'		variety.name = $hybrid '
			' MERGE '
			'	(maternal: Variety { '
			'		name_lower: toLower($maternal_donor) '
			'	}) '
			'	ON CREATE SET '
			'		maternal.name = $maternal_donor '
			' MERGE '
			'	(paternal: Variety { '
			'		name_lower: toLower($paternal_donor) '
			'	}) '
			'	ON CREATE SET '
			'		paternal.name = $paternal_donor  '
			' MERGE '
			'	(variety)-[:MATERNAL_DONOR]->(maternal) '
			' MERGE '
			'	(variety)-[:PATERNAL_DONOR]->(paternal) ',
			hybrid=hybrid[0],
			maternal_donor=hybrid[1],
			paternal_donor=hybrid[2]
		)
	# same for grafts (no additional lists here though)
	tx.run(
		' MATCH '
		'	(variety: Variety {type: "graft"}) '
		' WITH '
		'	variety, '
		'	split(variety.name_lower, " / ") as source_tissue '
		' WHERE size(source_tissue) = 2 '
		' MATCH '
		'	(scion: Variety {name_lower: source_tissue[0]}), '
		'	(rootstock: Variety {name_lower: source_tissue[1]})'
		' MERGE '
		'	(variety)-[:SCION]->(scion) '
		' MERGE '
		'	(variety)-[:ROOTSTOCK]->(rootstock) '
	)
	# now sort the list of variety names (this sorting will handle numbers better than a simple string sort does)
	tx.run(
		' MATCH (input: Input {name_lower: "assign variety name"}) '
		' WITH input, input.category_list as L '
		' UNWIND L as l '
		' WITH input, coalesce(toInteger(l), l) as L ORDER BY L '
		' WITH input, collect(toString(L)) as l '
		' SET input.category_list = l '
	)


def create_variety_codes(tx, variety_codes, input_name):
	tx.run(
		' MATCH (input: Input {name_lower: $input_name}) '
		' SET input.category_list = [] ',
		input_name=input_name
	)
	for item in variety_codes:
		if item[1].lower() == item[2].lower():  # if inbred
			result = tx.run(
				' MATCH (input: Input {name_lower: toLower($input_name)}) '
				' MERGE (var:Variety {name_lower: toLower($variety)}) '
				'	ON CREATE SET '
				'		var.name = $variety, '
				'		var.found = False '
				'	ON MATCH SET '
				'		var.found = True '
				' SET '
				'	input.category_list = input.category_list + $code'
				' RETURN var.found ',
				code=str(item[0]),
				variety=str(item[1]),
				input_name=input_name
			)
		else:  # hybrid
			result = tx.run(
				' MATCH (input: Input {name_lower: toLower($input_name)}) '
				' MERGE (mat:Variety {name_lower: toLower($maternal)}) '
				'	ON CREATE SET '
				'		mat.name = $maternal '
				' MERGE (pat:Variety {name_lower: toLower($paternal)}) '
				'	ON CREATE SET '
				'		pat.name = $paternal '
				' MERGE '
				'	(mat)<-[:MATERNAL_DONOR]-(var:Variety) '
				'		-[:PATERNAL_DONOR]->(pat) '
				' ON CREATE SET '
				'	var.name = ($maternal + " x " + $paternal), '
				'	var.found = False '
				' ON MATCH SET '
				'	var.found = True '
				' SET '
				'	input.category_list = input.category_list + $code '
				' RETURN var.found ',
				code=str(item[0]).lower(),
				maternal=str(item[1]).lower(),
				paternal=str(item[2]).lower(),
				input_name=input_name
			)
		print str(item[0]).lower(), str(item[1]).lower(), str(item[2]).lower()
		for record in result:
			print "Merging variety codes for " + str(input_name)
			if record[0]:
				print "Existing variety, code set"
			else:
				print "New variety created"
	# now sort that list of codes (this sorting will handle numbers better than a simple string sort does)
	tx.run(
		' MATCH (input: Input {name_lower: $input_name}) '
		' WITH input, input.category_list as L '
		' UNWIND L as l '
		' WITH input, coalesce(toInteger(l), l) as L ORDER BY L '
		' WITH input, collect(toString(L)) as l '
		' SET input.category_list = l ',
		input_name=input_name
	)


def create_start_email(tx):
	email = raw_input('Enter the email address to be first registrant: ')
	print ('Adding email to allowed users: ' + email)
	tx.run(
		' CREATE (emails: Emails {'
		'	allowed: $email '
		' }) ',
		email=[email]
	)


if not confirm('Are you sure you want to proceed? This is should probably only be run when setting up the database'):
	sys.exit()
else:
	if not confirm(
			'Would you like to remove everything? Select no to delete just the items, data or input variables.'
	):
		with driver.session() as session:
			if confirm('Would you like to delete all data?'):
				session.write_transaction(delete_data)
			if confirm('Would you like to delete all items?'):
				session.write_transaction(delete_items)
			if confirm('Would you like to delete inputs and varieties then recreate them from input_variables.csv and varieties.py?'):
				session.write_transaction(delete_inputs)
				session.write_transaction(create_inputs, './instance/input_variables.csv')
				session.write_transaction(create_trials, varieties.trials)
				session.write_transaction(
					create_variety_codes,
					varieties.el_frances_variety_codes,
					"Assign variety (El Frances code)"
				)
	elif confirm('Do you want to a delete everything rebuild the constraints and reset the indexes?'):
		print('Performing a full reset of database')
		with driver.session() as session:
			print('deleting all nodes and relationships')
			session.write_transaction(delete_database)
			print('clearing schema')
			session.write_transaction(clear_schema)
			print('creating constraints')
			session.write_transaction(create_constraints, app.config['CONSTRAINTS'])
			print('creating indexes')
			session.write_transaction(create_indexes, app.config['INDEXES'])
			session.write_transaction(create_partners, app.config['PARTNERS'])
			session.write_transaction(create_item_levels, app.config['ITEM_LEVELS'])
			session.write_transaction(create_inputs, './instance/inputs.csv')
			session.write_transaction(create_input_groups, input_groups.input_groups)
			session.write_transaction(create_trials, varieties.trials)
			session.write_transaction(
				create_variety_codes,
				varieties.el_frances_variety_codes,
				"Assign variety (El Frances code)"
			)
			session.write_transaction(create_start_email)
	else:
		print('Nothing done')
	print ('Finished')
