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

# neo4j config
uri = app.config['BOLT_URI']
print('Initialising DB:' + uri)

auth = (os.environ['NEO4J_USERNAME'], os.environ['NEO4J_PASSWORD'])
driver = GraphDatabase.neo4j_driver(uri, auth=auth)


def confirm(question):
	valid = {"yes": True, "y": True, "no": False, "n": False}
	prompt = " [y/n] "
	while True:
		sys.stdout.write(question + prompt)
		choice = input().lower()
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
		' CALL apoc.periodic.iterate( '
		'	" '
		'		MATCH '
		'			(item:Item), '
		'			(counter:Counter) '
		'		RETURN item, counter '
		'	","'
		'		DETACH DELETE counter, item'
		'	", '
		'	{batchSize:1000} '
		' ) '
	)
	tx.run(' CREATE (:Counter {name: "field", count: 0})')


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
	tx.run(
		' MATCH (input_group:InputGroup) '
		' DETACH DELETE input_group '
	)


def delete_varieties(tx):
	tx.run(
		' MATCH '
		'	(variety:Variety) ' 
		' DETACH DELETE variety '
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
				print((
					'Found: '
					+ partner['name']
					+ ' BASED_IN '
					+ partner['BASED_IN']
				))
			elif record['p.found'] and not record['r.found'] and record['c.found']:
				print((
					'Created: '
					+ partner['name']
					+ ' BASED_IN '
					+ partner['BASED_IN']
					+ '(relationship only)'
				))
			elif record['p.found'] and not record['r.found'] and not record['c.found']:
				print((
					'Created: '
					+ partner['name']
					+ ' BASED_IN '
					+ partner['BASED_IN']
					+ '(country and relationship only)'
				))
			elif not record['p.found'] and not record['r.found'] and record['c.found']:
				print((
					'Created: '
					+ partner['name']
					+ ' BASED_IN '
					+ partner['BASED_IN']
					+ '(partner and relationship only)'
				))
			elif not record['p.found'] and not record['r.found'] and not record['c.found']:
				print((
					'Created: '
					+ partner['name']
					+ ' BASED_IN '
					+ partner['BASED_IN']
				))
			else:
				print((
					'Error with merger of partner '
					+ partner['name']
					+ ' and/or BASED_IN relationship'
				))


def create_inputs(tx, inputs_file):
	with open(inputs_file, 'r') as inputs_csv:
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
				' WITH distinct i '
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
					print(('Found input variable ' + input_variable['name']))
				elif not record['i.found']:
					print(('Created input variable: ' + input_variable['name']))
				else:
					print(('Error with merger of input variable: ' + input_variable['name']))


def create_input_groups(tx, groups):
	tx.run(
		' MERGE (c:Counter { '
		'	name: "input_group" '
		' }) '
		' ON CREATE SET c.count = 0 '
	)
	for group in groups:
		connect_inputs_to_group = tx.run(
			' MATCH (c:Counter {name: "input_group"}) '
			' SET c._LOCK_ = True '
			' WITH c '
			' MERGE (c)-[:FOR]->(ig: InputGroup {name_lower:toLower(trim($name))}) '
			'	ON CREATE SET '
			'		c.count = c.count + 1, '
			'		ig.name = trim($name), '
			'		ig.id = c.count '
			' SET c._LOCK_ = False '
			' WITH ig '
			' MATCH (l:ItemLevel) '
			' WHERE l.name_lower IN [x in $levels | toLower(trim(x))] '
			' MERGE '
			'	(ig)-[:AT_LEVEL]->(l) '
			' WITH DISTINCT ig, range(0, size($inputs)) as count '
			' UNWIND count AS index '
			'	MATCH (input:Input {name_lower: toLower(trim($inputs[index]))}) '
			'	MERGE (input)-[:IN_GROUP {position: index}]->(ig) '
			' RETURN collect(input.name) as input_names ',
			name=group['input_group'],
			levels=group['input_levels'],
			inputs=group['input_variables']
		)
		for record in connect_inputs_to_group:
			if not record['input_names']:
				print('Error adding variables to group')
			elif not len(record['input_names']) == len(group['input_variables']):
				print((
					'Some variables not matched by name: ' +
					','.join([i for i in group['input_variables'] if i not in record['input_names']])
				))


def create_varieties(tx, varieties_file):
	with open(varieties_file, 'r') as varieties_csv:
		reader = csv.DictReader(varieties_csv, delimiter=',', quotechar='"')
		for variety in reader:
			tx.run(
				' MERGE '
				'	(v: Variety {name_lower: toLower(trim($name))}) '
				' SET '
				'	v.name = $name ',
				name=variety['name']
			)
			#'	v.type = $type, '
			#'	v.lineage_description = $lineage_description ',
			#type=variety['type'],
			#lineage_description=variety['lineage_description']
			#f variety['lineage_group']:
			#	tx.run(
			#		' MATCH '
			#		'	(v: Variety {name: $name}) '
			#		' MERGE '
			#		'	(lg: LineageGroup {'
			#		'		type: "basic" '
			#		'	}) '
			#		' MERGE '
			#		'	(v)-[:IN_GROUP]->(lg) ',
			#		name=variety['name'],
			#		lineage_group=variety['lineage_group']
			#	)
			#f variety['type'] == 'hybrid':
			#	tx.run(
			#		' MATCH '
			#		'	(v: Variety {name: $name}) '
			#		' MERGE '
			#		'	(m: Variety {name_lower: toLower(trim($maternal))}) '
			#		'	ON MATCH SET m.found = True '
			#		' MERGE '
			#		'	(p: Variety {name_lower: toLower(trim($paternal))}) '
			#		'	ON MATCH SET p.found = True '
			#		' MERGE '
			#		'	(v)-[:MATERNAL_DONOR]->(m) '
			#		' MERGE '
			#		'	(v)-[:PATERNAL_DONOR]->(p) ',
			#		name=variety['name'],
			#		maternal=variety['maternal'],
			#		paternal=variety['paternal']
			#	)
			#lif variety['type'] == 'graft':
			#	tx.run(
			#		' MATCH '
			#		'	(v: Variety {name: $name}) '
			#		' MERGE '
			#		'	(s: Variety {name_lower: toLower(trim(scion))}) '
			#		'	ON MATCH SET s.found = True '
			#		' MERGE '
			#		'	(r: Variety {name_lower: toLower(trim($rootstock))}) '
			#		'	ON MATCH SET r.found = True '
			#		' MERGE '
			#		'	(v)-[:SCION_TISSUE]->(s) '
			#		' MERGE '
			#		'	(v)-[:ROOTSTOCK_TISSUE]->(r) '
			#		' RETURN m.found, p.found ',
			#		name=variety['name'],
			#		scion=variety['scion'],
			#		rootstock=variety['rootstock']
			#	)
	# now sort the list of variety names (this sorting will handle numbers better than a simple string sort does)
	tx.run(
		' MATCH '
		'	(i: Input {name_lower: "assign variety name"}), '
		'	(v: Variety) '
		' WITH i, v.name as variety '
		' ORDER BY variety '
		' WITH i, collect(variety) as varieties	'
		' SET i.category_list = varieties '
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
		print(str(item[0]).lower(), str(item[1]).lower(), str(item[2]).lower())
		for record in result:
			print("Merging variety codes for " + str(input_name))
			if record[0]:
				print("Existing variety, code set")
			else:
				print("New variety created")
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
	email = input('Enter the email address to be first registrant: ')
	print(('Adding email to allowed users: ' + email))
	tx.run(
		' CREATE (emails: Emails {'
		'	allowed: $email '
		' }) ',
		email=[email]
	)


def go():
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
				if confirm(
						'Would you like to delete inputs and varieties then recreate them from inputs.csv, '
						'input_groups.py and varieties.csv?'
				):
					session.write_transaction(delete_inputs)
					session.write_transaction(delete_varieties)
					session.write_transaction(create_inputs, './instance/inputs.csv')
					session.write_transaction(create_input_groups, input_groups.input_groups)
					session.write_transaction(create_varieties, './instance/varieties.csv')
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
				session.write_transaction(create_varieties, './instance/varieties.csv')
				session.write_transaction(
					create_variety_codes,
					varieties.el_frances_variety_codes,
					"Assign variety (El Frances code)"
				)
				session.write_transaction(create_start_email)
		else:
			print('Nothing done')
		print('Finished')

go()
driver.close()

