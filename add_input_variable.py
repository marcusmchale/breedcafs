#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import sys
import os
import csv
from flask import Flask
from neo4j.v1 import GraphDatabase

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
driver = GraphDatabase.driver(uri, auth=auth)

record_types = ['property', 'trait', 'condition', 'curve']
item_levels = ['field', 'block', 'tree', 'sample']
input_formats = [
	'numeric',
	'percent',
	'categorical',
	'boolean',
	'date',
	'text',
	'multicat',
	'location'
]


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


def create_input(tx):
	name = None
	while not name:
		name = input(
			'Enter the name for this input variable:\n'
		).strip()
	details = input('Enter a description for this input variable, include details about the units used:\n')
	record_type = None
	while not record_type:
		record_type = input('Enter the record_type:\n')
		if not record_type.lower().strip() in record_types:
			record_type = None
			print('Record type must be one of the following')
			print('Please try again')
		else:
			record_type = record_type.lower().strip()
	levels = None
	while not levels:
		levels = input(
			'Enter a list of levels that this input variable should be available at (separated by /)\n'
		)
		if not set([i.lower().strip() for i in levels.split('/')]).issubset(set(item_levels)):
			levels = None
			print(('Levels can only include the following:' + ', '.join(item_levels)))
			print ('Please try again')
		else:
			levels = [i.lower().strip() for i in levels.split('/')]
			levels = '/'.join(levels)
	input_format = None
	while not input_format:
		input_format = input('Enter an input format:\n')
		if not input_format.lower().strip() in input_formats:
			input_format = None
			print(('Input format must be one of the following: ' + ', '.join(input_formats)))
			print('Please try again')
		else:
			input_format = input_format.lower().strip()
	minimum = None
	maximum = None
	if input_format in ['numeric', 'percent']:
		minimum = input('Enter a numeric minimum if relevant else leave blank:\n')
		maximum = input('Enter a numeric maximum if relevant else leave blank:\n')
	categories = None
	if input_format in ['categorical', 'multicat']:
		categories = input('Enter a list of categories (separated by /) if categorical variable:\n')
	if input_format == 'boolean':
		categories = input('Enter two names for boolean values (e.g. Yes/No):\n')
	notes = input(
		'Optionally enter any notes about how this variable should be handled internally '
		'(not displayed in database tools)'
		'\n'
	)
	statement = (
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
		' WITH i '
		' MATCH '
		' 	(record_type: RecordType {name_lower: toLower(trim($type))}) '
		' MERGE (i)-[:OF_TYPE]->(record_type) '
		' WITH i, record_type '
		' UNWIND [x in split($levels, "/") | trim(x)] as level '
		' MATCH '
		'	(item_level: ItemLevel { '
		'		name_lower: toLower(trim(level)) '
		'	}) '
		' MERGE '
		'	(i)-[:AT_LEVEL]->(item_level) '
		' WITH i, record_type, collect(item_level.name) as item_levels '
		' RETURN { '
		'	type: record_type.name, '
		'	levels: item_levels, '
		'	name: i.name, '
		'	format: i.format, '
		'	minimum: i.minimum, '
		'	maximum: i.maximum, '
		'	details: i.details, '
		'	categories: i.category_list, '
		'	notes: i.notes, '
		'	found: i.found '
		' } '
	)
	input_create = tx.run(
		statement,
		type=record_type,
		levels=levels,
		name=name,
		format=input_format,
		minimum=minimum,
		maximum=maximum,
		details=details,
		categories=categories,
		notes=notes
	)
	record = input_create.peek()
	if record[0]['found']:
		print(('Found input variable ' + name))
	elif not record[0]['found']:
		print(('Created input variable: ' + name))
		# CSV header:
		# type, levels, name, format, minimum, maximum, details, categories, notes
		input_details = [
			record[0]['type'],
			' / '.join(record[0]['levels']),
			record[0]['name'],
			record[0]['format'],
			record[0]['minimum'],
			record[0]['maximum'],
			record[0]['details'],
			' / '.join(record[0]['categories']) if record[0]['categories'] else '',
			record[0]['notes']
		]
		with open('./instance/inputs.csv', 'a') as inputs_csv:
			csv_writer = csv.writer(inputs_csv, quoting=csv.QUOTE_ALL)
			csv_writer.writerow(input_details)
	else:
		print(('Error with merger of input variable: ' + name))


if not confirm('Do you want to add an input variable?'):
	print('Nothing done')
	sys.exit()
else:
	with driver.session() as session:
		session.write_transaction(create_input)
	print('Finished')

driver.close()
