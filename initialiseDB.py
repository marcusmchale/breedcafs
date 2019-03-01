#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import csv
from instance import config, varieties
from neo4j.v1 import GraphDatabase

# neo4j config
uri = "bolt://localhost:7687"
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


def delete_features(tx):
	tx.run(
		' MATCH '
		'	(feature:Feature) ' 
		' DETACH DELETE feature '
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


def create_features(tx, features_file):
	with open(features_file, 'rb') as features_csv:
		reader = csv.DictReader(features_csv, delimiter=',', quotechar='"')
		for feature in reader:
			feature_create = tx.run(
				' MERGE '
				'	(f:Feature { '
				'		name_lower: toLower(trim($name)) '
				'	}) '
				'	ON CREATE SET '
				'		f.name = trim($name), '
				'		f.format = toLower(trim($format)), '
				'		f.default_value = CASE '
				'			WHEN size(trim($default_value)) = 0 '
				'				THEN Null '
				'			ELSE $default_value '
				'			END, '
				'		f.minimum = CASE '
				'			WHEN size(trim($minimum)) = 0 '
				'				THEN Null '
				'			ELSE $minimum '
				'			END, '
				'		f.maximum = CASE '
				'			WHEN size(trim($maximum)) = 0 '
				'				THEN Null '
				'			ELSE $maximum '
				'			END, '
				'		f.details = $details, '
				'		f.categories = CASE '
				'			WHEN size(trim($categories)) = 0 '
				'				THEN Null '
				'			ELSE $categories '
				'			END, '
				'		f.category_list  = CASE '
				'			WHEN size(trim($categories)) = 0 '
				'				THEN Null '
				'			ELSE split($categories, "/") '
				'			END, '
				'		f.found = False '
				'	ON MATCH SET '
				'		f.found = True '
				' MERGE '
				' (record_type: RecordType {name_lower: toLower(trim($type))}) '
				' MERGE (f)-[:OF_TYPE]->(record_type) '
				'	ON CREATE SET record_type.name = trim($type) '
				' FOREACH (group in extract(x in split($groups, "/") | trim(x)) | '
				'	MERGE '
				'		(feature_group: FeatureGroup {'
				'			name_lower:toLower(trim(group))'
				'		}) '
				'		ON CREATE SET '
				'			feature_group.name = trim(group) '
				' 	MERGE '
				'		(f)-[:IN_GROUP]->(feature_group) '
				' ) '
				' FOREACH (level in extract(x in split($levels, "/") | trim(x)) | '
				'	MERGE '
				'		(item_level: ItemLevel { '
				'			name_lower: toLower(trim(level)) '
				'		}) '
				'		ON CREATE SET '
				'			item_level.name = trim(level) '
				'	MERGE '
				'		(f)-[:AT_LEVEL]->(item_level) '
				' ) '
				' RETURN f.found ',
				type=feature['type'],
				groups=feature['groups'],
				levels=feature['levels'],
				name=feature['name'],
				format=feature['format'],
				default_value=feature['defaultValue'],
				minimum=feature['minimum'],
				maximum=feature['maximum'],
				details=feature['details'],
				categories=feature['categories']
			)
			for record in feature_create:
				if record['f.found']:
					print ('Found feature ' + feature['name'])
				elif not record['f.found']:
					print ('Created feature: ' + feature['name'])
				else:
					print ('Error with merger of feature: ' + feature['name'])


def create_trials(tx, trials):
	# also creates the variety name feature and list of varieties as its categories
	# as well as a number of known locations (from trial information)
	tx.run(
		' MATCH (feature: Feature {name_lower: "variety name"}) '
		' SET feature.category_list = [] '
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
		# add a relationship between the trial and the "variety name" feature
		# make this relationship contain a category list
		# use this later with coalesce to obtain location dependent category lists
		tx.run(
			' MATCH '
			'	(trial: Trial {uid:$uid}), '
			'	(feature: Feature {name_lower: "variety name"}) '
			' MERGE '
			'	(trial)'
			'		<-[assessed_in: ASSESSED_IN]-(feature) '
			' SET assessed_in.category_list = [] ',
			uid=trial['uid']
		)
		# now merge these varieties into the assessed_in relationship as a category_list
		# but also collect all as categories on feature category_list
		# will rely on a coalesce of assessed in and the feature category list to return items
		# also create the varieties
		for variety_type in trial['varieties']:
			for variety in trial['varieties'][variety_type]:
				variety_create = tx.run(
					' MATCH '
					'	(trial: Trial { '
					'		uid: $trial '
					'	})<-[assessed_in:ASSESSED_IN]-(feature: Feature { '
					'		name_lower: "variety name" '
					'	}) '
					' SET '
					'	assessed_in.category_list = CASE '
					'		WHEN $variety IN assessed_in.category_list '
					'		THEN assessed_in.category_list '
					'		ELSE assessed_in.category_list + $variety '
					'		END, '
					'	feature.category_list = CASE '
					'		WHEN $variety IN feature.category_list '
					'		THEN feature.category_list '
					'		ELSE feature.category_list + $variety '
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
		' MATCH (feature: Feature {name_lower: "variety name"}) '
		' WITH feature, feature.category_list as L '
		' UNWIND L as l '
		' WITH feature, coalesce(toInteger(l), l) as L ORDER BY L '
		' WITH feature, collect(toString(L)) as l '
		' SET feature.category_list = l '
	)


def create_variety_codes(tx, variety_codes):
	tx.run(
		' MATCH (feature: Feature {name_lower: "variety code"}) '
		' SET feature.category_list = [] '
	)
	for item in variety_codes:
		if item[1].lower() == item[2].lower():  # if inbred
			result = tx.run(
				' MATCH (feature: Feature {name_lower: "variety code"}) '
				' MERGE (var:Variety {name_lower: toLower($variety)}) '
				'	ON CREATE SET '
				'		var.name = $variety, '
				'		var.found = False, '
				'		feature.category_list = feature.category_list + $code '
				'	ON MATCH SET '
				'		var.found = True '
				' SET var.code = $code '
				' SET var.code_lower = toLower($code) '
				' RETURN var.found ',
				code = str(item[0]),
				variety = str(item[1])
			)
		else:  # hybrid
			result = tx.run(
				' MATCH (feature: Feature {name_lower: "variety code"}) '
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
				'	var.code = $code, '
				'	var.code_lower = toLower($code), '
				'	feature.category_list = feature.category_list + $code '
				' RETURN var.found ',
				code = str(item[0]).lower(),
				maternal = str(item[1]).lower(),
				paternal = str(item[2]).lower()
			)
		print str(item[0]).lower(), str(item[1]).lower(), str(item[2]).lower()
		for record in result:
			print "Merging variety codes"
			if record[0]:
				print "Existing variety, code set"
			else:
				print "New variety created"
	# now sort that list of codes (this sorting will handle numbers better than a simple string sort does)
	tx.run(
		' MATCH (feature: Feature {name_lower: "variety code"}) '
		' WITH feature, feature.category_list as L '
		' UNWIND L as l '
		' WITH feature, coalesce(toInteger(l), l) as L ORDER BY L '
		' WITH feature, collect(toString(L)) as l '
		' SET feature.category_list = l '
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
			'Would you like to remove everything? Select no to delete just the items, data or features.'
	):
		with driver.session() as session:
			if confirm('Would you like to delete all data?'):
				session.write_transaction(delete_data)
			if confirm('Would you like to delete all items?'):
				session.write_transaction(delete_items)
			if confirm('Would you like to delete features and varieties then recreate them from features.csv and varieties.py?'):
				session.write_transaction(delete_features)
				session.write_transaction(create_features, './instance/features.csv')
				session.write_transaction(create_trials, varieties.trials)
				session.write_transaction(create_variety_codes, varieties.variety_codes)
	elif confirm('Do you want to a delete everything rebuild the constraints and reset the indexes?'):
		print('Performing a full reset of database')
		with driver.session() as session:
			print('deleting all nodes and relationships')
			session.write_transaction(delete_database)
			print('clearing schema')
			session.write_transaction(clear_schema)
			print('creating constraints')
			session.write_transaction(create_constraints, config.constraints)
			print('creating indexes')
			session.write_transaction(create_indexes, config.indexes)
			session.write_transaction(create_partners, config.partners)
			session.write_transaction(create_features, './instance/features.csv')
			session.write_transaction(create_trials, varieties.trials)
			session.write_transaction(create_variety_codes, varieties.variety_codes)
			session.write_transaction(create_start_email)
	else:
		print('Nothing done')
	print ('Finished')
