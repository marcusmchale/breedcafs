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


def create_conditions(tx, conditions_file, level):
	with open(conditions_file, 'rb') as conditions_csv:
		reader = csv.DictReader(conditions_csv, delimiter=',', quotechar='"')
		for condition in reader:
			condition_create = tx.run(
				' MERGE '
				'	(c:Condition:' + level + 'Condition { '
				'		name_lower: toLower(trim($name)) '
				'	}) '
				'	ON CREATE SET '
				'		c.level = toLower(trim($level)), '
				'		c.group = toLower(trim($group)), '
				'		c.name = trim($name), '
				'		c.format = toLower(trim($format)), '
				'		c.default_value = CASE '
				'			WHEN size(trim($default_value)) = 0 '
				'				THEN Null '
				'			ELSE $default_value '
				'			END, '
				'		c.minimum = CASE '
				'			WHEN size(trim($minimum)) = 0 '
				'				THEN Null '
				'			ELSE $minimum '
				'			END, '
				'		c.maximum = CASE '
				'			WHEN size(trim($maximum)) = 0 '
				'				THEN Null '
				'			ELSE $maximum '
				'			END, '
				'		c.details = $details, '
				'		c.categories = CASE '
				'			WHEN size(trim($categories)) = 0 '
				'				THEN Null '
				'			ELSE $categories '
				'			END, '
				'		c.category_list  = CASE '
				'			WHEN size(trim($categories)) = 0 '
				'				THEN Null '
				'			ELSE split($categories, "/") '
				'			END, '
				'		c.found = False '
				'	ON MATCH SET '
				'		c.found = True '
				' RETURN c.found ',
				group=condition['group'],
				level=level,
				name=condition['name'],
				format=condition['format'],
				default_value=condition['defaultValue'],
				minimum=condition['minimum'],
				maximum=condition['maximum'],
				details=condition['details'],
				categories=condition['categories']
			)
			for record in condition_create:
				if record['c.found']:
					print ('Found: ' + level + 'Trait ' + condition['name'])
				elif not record['c.found']:
					print ('Created: ' + level + 'Trait ' + condition['name'])
				else:
					print ('Error with merger of ' + level + 'Trait ' + condition['name'])


with driver.session() as session:
	session.write_transaction(create_conditions, 'traits/field_conditions.csv', 'Field')