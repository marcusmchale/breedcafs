#!/usr/bin/env python
# -*- coding: utf-8 -*-

# run this to add global_admin privileges to an account

import os
from neo4j.v1 import GraphDatabase
from instance import config

# neo4j config
uri = config.BOLT_URI
auth = (os.environ['NEO4J_USERNAME'], os.environ['NEO4J_PASSWORD'])
driver = GraphDatabase.driver(uri, auth=auth)


def add_global_admin(tx):
	username = raw_input('Enter the username that is to receive global_admin privileges: ')
	print ('Adding global admin privilege to account: ' + username)
	result = tx.run(
		' MATCH '
		'	(u:User {username_lower: toLower($username)}) '
		' SET '
		'	u.access = u.access + ["global_admin"]',
		username=username
	)
	return result


with driver.session() as session:
	session.write_transaction(add_global_admin)
