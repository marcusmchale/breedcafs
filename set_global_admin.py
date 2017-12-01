#!/usr/bin/env python
# -*- coding: utf-8 -*-

#run this to add global_admin priviledges to an account

import sys
import os
#import shutil
from neo4j.v1 import GraphDatabase

#neo4j config
uri = "bolt://localhost:7687"
auth = (os.environ['NEO4J_USERNAME'], os.environ['NEO4J_PASSWORD'])
driver = GraphDatabase.driver(uri, auth=auth)

def add_global_admin(tx):
	username = raw_input('Enter the username that is to receive global_admin privileges: ')
	print ('Adding global admin privilege to account: ' + username)
	result = tx.run('MATCH (u:User {username: $username}) SET u.access = u.access + ["global_admin"]', username=username)
	return result

with driver.session() as session:
	session.write_transaction(add_global_admin)