#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import csv
import getpass
#import shutil
from neo4j.v1 import GraphDatabase

#neo4j config
uri = "bolt://localhost:7687"
passwd = getpass.getpass()
auth = ('neo4j', passwd)
driver = GraphDatabase.driver(uri, auth=auth)



def add_database_user(tx):
	username = os.environ['NEO4J_USERNAME']
	password = os.environ['NEO4J_PASSWORD']
	print username
	tx.run('CALL dbms.security.createUser($username, $password, false)', username=username, password=password)

with driver.session() as session:
	session.write_transaction(add_database_user)