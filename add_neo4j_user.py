#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import getpass
from neo4j.v1 import GraphDatabase
from instance import config

# neo4j config
passwd = getpass.getpass()
auth = ('neo4j', passwd)
driver = GraphDatabase.driver(config.BOLT_URI, auth=auth)


def add_database_user(tx):
	username = os.environ['NEO4J_USERNAME']
	password = os.environ['NEO4J_PASSWORD']
	print username
	tx.run('CALL dbms.security.createUser($username, $password, false)', username=username, password=password)


with driver.session() as session:
	session.write_transaction(add_database_user)