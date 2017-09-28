#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
#import shutil
from neo4j.v1 import GraphDatabase

#neo4j config
uri = "bolt://localhost:7687"
auth = (os.environ['NEO4J_USERNAME'], os.environ['NEO4J_PASSWORD'])
driver = GraphDatabase.driver(uri, auth=auth)

#inputs
class User:
	def __init__(self):
		pass
	def get_users(self):
		with driver.session() as session: 
			session.read_transaction(self._get_users)
	@staticmethod
	def _get_users(tx):
		for record in tx.run('MATCH (u:User) RETURN u'):
			print (record['u'])

User().get_users()