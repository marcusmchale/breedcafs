import os
from neo4j.v1 import GraphDatabase

DEBUG = False

#neo4j config
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=(os.environ['NEO4J_USERNAME'], os.environ['NEO4J_PASSWORD']))
#ALLOWED_EXTENSIONS = 'csv'
ALLOWED_EXTENSIONS = set(['csv'])
