import os
from app import GraphDatabase, ServiceUnavailable

class Driver_holder:
	driver = None

def get_driver():
	if Driver_holder.driver:
		return Driver_holder.driver
	uri = "bolt://localhost:7687"
	Driver_holder.driver = GraphDatabase.driver(uri, auth=(os.environ['NEO4J_USERNAME'], os.environ['NEO4J_PASSWORD']))
	return Driver_holder.driver



