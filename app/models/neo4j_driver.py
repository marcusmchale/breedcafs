from app import (os,
	GraphDatabase, 
	ServiceUnavailable, 
	watch, 
	logging)

#this was pulled out of the initialisation process for the app so that the site can handle server downtime for backups
class Driver_holder:
	driver = None

def get_driver():
	if Driver_holder.driver:
		return Driver_holder.driver
	uri = "bolt://localhost:7687"
	Driver_holder.driver = GraphDatabase.driver(uri, auth=(os.environ['NEO4J_USERNAME'], os.environ['NEO4J_PASSWORD']))
	watch("neo4j.bolt", logging.INFO, open("logs/neo4j_driver.log", 'w+'))
	return Driver_holder.driver



