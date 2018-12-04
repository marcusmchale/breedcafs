from app import (
	app,
	os,
	GraphDatabase, 
	# ServiceUnavailable,
	# AuthError,
	watch, 
	logging
)


# this was pulled out of the initialisation process for the app so that the site can handle server downtime for backups
class DriverHolder:
	driver = None

	def __init__(self):
		pass


def get_driver():
	if DriverHolder.driver:
		return DriverHolder.driver
	uri = "bolt://localhost:7687"
	DriverHolder.driver = GraphDatabase.driver(
		uri,
		auth=(
			os.environ['NEO4J_USERNAME'],
			os.environ['NEO4J_PASSWORD']
		),
		connection_timeout=5,
		connection_acquisition_timeout=5,
		max_retry_time=5
	)
	watch("neo4j.bolt", logging.INFO, open(app.config['NEO4J_DRIVER_LOG'], 'w+'))
	return DriverHolder.driver


def neo4j_query(tx, query, parameters=None):
	try:
		result = tx.run(query, parameters)
		# must not return live result object or may break retry
		return [record for record in result]
	except Exception as e:
		logging.error("Error with neo4j_query:" + str(e))
