from app import (
	app,
	os,
	GraphDatabase,
	logging
)


class DriverHolder:
	driver = None

	def __init__(self):
		pass


def get_driver():
	if DriverHolder.driver:
		return DriverHolder.driver
	DriverHolder.driver = GraphDatabase.driver(
		app.config['BOLT_URI'],
		auth=(
			os.environ['NEO4J_USERNAME'],
			os.environ['NEO4J_PASSWORD']
		),
		connection_timeout=5,
		connection_acquisition_timeout=5,
		max_retry_time=5
	)
	fh = logging.FileHandler(app.config['NEO4J_DRIVER_LOG'])
	neo4j_log = logging.getLogger("neobolt")
	neo4j_log.addHandler(fh)
	return DriverHolder.driver


# the below is deprecated, should use the new Query class that applies the query in a transaction
def bolt_result(tx, query, parameters=None):
	try:
		result = tx.run(query, parameters)
		# must not return live result object or may break retry
		return result
	except Exception as e:
		logging.error("Error with neo4j_query:" + str(e))