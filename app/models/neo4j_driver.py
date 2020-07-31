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
