import logging
import os
from app import app
from neo4j import GraphDatabase


# this was pulled out of the initialisation process for the app so that the site can handle server downtime for backups
class DriverHolder:
	driver = None

	def __init__(self):
		pass


def get_driver():
	if DriverHolder.driver:
		return DriverHolder.driver
	uri = app.config['BOLT_URI']
	DriverHolder.driver = GraphDatabase.driver(
		uri,
		auth=(
			os.environ['NEO4J_USERNAME'],
			os.environ['NEO4J_PASSWORD']
		),
		connection_timeout=5,
		connection_acquisition_timeout=5,
		max_transaction_retry_time=5
	)
	neo4j_log = logging.getLogger("neobolt")
	neo4j_log.setLevel(logging.DEBUG)
	fh = logging.FileHandler(app.config['NEO4J_LOG'])
	fh.setLevel(logging.DEBUG)
	neo4j_log.addHandler(fh)
	return DriverHolder.driver


def list_records(tx, query, **kwargs):
	try:
		return tx.run(query, kwargs).data()
		# must not return live result object or may break retry
	except Exception as e:
		logging.error(f"Error with neo4j_query: {e}")


def single_record(tx, query, **kwargs):
	try:
		return tx.run(query, kwargs).single().value()
	except Exception as e:
		logging.error(f"Error with neo4j_query: {e}")


def no_result(tx, query, **kwargs):
	try:
		tx.run(query, kwargs)
	except Exception as e:
		logging.error(f"Error with neo4j_query: {e}")
