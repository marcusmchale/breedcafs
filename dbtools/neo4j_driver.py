import os
import time

from neo4j import GraphDatabase
from neo4j.exceptions import TransactionError

from dbtools import app, logging


class Neo4jDriver:

	def __init__(self):
		self.driver = GraphDatabase.driver(
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

	@staticmethod
	def get_transaction(session):
		for i in range(25):  # i.e. retry for 300 seconds (5 minutes) retrying with a 1 second delay each time
			try:
				return session.begin_transaction()
			except TransactionError:
				logging.warning('Sessions do not support concurrent transactions - retrying after %s seconds' % i)
				time.sleep(i)
		logging.error('Failed to obtain a transaction in the provided session')
		raise TransactionError

	def close(self):
		self.driver.close()

	def read(self, func, *args, **kwargs):
		with self.driver.session() as session:
			session.read_transaction(func, *args, **kwargs)

	def write(self, func, *args, **kwargs):
		with self.driver.session() as session:
			session.write_transaction(func, *args, **kwargs)


