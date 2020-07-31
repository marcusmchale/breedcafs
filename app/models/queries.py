from app import app, os, logging, ServiceUnavailable, SecurityError
from .neo4j_driver import get_driver


class Query:

	def __init__(self, write=False):
		with get_driver().session() as session:
			if write:
				self.tx = session.write_transaction()
			else:
				self.tx = session.read_transaction()

	def run_query(self, query, parameters=None):
		try:
			result = self.tx.run(query, parameters)
			# must not return live result object or may break retry
			return result
		except (ServiceUnavailable, SecurityError) as e:
			logging.warning("Connection to neo4j failed:" + str(e))
			raise e
		except Exception as e:
			logging.exception(e)

	def get_bolt(self, query, parameters=None):
		return self.run_query(query, parameters)

	def get_list(self, query, parameters=None):
		return list(self.run_query(query, parameters))

	def get_boolean(self, query, parameters=None):
		if self.run_query(query, parameters).single():
			return True
		else:
			return False
