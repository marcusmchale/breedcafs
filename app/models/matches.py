from neo4j_driver import (
	get_driver,
	neo4j_query
)


class MatchNode:
	def __init__(
			self
	):
		pass

	@staticmethod
	def tissue(tissue_type):
		parameters = {
			'tissue_type': tissue_type
		}
		query = (
			' MATCH (tissue:Tissue {name_lower: $tissue_type}) '
			' RETURN [ '
			'	tissue.name_lower, '
			'	tissue.name '
			' ] '
			' ORDER BY tissue.name_lower'
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				neo4j_query,
				query,
				parameters
			)
		return [(record[0][0], record[0][1]) for record in result]

	@staticmethod
	def storage(storage_type):
		parameters = {
			'storage_type': storage_type
		}
		query = (
			' MATCH (storage:Storage {name_lower: $storage_type}) '
			' RETURN [ '
			'	storage.name_lower, '
			'	storage.name '
			' ] '
			' ORDER BY storage.name_lower'
		)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				neo4j_query,
				query,
				parameters
			)
		return [(record[0][0], record[0][1]) for record in result]