from app import ServiceUnavailable, SecurityError, TransactionError

from app.models import ItemList

from app.cypher import Cypher

from flask import jsonify

from .neo4j_driver import get_driver, bolt_result

import datetime


class InputManager:
	def __init__(self, username):
		self.username = username
		self.neo4j_session = get_driver().session()

	def add_input_group(
			self,
			input_group_name,
			partner_to_copy=None,
			group_to_copy=None
	):
		tx = self.neo4j_session.begin_transaction()
		parameters = {
			'input_group_name': input_group_name,
			'partner_to_copy': partner_to_copy,
			'group_to_copy': group_to_copy,
			'username': self.username
		}
		# need to check if name is already registered by anyone with [AFFILIATED {data_shared:True}] to this partner
		check_existing_statement = (
			' MATCH '
			'	(partner: Partner)'
			'	<-[affiliated: AFFILIATED {'
			'		data_shared: True'
			'	}]-(: User {'
			'		username_lower: toLower(trim($username))'
			'	}) '
			' MATCH '
			'	(partner)<-[: AFFILIATED {'
			'		data_shared: True '
			'	}]-(user: User) '
			'	-[:SUBMITTED]->(: Submissions) '
			'	-[:SUBMITTED]->(: InputGroups) '
			'	-[:SUBMITTED]->(ig: InputGroup { '
			'		name_lower: toLower(trim($input_group_name)) '
			'	}) '
			' RETURN [ '
			'	True, '
			'	ig.id, '
			'	ig.name '
			' ] '
		)
		existing_for_partner = tx.run(
			check_existing_statement,
			parameters
		).single()
		if existing_for_partner:
			return existing_for_partner[0]
		# now create a new group
		# first make sure the user has the InputGroups submission node
		statement = (
			' MATCH '
			' 	(c:Counter {name: "input_group"}), '
			'	(: User { '
			'		username_lower: toLower(trim($username)) '
			'	})-[:SUBMITTED]->(sub: Submissions) '
			' MERGE '
			'	(sub)-[:SUBMITTED]->(igs: InputGroups) '
			' WITH c, igs '
		)
		if group_to_copy:
			if partner_to_copy:
				statement += (
					' MATCH '
					'	(source_partner: Partner { '
					'		name_lower:toLower(trim($partner_to_copy)) '
					'	}) '
					'	<-[: AFFILIATED {'
					'		data_shared: True'
					'	}]-(: User)-[: SUBMITTED]->(: Submissions) '
					'	-[:SUBMITTED]->(: InputGroups) '
					'	-[:SUBMITTED]->(source_ig: InputGroup { '
					'		id: $group_to_copy '
					'	}) '
					' WITH c, igs, source_ig '
				)
			else:
				statement += (
					' MATCH '
					' (source_ig: InputGroup { '
					'		id: $group_to_copy '
					' }) '
					' OPTIONAL MATCH (source_ig)<-[s:SUBMITTED]-() '
					# prioritise selection of the oldest if partner not specified 
					# (including original 'un-submitted' groups )
					' WITH c, igs, source_ig ORDER BY s.time DESC LIMIT 1 '
				)
		statement += (
				' SET c._LOCK_ = True '
				' SET c.count = c.count + 1 '
				' CREATE '
				'	(igs)-[: SUBMITTED { '
				'		time: datetime.transaction().epochMillis '
				'	}]->(ig: InputGroup {'
				'		id: c.count, '
				'		name_lower: toLower(trim($input_group_name)), '
				'		name: trim($input_group_name), '
				'		found: False '
				'	}) '
				' SET c._LOCK_ = False '
		)
		if group_to_copy:
			statement += (
					' WITH ig, source_ig '
					' OPTIONAL MATCH '
					'	(source_ig) '
					'	-[: AT_LEVEL]->(level: ItemLevel) '
					' WITH DISTINCT '
					'	ig, source_ig, level '
					' CREATE '
					'	(ig)-[:AT_LEVEL]->(level) '
					' WITH DISTINCT ig, source_ig '
					' OPTIONAL MATCH '
					'	(source_ig) '
					'	<-[in_group: IN_GROUP]-(input: Input) '
					' CREATE (input)-[new_in_group:IN_GROUP]->(ig) '
					' SET new_in_group = in_group '
			)
		statement += (
			' WITH DISTINCT ig '
			' RETURN [ '
			'	ig.found, '
			'	ig.id, '
			'	ig.name '
			' ] '
		)
		result = tx.run(
			statement,
			parameters
		).single()
		if result:
			tx.commit()
			return result[0]

	def update_group(self, input_group, inputs, levels):
		parameters = {
			'username': self.username,
			'input_group': input_group,
			'inputs': inputs,
			'levels': levels
		}
		statement = (
			' MATCH '
			'	(user:User {username_lower:toLower(trim($username))}) '
			'	-[:SUBMITTED]->(sub:Submissions) '
			' MATCH (user)-[: AFFILIATED { '
			'	data_shared: True '
			'	}]->(p:Partner) '
			' MATCH '
			'	(p)<-[:AFFILIATED {data_shared: True}]-(:User) '
			'	-[:SUBMITTED]->(: Submissions) '
			'	-[:SUBMITTED]->(: InputGroups) '
			'	-[:SUBMITTED]->(ig: InputGroup {'
			'		id: $input_group '
			'	}) '
			' MERGE '
			'	(sub)-[:SUBMITTED]->(igs:InputGroups) '
			' MERGE '
			'	(igs)-[mod:MODIFIED]->(ig) '
			'	ON CREATE SET mod.times = [datetime.transaction().epochMillis] '
			'	ON MATCH SET mod.times = mod.times + datetime.transaction().epochMillis '
			' WITH user, ig '
			' OPTIONAL MATCH '
			'	(ig)-[at_level:AT_LEVEL]->(:ItemLevel) '
			' DELETE at_level '
			' WITH user, ig '
			' MATCH (level:ItemLevel) '
			'	WHERE level.name_lower in $levels '
			' MERGE (ig)-[:AT_LEVEL]->(level) '
			' WITH user, ig '
			' OPTIONAL MATCH '
			'	(ig)<-[in_rel:IN_GROUP]-(:Input) '
			' DELETE in_rel '
			' WITH DISTINCT '
			'	user, ig, range(0, size($inputs)) as index '
			' UNWIND index as i '
			'	MATCH (input:Input {name_lower:toLower(trim($inputs[i]))}) '
			'	CREATE (input)-[rel:IN_GROUP {position: i}]->(ig) '
			' RETURN '
			'	input.name '
			' ORDER BY rel.position '
		)
		result = self.neo4j_session.run(
				statement,
				parameters
		)
		return [record[0] for record in result]

	def add_inputs_to_group(self, input_group, inputs):
		parameters = {
			'username': self.username,
			'input_group': input_group,
			'inputs': inputs
		}
		statement = (
			' MATCH '
			'	(user:User {username_lower:toLower(trim($username))}) '
			'	-[:SUBMITTED]->(sub:Submissions) '
			' MATCH '
			'	(ig: InputGroup {'
			'		name_lower: toLower(trim($input_group)) '
			'	}) '
			' MERGE '
			'	(sub)-[:SUBMITTED]->(igs:InputGroups) '
			' MERGE '
			'	(igs)-[:MODIFIED]->(ig) '
			' WITH '
			'	user, ig '
			' UNWIND $inputs as input_name '
			'	MATCH (input:Input {name_lower:toLower(trim(input_name))}) '
			'	MERGE (input)-[add:IN_GROUP]->(ig) '
			'		ON CREATE SET '
			'			add.user = user.username, '
			'			add.time = datetime.transaction().epochMillis, '
			'			add.found = False '
			'		ON MATCH SET '
			'			add.found = True '
			' RETURN [ '
			'	add.found, '
			'	input.name '
			' ] '
		)
		result = self.neo4j_session.run(
				statement,
				parameters
		)
		return [record[0] for record in result]
