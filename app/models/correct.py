from app import app, os
import grp
from app.cypher import Cypher
from neo4j_driver import (
	get_driver,
	bolt_result
)
from flask import (
	url_for,
	render_template
)

from app.models import(
	AddFieldItems,
	ItemList,
	FeatureList,
	User,
	Download
)

from app.emails import send_email

import unicodecsv as csv

from datetime import datetime

from xlsxwriter import Workbook


# User class related (all uploads are tied to a user) yet more specifically regarding uploads
class Correct:
	def __init__(self, username, access):
		self.username = username,
		self.access = access

	def collect_records(
			self,
			parameters
	):
		if self.access == 'global_admin':
			statement = (
				' MATCH '
				'	(partner:Partner) '
				'	<-[:AFFILIATED {data_shared:True})-(user:User) '
			)
		elif self.access == 'partner_admin':
			statement = (
				' MATCH '
				'	(: User {'
				'		username_lower: toLower($username)'
				'	}) '
				'	-[: AFFILIATED {admin: True}]->(partner: Partner) '
				' MATCH '
				'	(partner) '
				'	<-[:AFFILIATED {data_shared:True}]-(user:User) '
			)
		else:
			statement = (
				' MATCH '
				'	(partner: Partner) '
				'	<-[:AFFILIATED {data_shared:True}]-(user: User {'
				'		username_lower: toLower($username)'
				'	}) '
			)
		statement += Download.user_record_query(parameters, data_format='db')
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				bolt_result,
				statement,
				parameters
			)
		return result

