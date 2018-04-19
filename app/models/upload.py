from app import app, celery, ServiceUnavailable
from app.cypher import Cypher
from app.emails import send_email
from flask import render_template
from user import User
from config import ALLOWED_EXTENSIONS
from neo4j_driver import get_driver


class Upload:
	def __init__(self, username, filename):
		self.username=username
		self.filename=filename
	def allowed_file(self):
		return '.' in self.filename and \
			self.filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


#needed to separate this for celery as the class is not easily serialised into JSON for async calling
@celery.task(bind=True)
def async_submit(
		self,
		username,
		filename,
		submission_type,
		level,
		traits = [],
		form_user = 'unknown',
		form_date = 'unknown',
		form_time = 'unknown',
		neo4j_time = 0
):
	try:
		with get_driver().session() as neo4j_session:
			result = neo4j_session.write_transaction(
				_submit,
				username,
				filename,
				submission_type,
				level,
				traits,
				form_user,
				form_date,
				form_time,
				neo4j_time
			)
			with app.app_context():
					#send result of merger in an email
					subject = "BreedCAFS upload summary"
					recipients = [User(username).find('')['email']]
					body = ("You uploaded a file to the BreedCAFS database." 
						+ str(result[0]) + " new values were submitted to the database and "
						+ str(result[1]) + " existing values were found.")
					html = render_template('emails/upload_report.html', new = result[0], old = result[1])
					send_email(subject, app.config['ADMINS'][0], recipients, body, html )
			return result
	except (ServiceUnavailable) as exc:
		raise self.retry(exc=exc)

def _submit(
		tx,
		username,
		filename,
		submission_type,
		level,
		traits,
		form_user,
		form_date,
		form_time,
		neo4j_time
):
	if submission_type == 'FB':
		if level == 'plot':
			query = Cypher.upload_FB_plot
		elif level == 'block':
			query = Cypher.upload_FB_block
		elif level == 'tree':
			query = Cypher.upload_FB_tree
		elif level == 'branch':
			query = Cypher.upload_FB_branch
		elif level == 'leaf':
			query = Cypher.upload_FB_leaf
		elif level == 'sample':
			query = Cypher.upload_FB_sample
		else:
			query = 'MATCH (d:Data) return d.found'
		result = tx.run(
			query,
			username=username,
			filename=("file:///" + username + '/' + filename),
			submission_type=submission_type
		)
	else: #submission_type == 'table':
			if level == 'sample':
				query = 'MATCH (d:Data) return d.found'
			elif level == 'tree':
				query = Cypher.upload_table_tree
			elif level == 'block':
				query = Cypher.upload_table_block
			elif level == 'plot':
				query = Cypher.upload_table_plot
			else:
				query = 'MATCH (d:Data) return d.found'
			result = tx.run(
				query,
				username = username,
				filename = ("file:///" + username + '/' + filename),
				submission_type = submission_type,
				traits = traits,
				form_user = form_user,
				form_date = form_date,
				form_time = form_time,
				neo4j_time = neo4j_time
			)
	fcount=0
	ncount=0
	for record in result:
		if record['d.found'] == True:
			fcount = fcount + 1
		elif record['d.found'] == False:
			ncount = ncount + 1
	return [ncount, fcount]
