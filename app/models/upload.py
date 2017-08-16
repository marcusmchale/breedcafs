from datetime import datetime
from app import app
from app.cypher import Cypher
from user import User
from config import uri, driver, ALLOWED_EXTENSIONS


#User class related (all uploads are tied to a user) yet more specifically regarding uploads
class Upload(User):
	def __init__(self, username, filename):
		self.username=username
		self.filename=filename
	def allowed_file(self):
		return '.' in self.filename and \
			self.filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
	def submit(self, submission_type):
		self.submission_type=submission_type
		if self.submission_type == 'FB':
			with driver.session() as session:
				return session.write_transaction(self._submit)
		else:
			pass
	def _submit(self, tx):
			fcount=0
			ncount=0
			for record in tx.run(Cypher.upload_submit, username=self.username,
				filename='file://' + self.filename,
				submission_time=str(datetime.now()),
				submission_type=self.submission_type):
				if record['d.found'] == 'TRUE':
					fcount = fcount + 1
				elif record['d.found'] == 'FALSE':
					ncount = ncount + 1
			return [ncount, fcount]