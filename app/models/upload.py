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
				session.write_transaction(self._submit)
			return True
		else:
			return False
	def _submit(self, tx):
			tx.run(Cypher.upload_submit, username=self.username,
				filename='file://' + self.filename,
				submission_time=str(datetime.now()),
				submission_type=self.submission_type)