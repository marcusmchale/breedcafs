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
	def submit(self, submission_type, level):
		self.submission_type=submission_type
		self.level = level
		with driver.session() as session:
			return session.write_transaction(self._submit)
	def _submit(self, tx):
			fcount=0
			ncount=0
			if self.submission_type == 'FB':
				if self.level == 'tree':
					query = Cypher.upload_FB_tree
				elif self.level == 'block':
					query = Cypher.upload_FB_block
			for record in tx.run(query, username=self.username,
				filename= ("file:///" + self.username + '/' + self.filename),
				submission_type=self.submission_type):
				if record['d.found'] == True:
					fcount = fcount + 1
				elif record['d.found'] == False:
					ncount = ncount + 1
			return [ncount, fcount]
			#could be doing this with result.consume().counters