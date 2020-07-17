from app import app, os, logging
from werkzeug.utils import secure_filename


class Resumable:
	def __init__(self, username, raw_filename, resumable_id):
		self.username = username
		self.resumable_id = resumable_id
		self.chunk_paths = None
		user_upload_dir = os.path.join(
			app.config['UPLOAD_FOLDER'],
			self.username
		)
		if not os.path.isdir(user_upload_dir):
			logging.debug('Creating upload path for user: %s', username)
			os.mkdir(user_upload_dir, mode=app.config['IMPORT_FOLDER_PERMISSIONS'])
		self.temp_dir = os.path.join(
			app.config['UPLOAD_FOLDER'],
			self.username,
			self.resumable_id
		)
		if not os.path.isdir(self.temp_dir):
			logging.debug('Creating upload path for user: %s', username)
			os.mkdir(self.temp_dir, mode=app.config['IMPORT_FOLDER_PERMISSIONS'])
		self.filename = secure_filename(raw_filename)
		self.file_path = os.path.join(
			app.config['UPLOAD_FOLDER'],
			self.username,
			self.filename
		)

	@staticmethod
	def allowed_file(raw_filename, submission_type=None):
		if '.' in raw_filename:
			file_extension = raw_filename.rsplit('.', 1)[1].lower()
			if file_extension in app.config['ALLOWED_EXTENSIONS']:
				if submission_type:
					if submission_type in ['db', 'table']:
						if file_extension not in ['csv', 'xlsx']:
							return False
						else:
							return True
					elif submission_type == 'seq':
						if file_extension not in ['fastq', 'gz', 'zip']:
							return False
						else:
							return True
					else:
						return False
				else:
					return True
		else:
			return False

	def get_chunk_name(self, chunk_number):
		return self.filename + "_part%03d" % chunk_number

	def check_for_chunk(self, resumable_id, chunk_number):
		temp_dir = os.path.join(
			app.instance_path,
			app.config['UPLOAD_FOLDER'],
			self.username,
			resumable_id
		)
		chunk_file = os.path.join(
			temp_dir,
			self.get_chunk_name(chunk_number)
		)
		logging.debug('Getting chunk: %s', chunk_file)
		if os.path.isfile(chunk_file):
			return True
		else:
			return False

	def save_chunk(self, chunk_data, chunk_number):
		chunk_name = self.get_chunk_name(chunk_number)
		chunk_file = os.path.join(self.temp_dir, chunk_name)
		chunk_data.save(chunk_file)
		logging.debug('Saved chunk: %s', chunk_file)

	def complete(self, total_chunks):
		self.chunk_paths = [
			os.path.join(self.temp_dir, self.get_chunk_name(x))
			for x in range(1, total_chunks + 1)
		]
		return all([
			os.path.exists(p) for p in self.chunk_paths
		])

	def assemble(self, size):
		# replace file if same name already found
		if os.path.isfile(self.file_path):
			os.unlink(self.file_path)
		with open(os.open(self.file_path, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o640), "ab") as target_file:
			for p in self.chunk_paths:
				stored_chunk_filename = p
				stored_chunk_file = open(stored_chunk_filename, 'rb')
				target_file.write(stored_chunk_file.read())
				stored_chunk_file.close()
				os.unlink(stored_chunk_filename)
			target_file.close()
			os.rmdir(self.temp_dir)
			logging.debug('File saved to: %s', self.file_path)
		return os.path.getsize(self.file_path) == size
