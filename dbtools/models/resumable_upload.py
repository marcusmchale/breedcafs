import os
from pathlib import Path, PurePath

from werkzeug.utils import secure_filename

from dbtools import app, logging


class Resumable:
	def __init__(self, username, raw_filename, resumable_id):
		self.filename = secure_filename(raw_filename)
		self.file_path = Path(
			app.config['UPLOAD_FOLDER'],
			username,
			self.filename
		)
		self.chunk_paths = None
		self.temp_dir = Path(
			app.config['UPLOAD_FOLDER'],
			username,
			resumable_id
		)
		if not self.temp_dir.is_dir():
			logging.debug('Creating upload path for user: %s', username)
			self.temp_dir.mkdir(
				parents=True,
				mode=app.config['IMPORT_FOLDER_PERMISSIONS']
			)

	def get_chunk_name(self, chunk_number):
		return self.filename + "_part%03d" % chunk_number

	def check_for_chunk(self, chunk_number):
		chunk_file = Path(
			self.temp_dir,
			self.get_chunk_name(chunk_number)
		)
		logging.debug('Getting chunk: %s', chunk_file)
		return chunk_file.is_file()

	def save_chunk(self, chunk_data, chunk_number):
		chunk_name = self.get_chunk_name(chunk_number)
		chunk_file = Path(self.temp_dir, chunk_name)
		chunk_data.save(chunk_file)
		logging.debug('Saved chunk: %s', chunk_file)

	def complete(self, total_chunks):
		self.chunk_paths = [
			Path(self.temp_dir, self.get_chunk_name(x))
			for x in range(1, total_chunks + 1)
		]
		return all([
			p.is_file() for p in self.chunk_paths
		])

	def assemble(self, size):
		# replace file if same name already found
		if self.file_path.is_file:
			self.file_path.unlink()
		with self.file_path.open("ab") as target_file:
			for p in self.chunk_paths:
				target_file.write(p.read_bytes())
				p.unlink()
		self.temp_dir.rmdir()
		logging.debug('File saved to: %s', self.file_path)
		return os.path.getsize(self.file_path) == size
