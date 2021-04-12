from pathlib import Path, PurePath

from werkzeug.utils import secure_filename
from dbtools.models.entities.enums import SubmissionType, FileExtension


from dbtools import app, logging


class File:
	def __init__(
			self,
			name: str,
			submission_type: SubmissionType,
			username: str
	):
		self.name = secure_filename(name)
		self.extension = FileExtension(PurePath(name).suffix.lower())
		self.submission_type = submission_type
		self.username = username


class ImportFile(File):
	def __init__(
			self,
			name: str,
			submission_type: SubmissionType,
			username: str
	):
		super().__init__(name, submission_type, username)
		user_path = Path(app.config['IMPORT_FOLDER'], username)
		if not user_path.is_dir():
			logging.debug('Creating import path: %s', user_path)
			self.path.mkdir(
				mode=app.config['IMPORT_FOLDER_PERMISSIONS']
			)
		self.path = Path(
			user_path,
			name
		)


class ResumableFile(ImportFile):
	def __init__(
			self,
			name: str,
			submission_type: SubmissionType,
			username: str,
			resumable_id
	):
		super().__init__(name, submission_type, username)
		self.chunk_paths = None
		self.temp_dir = Path(app.config['IMPORT_FOLDER'], username,	resumable_id)
		if not self.temp_dir.is_dir():
			logging.debug('Creating temp directory for resumable upload: %s', self.temp_dir)
			self.temp_dir.mkdir(
				mode=app.config['IMPORT_FOLDER_PERMISSIONS']
			)


class ExportFile(File):
	def __init__(
			self,
			name: str,
			submission_type: SubmissionType,
			username: str
	):
		super().__init__(name, submission_type, username)
		user_path = Path(app.config['EXPORT_FOLDER'], username)
		if not user_path.is_dir():
			logging.debug('Creating export path: %s', username)
			self.path.mkdir(
				mode=app.config['EXPORT_FOLDER_PERMISSIONS']
			)
		self.path = Path(
			user_path,
			name
		)
