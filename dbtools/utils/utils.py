from pathlib import PurePath

from werkzeug.utils import secure_filename

from dbtools import app


def valid_extension(raw_filename, submission_type):
	filename = secure_filename(raw_filename)
	suffix = PurePath(filename).suffix
	return suffix in app.config[submission_type]

