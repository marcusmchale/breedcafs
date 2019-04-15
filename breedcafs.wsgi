import sys, os

CWD = os.path.dirname(os.path.abspath(__file__))
sys.path.insert (0, CWD)

activate_this = os.path.join(CWD, 'venv/bin/activate_this.py')
execfile(activate_this, dict(__file__=activate_this))

def application(environ, start_response):
	ENVARS = [
		'NEO4J_USERNAME',
		'NEO4J_PASSWORD',
		'MAIL_USERNAME',
		'MAIL_PASSWORD',
		'BREEDCAFS_LOG',
		'NEO4J_DRIVER_LOG'
	]
	for VAR in ENVARS:
		os.environ[VAR] = environ.get(VAR, '')
	from app import app as _application
	print environ
	return _application(environ, start_response)
