import sys, os
sys.path.insert (0, '/var/www/breedcafs')

activate_this = '/var/www/breedcafs/venv/bin/activate_this.py'
execfile(activate_this, dict(__file__=activate_this))

def application(environ, start_response):
	ENVARS = [
		'NEO4J_USERNAME',
		'NEO4J_PASSWORD',
		'MAIL_USERNAME',
		'MAIL_PASSWORD'
	]
	for VAR in ENVARS:
		os.environ[VAR] = environ.get(VAR, '')
	from app import app as _application
	print environ
	return _application(environ, start_response)
