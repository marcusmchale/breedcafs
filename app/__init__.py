# -*- coding: utf-8 -*-
import logging.config
import os
from celery import Celery
from redis import StrictRedis, exceptions
from flask import Flask
from flask_bcrypt import Bcrypt


app = Flask(__name__, instance_relative_config=True)
app.config.from_object('config')
app.config.from_pyfile('config.py')

# log file called from envars
# set in httpd.conf if using Apache and in /etc/init.d/celeryd for celery
# Needed to avoid conflict for access to these whether function is called by celery or by web server
# needs to be after app init or wsgi is not running yet so can't get apache envars
DBTOOLS_LOG = os.environ.get('DBTOOLS_LOG')
NEO4J_LOG = os.environ.get('NEO4J_LOG')

logger = logging.getLogger(__name__)

logger.info(f"Logging to: {DBTOOLS_LOG}, {NEO4J_LOG}")

LOG_CONFIG = {
	'version': 1,
	'disable_existing_loggers': True,
	'formatters': {
		'standard': {
			'format': '%(asctime)s [%(levelname)s]: %(message)s'
		},
		'named': {
			'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
		},
	},
	'handlers': {
		'app': {
			'formatter': 'named',
			'class': 'logging.FileHandler',
			'filename': DBTOOLS_LOG
		},
		'neo4j': {
			'formatter': 'named',
			'class': 'logging.FileHandler',
			'filename': NEO4J_LOG
		},
	},
	'loggers': {
		'app': {
			'level': 'INFO',
			'handlers': ['app'],
			'propagate': True
		},
		'neo4j': {
			'level': 'INFO',
			'handlers': ['neo4j'],
			'propagate': True
		}
	}
}


logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)
logger.info("Start")


bcrypt = Bcrypt(app)

# celery for scheduling large uploads
celery = Celery(
	app.name,
	backend=app.config['CELERY_RESULT_BACKEND'],
	broker=app.config['CELERY_BROKER_URL']
)


celery.conf.update(
	task_serializer='pickle',
	result_serializer='pickle',
	event_serializer='pickle',
	accept_content=['pickle', 'json']
)


# and also use redis (not just with celery) for basic local data store like login attempts and caching
redis_store = StrictRedis(host='localhost', port=app.config['REDIS_PORT'], db=0)
redis_exceptions = exceptions

from app import views

# these are the variable view rules for retrieving lists:
app.add_url_rule(
	'/location/countries/',
	view_func=views.ListCountries.as_view('countries'),
	methods=['GET']
)

app.add_url_rule(
	'/location/<country>/',
	view_func=views.ListRegions.as_view('regions'),
	methods=['GET']
)

app.add_url_rule(
	'/location/<country>/<region>/',
	view_func=views.ListFarms.as_view('farms'),
	methods=['GET']
)

app.add_url_rule(
	'/location/<country>/<region>/<farm>/',
	view_func=views.ListFields.as_view('fields'),
	methods=['GET']
)

app.add_url_rule(
	'/location/blocks/<field_uid>/',
	view_func=views.ListBlocks.as_view('blocks'),
	methods=['GET']
)

app.add_url_rule(
	'/location/treecount/<uid>/',
	view_func=views.TreeCount.as_view('treecount'),
	methods=['GET']
)
