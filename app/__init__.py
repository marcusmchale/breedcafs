# -*- coding: utf-8 -*-
import os, logging
from celery import Celery
from redis import StrictRedis, exceptions
from flask import Flask

app = Flask(__name__, instance_relative_config=True)
app.config.from_object('config')
app.config.from_pyfile('config.py')

# configure logging
logging.basicConfig(
	filename=app.config['BREEDCAFS_LOG'],
	level=logging.DEBUG,
	format='%(asctime)s %(levelname)-8s %(message)s',
	datefmt='%Y-%m-%d %H:%M:%S'
)

# celery for scheduling large uploads
celery = Celery(
	app,
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

from neo4j import GraphDatabase, ServiceUnavailable, TransactionError
from neo4j.exceptions import SecurityError
#from neo4j import watch

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

#app.add_url_rule(
#	'/record/<record_type>/<item_level>/',
#	view_func=views.ListFeatureGroups.as_view('feature_groups'),
#	methods=['GET']
#)
#
#app.add_url_rule(
#	'/record/<record_type>/<item_level>/<feature_group>/',
#	view_func=views.ListFeatures.as_view('features'),
#	methods=['GET']
#)
