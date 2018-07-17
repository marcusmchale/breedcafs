# -*- coding: utf-8 -*-

import os, logging
from celery import Celery
from redis import StrictRedis, exceptions
from flask import Flask

app = Flask(__name__, instance_relative_config=True)
app.config.from_object('config')
app.config.from_pyfile('config.py')

# configure logging
logging.basicConfig(filename=app.config['BREEDCAFS_LOG'], 
	level = logging.DEBUG,
	format = '%(asctime)s %(levelname)-8s %(message)s',
	datefmt= '%Y-%m-%d %H:%M:%S')

# celery for scheduling large uploads
celery = Celery(
	app,
	backend = app.config['CELERY_RESULT_BACKEND'],
	broker = app.config['CELERY_BROKER_URL']
)

celery.conf.update(
	task_serializer = 'pickle',
	result_serializer = 'pickle',
	event_serializer = 'pickle',
	accept_content = ['pickle', 'json']
)

# and also use redis (not just with celery) for basic local data store like login attempts and caching
redis_store = StrictRedis(host= 'localhost', port = 6379, db=0)
redis_exceptions = exceptions

from neo4j.v1 import GraphDatabase, ServiceUnavailable, AuthError
from neo4j.util import watch

from app import views

# these are the variable view rules for retrieving lists:
app.add_url_rule('/location/countries/', 
	view_func=views.Countries.as_view('countries'),
	methods=['GET'])

app.add_url_rule('/location/<country>/', 
	view_func=views.Regions.as_view('regions'),
	methods=['GET'])

app.add_url_rule('/location/<country>/<region>/', 
	view_func=views.Farms.as_view('farms'),
	methods=['GET'])

app.add_url_rule('/location/<country>/<region>/<farm>/', 
	view_func=views.Trials.as_view('trials'),
	methods=['GET'])

app.add_url_rule('/location/blocks/<trial_uid>/',
	view_func=views.Blocks.as_view('blocks'),
	methods=['GET'])

app.add_url_rule('/location/treecount/<trial_uid>/',
	view_func=views.TreeCount.as_view('treecount'),
	methods=['GET'])

app.add_url_rule('/sample_reg/tissues/', 
	view_func=views.Tissues.as_view('tissues'),
	methods=['GET'])

app.add_url_rule('/sample_reg/storage_methods/', 
	view_func=views.StorageMethods.as_view('storage_methods'),
	methods=['GET'])

