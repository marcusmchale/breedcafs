# -*- coding: utf-8 -*-

import os, logging
from celery import Celery
from redis import StrictRedis
from flask import Flask

app = Flask(__name__, instance_relative_config=True)
app.config.from_object('config')
app.config.from_pyfile('config.py')

#configure logging
logging.basicConfig(filename=app.config['BREEDCAFS_LOG'], 
	level = logging.DEBUG,
	format = '%(asctime)s %(levelname)-8s %(message)s',
	datefmt= '%Y-%m-%d %H:%M:%S')

#celery for scheduling large uploads
celery = Celery(app , backend = app.config['CELERY_RESULT_BACKEND'], broker= app.config['CELERY_BROKER_URL'])

#and also use redis (not just with celery) for basic local datastore like login attempts and caching
redis_store = StrictRedis(host= 'localhost', port = 6379, db=0)

from neo4j.v1 import GraphDatabase, ServiceUnavailable
from neo4j.util import watch

from app import views

#these are the variable view rules for retrieving lists
app.add_url_rule('/location/countries/', 
	view_func=views.countries.as_view('countries'), 
	methods=['GET'])

app.add_url_rule('/location/<country>/', 
	view_func=views.regions.as_view('regions'), 
	methods=['GET'])

app.add_url_rule('/location/<country>/<region>/', 
	view_func=views.farms.as_view('farms'), 
	methods=['GET'])

app.add_url_rule('/location/<country>/<region>/<farm>/', 
	view_func=views.plots.as_view('plots'), 
	methods=['GET'])

app.add_url_rule('/location/blocks/<plotID>/',
	view_func=views.blocks.as_view('blocks'), 
	methods=['GET'])

app.add_url_rule('/sample_reg/tissues/', 
	view_func=views.tissues.as_view('tissues'), 
	methods=['GET'])

app.add_url_rule('/sample_reg/storage_methods/', 
	view_func=views.storage_methods.as_view('storage_methods'), 
	methods=['GET'])

