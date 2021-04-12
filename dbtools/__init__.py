import logging
from celery import Celery
from redis.client import Redis
from flask import Flask

from dbtools.neo4j_driver import Neo4jDriver

__all__ = [
	'app',
	'logging',
	'celery',
	'neo4j_driver',
	'redis_client',
	'views'
]


def create_app():
	app_instance = Flask(__name__, instance_relative_config=True)
	app_instance.config.from_object('config')
	app_instance.config.from_pyfile('config.py')
	return app_instance


app = create_app()

# configure logging
logging.basicConfig(
	filename=app.config['BREEDCAFS_LOG'],
	level=logging.DEBUG,
	format='%(asctime)s %(levelname)-8s %(message)s',
	datefmt='%Y-%m-%d %H:%M:%S'
)

# celery for scheduling large uploads
celery = Celery(
	app.name,
	backend=app.config['CELERY_RESULT_BACKEND'],
	broker=app.config['CELERY_BROKER_URL']
).conf.update(
	task_serializer='pickle',
	result_serializer='pickle',
	event_serializer='pickle',
	accept_content=['pickle', 'json']
)

neo4j_driver = Neo4jDriver()

# and also use redis (not just with celery) for basic local data store like login attempts and caching
redis_client = Redis(host='localhost', port=app.config['REDIS_PORT'], db=0)

# todo confirm that this late import is necessary for decorators etc in flask see:
# https://flask.palletsprojects.com/en/1.1.x/patterns/packages/
# noinspection PyPep8
import dbtools.views

