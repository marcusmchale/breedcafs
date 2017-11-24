from app import os

DEBUG = False

ALLOWED_EXTENSIONS = set(['csv'])

#redis and celery config
CELERY_BROKER_URL = 'redis://localhost:6379/0'
CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'
