#!/usr/bin/env python
# -*- coding: utf-8 -*-

# NB: only upper case config variables are stored in the app.Config object

#####
# Make a copy of this in your flask instance path
# Set the values in that copy according to your environment
#####

import os

# The layout template looks for this value to load a splash warning you are on the development site
# Also checked for which ports to access Neo4j and Redis instances
# Set to false for production
DEV = True
SERVER_NAME = 'www.breedcafs-db.eu'

# URI for bolt driver, 7687 for production, 7688 for dev
if DEV:
	BOLT_URI = "bolt://localhost:7688"
else:
	BOLT_URI = "bolt://localhost:7687"


# redis and celery config, 6379 for production, 6380 for dev
if DEV:
	REDIS_PORT = 6380
else:
	REDIS_PORT = 6379
CELERY_BROKER_URL = 'redis://localhost:%s/0' % REDIS_PORT
CELERY_RESULT_BACKEND = 'redis://localhost:%s/0' % REDIS_PORT

# Path to the breedcafs directory, be sure to set this according to your install location
INSTALL_PATH = '/path/to/breedcafs'

# Be sure to change all of these values, they are used in checking session cookies etc.
SECRET_KEY = 'some-secret-key_MAKE_SURE_TO_CHANGE_THIS'
CONFIRM_EMAIL_SALT = "ANOTHER_KEY_TO_SET"
PASSWORD_RESET_SALT = "YET_ANOTHER_KEY_TO_SET"

# log file called from envars
# set in httpd.conf if using Apache and in /etc/init.d/celeryd for celery
# Needed to avoid conflict for access to these whether function is called by celery or by web server
BREEDCAFS_LOG = os.environ.get('BREEDCAFS_LOG')
NEO4J_DRIVER_LOG = os.environ.get('NEO4J_DRIVER_LOG')

# PLace upload and download directories in the instance folder
IMPORT_FOLDER = 'import'
EXPORT_FOLDER = 'export'
# to enable uploads you will also need to enable import from this directory in neo4j
# found in /etc/neo4j/neo4j.conf
# neo4j config: dbms.directories.import = 'insert UPLOAD_FOLDER path'

# I am using the setgid bit so both celery and the web server can write to import path
# your solution may vary depending on your web infrastructure
IMPORT_FOLDER_PERMISSIONS = 0o2775
EXPORT_FOLDER_PERMISSIONS = 0o2775


CELERYGRPNAME = "CELERY_USER_GROUP"
WEBSERVERGRPNAME = "WEB_SERVER_USER_GROUP"

# email server configuration (i am currently using a gmail account)
MAIL_SERVER = 'smtp.gmail.com'
MAIL_PORT = 465
MAIL_USE_SSL = True
MAIL_USE_TLS = False
MAIL_USERNAME = os.environ.get('MAIL_USERNAME')
MAIL_PASSWORD = os.environ.get('MAIL_PASSWORD')
MAIL_ASCII_ATTACHMENTS = True

# administrator list
# included as sent from (except broken with gmail SMTP)
ADMINS = ['email_address@mail_server.com']

# Partners details e.g.
PARTNERS = [
	{'BASED_IN': 'Ireland', 'name': 'NUIG', 'fullname': 'National University of Ireland, Galway'}
]

ITEM_LEVELS = [
	"Field",
	"Block",
	"Tree",
	"Sample"
]

# NB: only upper case config variables are stored in the app.Config object
RECORD_TYPES = [
	'property',
	'trait',
	'condition',
	'curve'
]

# record type to worksheet name dictionary
WORKSHEET_NAMES = {
	'hidden': 'hidden',
	'input_details': 'Input Variables (Reference)',
	'item_details': 'Item Details (Reference)',
	'mixed': 'Records',
	'property': 'Properties (Input)',
	'trait': 'Traits (Input)',
	'condition': 'Conditions (Input)'
}

WORKSHEET_TYPES = {
	v.lower(): k for k, v in WORKSHEET_NAMES.items()
}

REFERENCE_WORKSHEETS = ['item_details', 'input_details', 'hidden']

REPLICATED_RECORD_TYPES = ['trait', 'curve']

# The below are used in initialise_db.py
CONSTRAINTS = [
	# User/Partner
	{'node': 'User', 'property': 'username_lower', 'constraint': 'IS UNIQUE'},
	{'node': 'Partner', 'property': 'name_lower', 'constraint': 'IS UNIQUE'},
	# Input variables
	{'node': 'Input', 'property': 'name_lower', 'constraint': 'IS UNIQUE'},
	# Items (Fields/Blocks/Trees/Samples)
	# all these should be created with the Item label
	{'node': 'Item', 'property': 'uid', 'constraint': 'IS UNIQUE'},
	# Counters
	{'node': 'Counter', 'property': 'uid', 'constraint': 'IS UNIQUE'},
	# Country is the top level location specifier
	{'node': 'Country', 'property': 'name_lower', 'constraint': 'IS UNIQUE'},
	# Genotype property
	{'node': 'Variety', 'property': 'name_lower', 'constraint': 'IS UNIQUE'}
]

# we store indexed traits as lower case for indexed matching e.g. name_lower = toLower(name)
# case insensitive indexed search isn't supported in base neo4j, at least up to v3
# it would be possible with manual indexing and Lucene query syntax

INDEXES = [
	# indexes are also created on anything in the unique constraints list
	{'label': 'Country', 'property': 'name'},
	{'label': 'Region', 'property': 'name'},
	{'label': 'Farm', 'property': 'name'},
	{'label': 'Field', 'property': 'name'},
	{'label': 'Block', 'property': 'name'},
	{'label': 'Region', 'property': 'name_lower'},
	{'label': 'Farm', 'property': 'name_lower'},
	{'label': 'Field', 'property': 'name_lower'},
	{'label': 'Block', 'property': 'name_lower'},
	{'label': 'Block', 'property': 'id'},
	{'label': 'Tree', 'property': 'id'},
	{'label': 'Sample', 'property': 'id'},
	{'label': 'Field', 'property': 'uid'},
	{'label': 'Block', 'property': 'uid'},
	{'label': 'Tree', 'property': 'uid'},
	{'label': 'Sample', 'property': 'uid'}
]

REQUIRED_FIELDNAMES = {
	'mixed': {
		'uid',
		'input variable',
		'value'
	},
	'property': {'uid'},
	'trait': {'uid', 'date'},
	'curve': {'uid', 'date'},
	'condition': {'uid'}
}

OPTIONAL_FIELDNAMES = {
	'mixed': {
		'replicate',
		'time',
		'period',
		'submitted at'
	},
	'property': {'person'},
	'trait': {'person', 'time'},
	'curve': {'person', 'time'},
	'condition': {'person', 'start date', 'start time', 'end date', 'end time'}
}

REFERENCE_FIELDNAMES = {
		'country',
		'region',
		'farm',
		'field',
		'field uid',
		'block',
		'block id',
		'source trees',
		'source samples',
		'name',
		'row',
		'column',
		'recorded by',
		'submitted by',
		'partner'
}

UID_LETTERS = {'b': 'block', 't': 'tree', 's': 'sample'}

BATCH_PROCESS_ROW_COUNT = 100
BATCH_PROCESS_MAX_ERROR_ROWS = 25
PERSON_FIELD_MAX_LEN = 100

