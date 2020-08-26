from flask import jsonify, request

from dbtools.views.custom_decorators import neo4j_required, login_required

from dbtools import app, neo4j_driver

from dbtools.models.get_locations import get_countries, get_regions, get_farms
from dbtools.models.get_items import get_fields, get_blocks



@app.route('/form_field_lists/fields/')
@login_required
@neo4j_required
def list_fields():
	country = request.args.get('country', None)
	region = request.args.get('region', None)
	farm = request.args.get('farm', None)
	return jsonify(neo4j_driver.read(get_fields(country, region, farm)))


@app.route('/form_field_lists/blocks/')
@login_required
@neo4j_required
def list_blocks():
	country = request.args.get('country', None)
	region = request.args.get('region', None)
	farm = request.args.get('farm', None)
	uid = request.args.get('uid', None)
	return jsonify(neo4j_driver.read(get_blocks(country, region, farm, uid)))


@app.route('/form_field_lists/countries')
@login_required
@neo4j_required
def list_countries():
	return jsonify(neo4j_driver.read(get_countries()))


@app.route('/form_field_lists/regions/')
@login_required
@neo4j_required
def list_regions():
	country = request.args.get('country', None)
	return jsonify(neo4j_driver.read(get_regions(country)))


@app.route('/form_field_lists/<country>/<region>/')
@login_required
@neo4j_required
def list_farms(country, region):
	country = request.args.get('country', None)
	region = request.args.get('region', None)
	return jsonify(neo4j_driver.read(get_farms(country, region)))

