from flask import jsonify, request

from dbtools.views.custom_decorators import neo4j_required, login_required

from dbtools import app, neo4j_driver

from dbtools.models.get_counts import get_item_count


@app.route('/counts/item_count/')
@login_required
@neo4j_required
def tree_count():
	item_label = request.args.get('item_label', None)
	country = request.args.get('country', None)
	region = request.args.get('region', None)
	farm = request.args.get('farm', None)
	uid = request.args.get('uid', None)
	return jsonify(neo4j_driver.read(get_item_count(item_label, country=country, region=region, farm=farm, uid=uid)))
