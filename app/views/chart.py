from app import app
from neo4j.exceptions import ServiceUnavailable, AuthError
from flask import session, flash, redirect, url_for, request, jsonify
from app.models import Chart
from datetime import datetime, timedelta


@app.route("/json_submissions")
def json_submissions():
	try:
		tomorrow = (datetime.utcnow()+timedelta(days=1)).strftime("%Y-%m-%d")
		yesterday = (datetime.utcnow()-timedelta(days=7)).strftime("%Y-%m-%d")
		return Chart().get_submissions_range(session['username'], yesterday, tomorrow)
	except (ServiceUnavailable, AuthError):
		flash("Database unavailable")
		return redirect(url_for('index'))


@app.route("/json_fields_treecount")
def json_fields_treecount():
	try:
		return Chart().get_fields_treecount()
	except (ServiceUnavailable, AuthError):
		flash("Database unavailable")
		return redirect(url_for('index'))


@app.route("/item_count")
def item_count():
	level = request.args.get('level', None)
	country = request.args.get('country', None)
	region = request.args.get('region', None)
	farm = request.args.get('farm', None)
	field_uid = request.args.get('field_uid', None)
	field_uid_list = request.args.get('field_uid_list', None)
	block_uid = request.args.get('block_uid', None)
	block_id_list = request.args.get('block_id_list', None)
	tree_id_list = request.args.get('tree_id_list', None)
	sample_id_list = request.args.get('sample_id_list', None)
	if level:
		try:
			count = Chart.get_item_count(
				level,
				country,
				region,
				farm,
				field_uid,
				field_uid_list,
				block_uid,
				block_id_list,
				tree_id_list,
				sample_id_list
			)
			return jsonify({
				"item_count": count
			})
		except (ServiceUnavailable, AuthError):
			return jsonify({
				'result': 'Database unavailable'
			})
	else:
		return jsonify({
			'result': 'Select level'
		})




