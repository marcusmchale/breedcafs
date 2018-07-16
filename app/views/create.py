from app import app, ServiceUnavailable, AuthError
from flask import (
	session,
	flash, 
	request, 
	redirect, 
	url_for, 
	render_template,
	make_response, 
	# send_from_directory,
	jsonify
)
from app.models import (
	Lists,
	Fields
	# User,
	# Samples
)
from app.forms import (
	LocationForm,
	AddCountry, 
	AddRegion, 
	AddFarm, 
	AddTrial,
	TrialsForm,
	AddBlock, 
	# BlocksForm,
	AddTreesForm
	# CreateTraits
)
# from app.emails import send_attachment, send_static_attachment, send_email

from flask.views import MethodView

# from datetime import datetime


# endpoints to get locations as tuples for forms
class Countries(MethodView):
	def get(self):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				countries = Lists('Country').create_list_tup('name', 'name')
				response = make_response(jsonify(countries))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable, AuthError):
				flash("Database unavailable")
				return redirect(url_for('index'))


class Regions(MethodView):
	def get(self, country):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				regions = Lists('Country').get_connected('name', country, 'IS_IN')
				response = make_response(jsonify([(i.lower(), i) for i in regions]))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable, AuthError):
				flash("Database unavailable")
				return redirect(url_for('index'))


class Farms(MethodView):
	def get(self, country, region):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				farms = Fields(country).get_farms(region)
				response = make_response(jsonify([(i.lower(),i) for i in farms]))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable, AuthError):
				flash("Database unavailable")
				return redirect(url_for('index'))


class Trials(MethodView):
	def get(self, country, region, farm):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				trials = Fields(country).get_trials_tup(region, farm)
				response = make_response(jsonify(trials))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable, AuthError):
				flash("Database unavailable")
				return redirect(url_for('index'))


class Blocks(MethodView):
	def get(self, trial_uid):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				blocks = Fields.get_blocks_tup(trial_uid)
				response = make_response(jsonify(blocks))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable, AuthError):
				flash("Database unavailable")
				return redirect(url_for('index'))


class TreeCount(MethodView):
	def get(self, trial_uid):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				tree_count = Fields.get_treecount(trial_uid)
				response = make_response(jsonify(tree_count))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable, AuthError):
				flash("Database unavailable")
				return redirect(url_for('index'))


@app.route('/create', methods=['GET', 'POST'])
def create():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			location_form = LocationForm.update()
			add_country_form = AddCountry()
			add_region_form = AddRegion()
			add_farm_form = AddFarm()
			add_trial_form = AddTrial()
			add_block_form = AddBlock()
			add_trees_form = AddTreesForm()
			return render_template(
				'create.html',
				location_form = location_form,
				add_country_form = add_country_form,
				add_region_form = add_region_form,
				add_farm_form = add_farm_form,
				add_trial_form = add_trial_form,
				add_block_form = add_block_form,
				add_trees_form = add_trees_form,
				title = 'Register fields and submit details'
			)
		except (ServiceUnavailable, AuthError):
				flash("Database unavailable")
				return redirect(url_for('index'))


@app.route('/add_country', methods=["POST"])
def add_country():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			form = AddCountry()
			text_country = request.form['text_country'].strip()
			if form.validate_on_submit():
				found = Fields(text_country).find_country()
				if found:
					return jsonify({"found": found[0]['name']})
				else:
					result = Fields(text_country).add_country()
					return jsonify({"submitted": result[0]})
			else:
				return jsonify([form.errors])
		except (ServiceUnavailable, AuthError):
			flash("Database unavailable")
			return redirect(url_for('index'))


@app.route('/add_region', methods = ["POST"])
def add_region():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			form = AddRegion()
			country = request.form['country']
			text_region = request.form['text_region'].strip()
			if form.validate_on_submit():
				if country in ['', 'None']:
					return jsonify([{"country": ["Please select a country to add a new region"]}])
				else:
					found = Fields(country).find_region(text_region)
					if found:
						return jsonify({"found": found[0]['name']})
					else:
						result = Fields(country).add_region(text_region)
						return jsonify({"submitted": result[0]})
			else:
				return jsonify([form.errors])
		except (ServiceUnavailable, AuthError):
				flash("Database unavailable")
				return redirect(url_for('index'))


@app.route('/add_farm', methods=["POST"])
def add_farm():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			form = AddFarm()
			country = request.form['country']
			region = request.form['region']
			text_farm = request.form['text_farm'].strip()
			if form.validate_on_submit():
				if bool({country, region} & {'', 'None'}):
					return jsonify([{"region": ["Please select a region to add a new farm"]}])
				else:
					found = Fields(country).find_farm(region, text_farm)
					if found:
						return jsonify({"found": found[0]['name']})
					else:
						result = Fields(country).add_farm(region, text_farm)
						return jsonify({"submitted": result[0]})
			else:
				return jsonify([form.errors])
		except (ServiceUnavailable, AuthError):
				flash("Database unavailable")
				return redirect(url_for('index'))


@app.route('/add_trial', methods=["POST"])
def add_trial():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			form = AddTrial()
			country = request.form['country']
			region = request.form['region']
			farm = request.form['farm']
			text_trial = request.form['text_trial'].strip()
			if form.validate_on_submit():
				if bool({country, region, farm} & {'', 'None'}):
					return jsonify([{"farm": ["Please select a farm to add a new trial"]}])
				else:
					found = Fields(country).find_trial(region, farm, text_trial)
					if found:
						return jsonify({"found": {'uid': found[0]['uid'], 'name': found[0]['name']}})
					else:
						result = Fields(country).add_trial(region, farm, text_trial)
						return jsonify({"submitted": {'uid': result[0]['uid'], 'name': result[0]['name']}})
			else:
				return jsonify([form.errors])
		except (ServiceUnavailable, AuthError):
				flash("Database unavailable")
				return redirect(url_for('index'))


@app.route('/add_block', methods=["POST"])
def add_block():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			location_form = LocationForm.update()
			add_block_form = AddBlock()
			if all([location_form.validate_on_submit(), add_block_form.validate_on_submit()]):
				trial_uid = int(request.form['trial'])
				text_block = request.form['text_block'].strip()
				if trial_uid in ['','None']:
					return jsonify([{"country": ["Please select a country to add a new region"]}])
				else:
					found = Fields.find_block(trial_uid, text_block)
					if found:
						return jsonify({"found": {'uid': found[0]['uid'], 'name': found[0]['name']}})
					else:
						result = Fields.add_block(trial_uid, text_block)
						return jsonify({"submitted": {'uid': result[0]['uid'], 'name': result[0]['name']}})
			else:
				errors = jsonify([location_form.errors, add_block_form.errors])
				return errors
		except (ServiceUnavailable, AuthError):
				flash("Database unavailable")
				return redirect(url_for('index'))


@app.route('/add_trees', methods=["POST"])
def add_trees():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			location_form = LocationForm.update()
			add_trees_form = AddTreesForm()
			if all([location_form.validate_on_submit(), add_trees_form.validate_on_submit()]):
				trial_uid = int(request.form['trial'])
				count = int(request.form['count'])
				block_uid = request.form['block'] if request.form['block'] != '' else None
				new_tree_count = Fields.add_trees(trial_uid, count, block_uid)[0]
				return jsonify({'submitted' : str(new_tree_count) + ' trees registered</a>'})
			else:
				errors = jsonify([location_form.errors, add_trees_form.errors])
				return errors
		except (ServiceUnavailable, AuthError):
				flash("Database unavailable")
				return redirect(url_for('index'))


