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
	SelectionList,
	Fields
	# User,
	# Samples
)
from app.forms import (
	LocationForm,
	AddCountry, 
	AddRegion, 
	AddFarm, 
	AddField,
	FieldsForm,
	AddBlock, 
	# BlocksForm,
	AddTreesForm
	# CreateTraits
)
# from app.emails import send_attachment, send_static_attachment, send_email

from flask.views import MethodView

# from datetime import datetime


# endpoints to get locations as tuples for forms
class ListCountries(MethodView):
	def get(self):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				countries = SelectionList.get_countries()
				response = make_response(jsonify(countries))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable, AuthError):
				flash("Database unavailable")
				return redirect(url_for('index'))


class ListRegions(MethodView):
	def get(self, country):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				regions = SelectionList.get_regions(country)
				response = make_response(jsonify(regions))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable, AuthError):
				flash("Database unavailable")
				return redirect(url_for('index'))


class ListFarms(MethodView):
	def get(self, country, region):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				farms = SelectionList.get_farms(country, region)
				response = make_response(jsonify(farms))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable, AuthError):
				flash("Database unavailable")
				return redirect(url_for('index'))


class ListFields(MethodView):
	def get(self, country, region, farm):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				fields = SelectionList.get_fields(country, region, farm)
				response = make_response(jsonify(fields))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable, AuthError):
				flash("Database unavailable")
				return redirect(url_for('index'))


class ListBlocks(MethodView):
	def get(self, field_uid):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				blocks = SelectionList.get_blocks(field_uid=field_uid)
				response = make_response(jsonify(blocks))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable, AuthError):
				flash("Database unavailable")
				return redirect(url_for('index'))


class TreeCount(MethodView):
	@staticmethod
	def get(field_uid):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				tree_count = Fields.get_treecount(field_uid)
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
			add_field_form = AddField()
			add_block_form = AddBlock()
			add_trees_form = AddTreesForm()
			return render_template(
				'create.html',
				location_form = location_form,
				add_country_form = add_country_form,
				add_region_form = add_region_form,
				add_farm_form = add_farm_form,
				add_field_form = add_field_form,
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


@app.route('/add_field', methods=["POST"])
def add_field():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			form = AddField()
			country = request.form['country']
			region = request.form['region']
			farm = request.form['farm']
			text_field = request.form['text_field'].strip()
			if form.validate_on_submit():
				if bool({country, region, farm} & {'', 'None'}):
					return jsonify([{"farm": ["Please select a farm to add a new field"]}])
				else:
					found = Fields(country).find_field(region, farm, text_field)
					if found:
						return jsonify({"found": {'uid': found[0]['uid'], 'name': found[0]['name']}})
					else:
						result = Fields.add_field(country, region, farm, text_field)
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
				field_uid = int(request.form['field'])
				text_block = request.form['text_block'].strip()
				if field_uid in ['','None']:
					return jsonify([{"country": ["Please select a country to add a new region"]}])
				else:
					found = Fields.find_block(field_uid, text_block)
					if found:
						return jsonify({"found": {'uid': found[0]['uid'], 'name': found[0]['name']}})
					else:
						result = Fields.add_block(field_uid, text_block)
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
				field_uid = int(request.form['field'])
				count = int(request.form['count'])
				block_uid = request.form['block'] if request.form['block'] != '' else None
				new_tree_count = Fields.add_trees(field_uid, count, block_uid)[0]
				return jsonify({'submitted' : str(new_tree_count) + ' trees registered</a>'})
			else:
				errors = jsonify([location_form.errors, add_trees_form.errors])
				return errors
		except (ServiceUnavailable, AuthError):
				flash("Database unavailable")
				return redirect(url_for('index'))


