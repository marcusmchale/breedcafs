from app import app, ServiceUnavailable
from flask import (session, 
	flash, 
	request, 
	redirect, 
	url_for, 
	render_template,
	make_response, 
	jsonify,
	send_from_directory)
from app.models import (Lists, 
	Fields, 
	User, 
	Samples)
from app.forms import (LocationForm, 
	AddCountry, 
	AddRegion, 
	AddFarm, 
	AddPlot, 
	PlotsForm, 
	AddBlock, 
	BlocksForm, 
	AddTreesForm,
	CreateTraits)
from app.emails import send_attachment, send_static_attachment, send_email
from flask.views import MethodView
from datetime import datetime

#endpoints to get locations as tuples for forms
class countries(MethodView):
	def get(self):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				countries = Lists('Country').create_list_tup('name','name')
				response = make_response(jsonify(countries))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable):
				flash("Database unavailable")
				return redirect(url_for('index'))


class regions(MethodView):
	def get(self, country):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				REGIONS = Lists('Country').get_connected('name', country, 'IS_IN')
				response = make_response(jsonify([(REGIONS[i],REGIONS[i].title()) for i, items in enumerate(REGIONS)]))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable):
				flash("Database unavailable")
				return redirect(url_for('index'))


class farms(MethodView):
	def get(self, country, region):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				FARMS = Fields(country).get_farms(region)
				response = make_response(jsonify([(FARMS[i],FARMS[i].title()) for i, items in enumerate(FARMS)]))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable):
				flash("Database unavailable")
				return redirect(url_for('index'))

class plots(MethodView):
	def get(self, country, region, farm):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				plots = Fields(country).get_plots_tup(region, farm)
				response = make_response(jsonify(plots))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable):
				flash("Database unavailable")
				return redirect(url_for('index'))

class blocks(MethodView):
	def get(self, plotID):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				blocks = Fields.get_blocks_tup(plotID)
				response = make_response(jsonify(blocks))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable):
				flash("Database unavailable")
				return redirect(url_for('index'))

class treecount(MethodView):
	def get(self, plotID):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				treecount = Fields.get_treecount(plotID)
				response = make_response(jsonify(treecount))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable):
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
			add_country = AddCountry()
			add_region = AddRegion()
			add_farm = AddFarm()
			add_plot = AddPlot()
			add_block_form = AddBlock()
			add_trees_form = AddTreesForm()
			return render_template('create.html',
				location_form = location_form,
				add_country = add_country, 
				add_region = add_region, 
				add_farm = add_farm, 
				add_plot = add_plot,
				add_block_form = add_block_form,
				add_trees_form = add_trees_form,
				title = 'Register fields and submit details')
		except (ServiceUnavailable):
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
			text_country = request.form['text_country'].lower().strip()
			if form.validate_on_submit():
				found = Fields(text_country).find_country()
				if found:
					return jsonify({"found" : found[0]['name'].title()})
				else:
					result = Fields(text_country).add_country()
					return jsonify({"submitted": result[0].title()})
			else:
				return jsonify([form.errors])
		except (ServiceUnavailable):
			flash("Database unavailable")
			return redirect(url_for('index'))

@app.route('/add_region', methods=["POST"])
def add_region():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			form = AddRegion()
			country = request.form['country']
			text_region = request.form['text_region'].lower().strip()
			if form.validate_on_submit():
				if country in ['','None']:
					return jsonify([{"country":["Please select a country to add a new region"]}])
				else:
					found = Fields(country).find_region(text_region)
					if found:
						return jsonify({"found": found[0]['name'].title()})
					else:
						result = Fields(country).add_region(text_region)
						return jsonify({"submitted": result[0].title()})
			else:
				return jsonify([form.errors])
		except (ServiceUnavailable):
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
			text_farm = request.form['text_farm'].lower().strip()
			if form.validate_on_submit():
				if bool(set([country,region]) & set(['','None'])):
					return jsonify([{"region": ["Please select a region to add a new farm"]}])
				else:
					found = Fields(country).find_farm(region, text_farm)
					if found:
						return jsonify({"found": found[0]['name'].title()})
					else:
						result = Fields(country).add_farm(region, text_farm)
						return jsonify({"submitted": result[0].title()})
			else:
				return jsonify([form.errors])
		except (ServiceUnavailable):
				flash("Database unavailable")
				return redirect(url_for('index'))

@app.route('/add_plot', methods=["POST"])
def add_plot():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			form = AddPlot()
			country = request.form['country']
			region = request.form['region']
			farm = request.form['farm']
			text_plot = request.form['text_plot'].lower().strip()
			if form.validate_on_submit():
				if bool(set([country,region,farm]) & set(['','None'])):
					return jsonify([{"farm":["Please select a farm to add a new plot"]}])
				else:
					found = Fields(country).find_plot(region, farm, text_plot)
					if found:
						return jsonify({"found": {'uid': found[0]['uid'], 'name': found[0]['name'].title()}})
					else:
						result = Fields(country).add_plot(region, farm, text_plot)
						return jsonify({"submitted": {'uid': result[0]['uid'], 'name': result[0]['name'].title()}})
			else:
				return jsonify([form.errors])
		except (ServiceUnavailable):
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
				plotID = int(request.form['plot'])
				text_block = request.form['text_block'].lower().strip()
				if plotID in ['','None']:
					return jsonify([{"country": ["Please select a country to add a new region"]}])
				else:
					found = Fields.find_block(plotID, text_block)
					if found:
						return jsonify({"found": {'uid': found[0]['uid'], 'name': found[0]['name'].title()}})
					else:
						result = Fields.add_block(plotID, text_block)
						return jsonify({"submitted": {'uid': result[0]['uid'], 'name': result[0]['name'].title()}})
			else:
				errors = jsonify([location_form.errors, add_block_form.errors])
				return errors
		except (ServiceUnavailable):
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
				plotID = int(request.form['plot'])
				count = int(request.form['count'])
				blockUID = request.form['block'] if request.form['block'] != '' else None
				new_tree_count = Fields.add_trees(plotID, count, blockUID)[0]
				return jsonify({'submitted' : str(new_tree_count) + ' trees registered</a>'})
			else:
				errors = jsonify([location_form.errors, add_trees_form.errors])
				return errors
		except (ServiceUnavailable):
				flash("Database unavailable")
				return redirect(url_for('index'))


