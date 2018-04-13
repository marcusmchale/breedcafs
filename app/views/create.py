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
	CustomTreesForm, 
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
				response = make_response(jsonify([(REGIONS[i],REGIONS[i]) for i, items in enumerate(REGIONS)]))
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
				response = make_response(jsonify([(FARMS[i],FARMS[i]) for i, items in enumerate(FARMS)]))
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
			text_country = request.form['text_country']
			if form.validate_on_submit():
				if Fields(text_country).find_country():
					return ("Country already found: " + str(text_country))
				else:
					Fields(text_country).add_country()
					return ("Country submitted: " + str(text_country))
			else:
				return form.errors["text_country"][0]
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
			text_region = request.form['text_region']
			if form.validate_on_submit():
				if country in ['','None']:
					return ('Please select a country to register a new region')
				elif Fields(country).find_region(text_region):
					return ("Region already found: " + str(text_region))
				else:
					Fields(country).add_region(text_region)
					return ("Region submitted: " + str(text_region))
			else:
				return form.errors["text_region"][0]
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
			text_farm = request.form['text_farm']
			if form.validate_on_submit():
				if bool(set([country,region]) & set(['','None'])):
					return ('Please select a country and region to register a new farm')
				elif Fields(country).find_farm(region, text_farm):
					return ("Farm already found: " + str(text_farm))
				else:
					Fields(country).add_farm(region, text_farm)
					return ("Farm submitted: " + str(text_farm))
			else:
				return form.errors["text_farm"][0]
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
			text_plot = request.form['text_plot']
			if form.validate_on_submit():
				if bool(set([country,region,farm]) & set(['','None'])):
					return ('Please select a country, region and farm to register a new plot')
				elif Fields(country).find_plot(region, farm, text_plot):
					return ("Plot already found: " + str(text_plot))
				else:
					Fields(country).add_plot(region, farm, text_plot )
					return ("Plot submitted: " + str(text_plot))
			else:
				return form.errors["text_plot"][0]
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
				text_block = request.form['text_block']
				if plotID in ['','None']:
					return jsonify({"submitted": "Please select a plot to register a new block"})
				elif Fields.find_block(plotID, text_block):
					return jsonify({"submitted": "Block already found: " + str(text_block)})
				else:
					Fields.add_block(plotID, text_block)
					return jsonify({"submitted" : "Plot submitted: " + str(text_block)})
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
				blockUID = request.form['block']
				#register trees, make a file and return filename etc.
				if blockUID == "":
					added_trees = Fields.add_trees(plotID, count)
				else:
					added_trees = Fields.add_trees(plotID, count, blockUID)
				return jsonify({'submitted' : str(count) + ' trees (TreeIDs: ' 
					+ str(added_trees['first_tree_id'])
					+ "-" + str(added_trees['last_tree_id']) + ') '
					+ 'added to plot (plotID: ' + str(plotID) + ')</a>'})
			else:
				errors = jsonify([location_form.errors, add_trees_form.errors])
				return errors
		except (ServiceUnavailable):
				flash("Database unavailable")
				return redirect(url_for('index'))


