from app import app
from flask import session, flash, request, redirect, url_for, render_template, send_file, make_response, jsonify
from app.models import Lists, Fields, User
from app.forms import CreateTraits, RegisterTrees, AddCountry, AddRegion, AddFarm, AddPlot
from app.emails import send_attachment
from flask.views import MethodView

@app.route('/create', methods=['GET'])
def create():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		return render_template('create.html', title='Create')

class countries(MethodView):
	def get(self):
		countries = Lists('Country').create_list('name','name')
		response = make_response(jsonify(countries))
		response.content_type = 'application/json'
		return response

class regions(MethodView):
	def get(self, country):
		regions = Lists('Country').get_connected('name', country, 'IS_IN')
		response = make_response(jsonify(regions))
		response.content_type = 'application/json'
		return response

class farms(MethodView):
	def get(self, country, region):
		farms = Fields(country).get_farms(region)
		response = make_response(jsonify(farms))
		response.content_type = 'application/json'
		return response

class plots(MethodView):
	def get(self, country, region, farm):
		plots = Fields(country).get_plots(region, farm)
		response = make_response(jsonify(plots))
		response.content_type = 'application/json'
		return response

@app.route('/add_country', methods=["POST"])
def add_country():
	form = AddCountry()
	text_country = request.form['text_country']
	if form.validate_on_submit():
		if Fields(text_country).find_country():
			return ("Country already found: " + text_country)
		else:
			Fields(text_country).add_country()
			return ("Country submitted: " + text_country)
	else:
		return form.errors["text_country"][0]

@app.route('/add_region', methods=["POST"])
def add_region():
	form = AddRegion()
	country = request.form['country']
	text_region = request.form['text_region']
	if form.validate_on_submit():
		if country in ['','None']:
			return ('Please select a country to register a new region')
		elif Fields(country).find_region(text_region):
			return ("Region already found: " + text_region)
		else:
			Fields(country).add_region(text_region)
			return ("Region submitted: " + text_region )
	else:
		return form.errors["text_region"][0]

@app.route('/add_farm', methods=["POST"])
def add_farm():
	form = AddFarm()
	country = request.form['country']
	region = request.form['region']
	text_farm = request.form['text_farm']
	if form.validate_on_submit():
		if bool(set([country,region]) & set(['','None'])):
			return ('Please select a country and region to register a new farm')
		elif Fields(country).find_farm(region, text_farm):
			return ("Farm already found: " + text_farm )
		else:
			Fields(country).add_farm(region, text_farm)
			return ("Farm submitted: " + text_farm )
	else:
		return form.errors["text_farm"][0]

@app.route('/add_plot', methods=["POST"])
def add_plot():
	form = AddPlot()
	country = request.form['country']
	region = request.form['region']
	farm = request.form['farm']
	text_plot = request.form['text_plot']
	if form.validate_on_submit():
		if bool(set([country,region,farm]) & set(['','None'])):
			return ('Please select a country, region and farm to register a new plot')
		elif Fields(country).find_plot(region, farm, text_plot):
			return ("Plot already found: " + text_plot )
		else:
			Fields(country).add_plot(region, farm, text_plot )
			return ("Farm submitted: " + text_plot )
	else:
		return form.errors["text_plot"][0]

@app.route('/add_trees', methods=["POST"])
def add_trees():
	form = RegisterTrees().update()
	country = request.form['country']
	region = request.form['region']
	farm = request.form['farm']
	plot = request.form['plot']
	count = request.form['count']
	if form.validate_on_submit():
		fields_csv=Fields(country).add_trees(region, farm, plot, count)
		recipients=[User(session['username']).find('')['email']]
		subject = "BreedCAFS: Trees registered"
		html = render_template('emails/register_trees.html', 
			count=count,
			plot=plot,
			farm=farm,
			region=region,
			country=country)
		send_attachment(subject, 
			app.config['ADMINS'][0], 
			recipients, 
			'copy of fields.csv', 
			html, 
			u'BreedCAFS_fields.csv', 
			'text/csv', 
			fields_csv)
		#return as jsonify so that can be interpreted the same way as error message
		return jsonify({"submitted" : str(count + " trees registered")})
	else:
		return jsonify(form.errors)

@app.route('/locations_trees', methods=['GET', 'POST'])
def register_trees():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		register_trees = RegisterTrees().update()
		add_country = AddCountry()
		add_region = AddRegion()
		add_farm = AddFarm()
		add_plot = AddPlot()
		return render_template('locations_trees.html', 
			register_trees=register_trees, 
			add_country=add_country, 
			add_region=add_region, 
			add_farm=add_farm, 
			add_plot=add_plot, 
			title='Register trees and create fields.csv')

@app.route('/traits', methods=['GET', 'POST'])
def create_trt():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		form = CreateTraits()
		if form.validate_on_submit():
			selection = form.traits.data
			trt=Lists('Trait').create_trt(selection, 'name')
			recipients=[User(session['username']).find('')['email']]
			subject = "BreedCAFS: traits.trt"
			html = render_template('emails/create_traits.html')
			send_attachment(subject, 
				app.config['ADMINS'][0], 
				recipients, 
				'copy of traits.trt', 
				html, 
				u'BreedCAFS_traits.trt', 
				'text/csv', 
				trt)
			return send_file(trt,
				attachment_filename='BreedCAFS_traits.trt', 
				as_attachment=True,
				mimetype=('txt/csv'))
			flash('Generated traits.trt')
	return render_template('traits.html', form=form, title='Create traits.trt')
