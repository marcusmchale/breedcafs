from app import app
from flask import (session, 
	flash, 
	request, 
	redirect, 
	url_for, 
	render_template, 
	send_file, 
	make_response, 
	jsonify)
from app.models import (Lists, 
	Fields, 
	FieldDetails, 
	User, 
	Samples)
from app.forms import (LocationForm, 
	AddCountry, 
	AddRegion, 
	AddFarm, 
	AddPlot, 
	AddBlock, 
	FieldsForm, 
	AddTreesForm, 
	CustomTreesForm, 
	SampleRegForm, 
	AddTissueForm, 
	AddStorageForm, 
	CreateTreeTraits, 
	CreateBlockTraits)
from app.emails import send_attachment
from flask.views import MethodView
from datetime import datetime

@app.route('/create', methods=['GET'])
def create():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		return render_template('create.html', title='Create')

#Locations
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

class blocks(MethodView):
	def get(self, plotID):
		blocks = Fields.get_blocks(plotID)
		response = make_response(jsonify(blocks))
		response.content_type = 'application/json'
		return response

@app.route('/add_country', methods=["POST"])
def add_country():
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

@app.route('/add_region', methods=["POST"])
def add_region():
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
			return ("Farm already found: " + str(text_farm))
		else:
			Fields(country).add_farm(region, text_farm)
			return ("Farm submitted: " + str(text_farm))
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
			return ("Plot already found: " + str(text_plot))
		else:
			Fields(country).add_plot(region, farm, text_plot )
			return ("Plot submitted: " + str(text_plot))
	else:
		return form.errors["text_plot"][0]

@app.route('/add_block', methods=["POST"])
def add_block():
	location_form = LocationForm().update()
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

@app.route('/generate_blocks_csv', methods=["POST"])
def generate_blocks_csv():
	location_form = LocationForm().update()
	fields_form = FieldsForm()
	if all([location_form.validate_on_submit(), fields_form.validate_on_submit()]):
		plotID = int(request.form['plot'])
		blocks_csv= Fields.get_blocks_csv(plotID)
		recipients=[User(session['username']).find('')['email']]
		subject = "BreedCAFS: Generate blocks.csv"
		body = "You requested a blocks.csv file for blocks in plotID: " + str(plotID)
		" These are described in the attached file that should be placed in the Field-Book field_import directory."
		html = render_template('emails/generate_blocks.html',
			plotID=plotID)
		send_attachment(subject, 
			app.config['ADMINS'][0], 
			recipients, 
			body, 
			html,
			u'BreedCAFS_plot_' + str(plotID) + '_blocks.csv',
			'text/csv', 
			blocks_csv)
		#return as jsonify so that can be interpreted the same way as error message
		return jsonify({"submitted" : "Blocks.csv sent to your email address"})
	else:
		errors = jsonify([location_form.errors, fields_form.errors])
		return errors

@app.route('/location_fields', methods=['GET', 'POST'])
def location_fields():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		location_form = LocationForm().update()
		add_country = AddCountry()
		add_region = AddRegion()
		add_farm = AddFarm()
		add_plot = AddPlot()
		add_block = AddBlock()
		fields_form = FieldsForm()
		return render_template('location_fields.html', 
			location_form = location_form,
			add_country = add_country, 
			add_region = add_region, 
			add_farm = add_farm, 
			add_plot = add_plot,
			add_block = add_block,
			fields_form = fields_form,
			title = 'Register fields and submit details')

#Trees
@app.route('/location_trees', methods=['GET', 'POST'])
def location_trees():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		location_form = LocationForm().update()
		add_trees = AddTreesForm()
		add_country = AddCountry()
		add_region = AddRegion()
		add_farm = AddFarm()
		add_plot = AddPlot()
		custom_trees_form = CustomTreesForm()
		return render_template('location_trees.html', 
			location_form = location_form,
			add_trees = add_trees, 
			custom_trees_form = custom_trees_form,
			title = 'Register trees and create trees.csv')

@app.route('/add_trees', methods=["POST"])
def add_trees():
	location_form = LocationForm().update()
	add_trees_form = AddTreesForm()
	if all([location_form.validate_on_submit(), add_trees_form.validate_on_submit()]):
		plotID = int(request.form['plot'])
		count = int(request.form['count'])
		blockUID = request.form['block']
		if blockUID == "":
			trees_csv=Fields.add_trees(plotID, count)
		else:
			trees_csv=Fields.add_trees(plotID, count, blockUID)
		recipients=[User(session['username']).find('')['email']]
		subject = "BreedCAFS: Trees registered"
		body = "You successfully registered " + str(count) + " trees in plotID: " + str(plotID) + "." 
		" These trees now have unique IDs (UID) and are described in the attached file." 
		" This file should be placed in the Field-Book field_import directory."
		html = render_template('emails/add_trees.html', 
			count=count,
			plotID=plotID)
		send_attachment(subject, 
			app.config['ADMINS'][0],  
			recipients, 
			body, 
			html, 
			u'BreedCAFS_plot_' + str(plotID) + '_trees_registered_' + datetime.now().strftime('%Y%m%d') + '.csv', 
			'text/csv', 
			trees_csv)
		#return as jsonify so that can be interpreted the same way as error message
		return jsonify({"submitted" : str(count) + " trees registered and trees.csv emailed to your registered address"})
	else:
		errors = jsonify([location_form.errors, add_trees_form.errors])
		return errors

@app.route('/custom_trees_csv', methods=["POST"])
def custom_trees_csv():
	location_form = LocationForm().update()
	custom_trees_form = CustomTreesForm()
	if all([location_form.validate_on_submit(), custom_trees_form.validate_on_submit()]):
		plotID = int(request.form['plot'])
		start = int(request.form['trees_start'])
		end = int(request.form['trees_end'])
		trees_csv=Fields.get_trees(plotID, start, end)
		recipients=[User(session['username']).find('')['email']]
		subject = "BreedCAFS: Custom trees.csv"
		body = "You requested a custom trees.csv file for trees " + str(start) + " to " + str(end) + " in PlotID: " + str(plotID)
		" These are described in the attached file. This file should be placed in the Field-Book field_import directory."
		html = render_template('emails/custom_trees.html',
			start=start,
			end=end,
			plotID=plotID)
		send_attachment(subject, 
			app.config['ADMINS'][0], 
			recipients, 
			body, 
			html, 
			u'BreedCAFS_' + str(plotID) + '_trees_' + str(start) + '-' + str(end) + '.csv', 
			'text/csv', 
			trees_csv)
		#return as jsonify so that can be interpreted the same way as error message
		return jsonify({"submitted" : "Trees.csv sent to your email address"})
	else:
		errors = jsonify([location_form.errors, custom_trees_form.errors])
		return errors

#traits
@app.route('/traits/<level>/', methods=['GET', 'POST'])
def select_traits(level):
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		tree_traits_form = CreateTreeTraits()
		block_traits_form = CreateBlockTraits()
		return render_template('traits.html', 
			level = level,
			tree_traits_form = tree_traits_form,
			block_traits_form = block_traits_form,
			title='Select traits for ' + level + '_traits.trt')

@app.route('/traits/<level>/create_trt', methods=['GET', 'POST'])
def create_trt(level):
	if level == 'tree':
		form = CreateTreeTraits()
		Level = 'Tree'
	elif level == 'block':
		form = CreateBlockTraits()
		Level = 'Block'
	if form.validate_on_submit():
		#tree traits
		gen = request.form.getlist('general')
		agro = request.form.getlist('agronomic')
		morph = request.form.getlist('morphological')
		photo = request.form.getlist('photosynthetic')
		metab = request.form.getlist('metabolomic')
		#block traits
		b_gen = request.form.getlist('block_general')
		b_agro = request.form.getlist('block_agronomic')
		selection = gen + agro + morph + photo + metab + b_agro + b_gen
		trt=Lists(Level + 'Trait').create_trt(selection, 'name')
		recipients=[User(session['username']).find('')['email']]
		subject = "BreedCAFS: traits.trt"
		body = "You generated a " + level + "_traits.trt file in the BreedCAFS database. The file is attached to this email. "
		html = render_template('emails/create_traits.html')
		send_attachment(subject, 
			app.config['ADMINS'][0], 
			recipients, 
			body, 
			html, 
			u'BreedCAFS_' + level +'_traits_' + datetime.now().strftime('%Y%m%d') + '.trt', 
			'text/csv', 
			trt)
		return jsonify({"submitted" : "Custom traits.trt sent to your email address"})
	else:
		return jsonify(form.errors)

#Samples
@app.route('/sample_reg', methods =['GET','POST'])
def sample_reg():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		add_tissue_form = AddTissueForm()
		add_storage_form = AddStorageForm()
		sample_reg_form = SampleRegForm().update()
		return render_template('sample_reg.html', 
			add_tissue_form = add_tissue_form,
			add_storage_form = add_storage_form,
			sample_reg_form = sample_reg_form,
			title = 'Sample registration')

class tissues(MethodView):
	def get(self):
		tissues = Lists('Tissue').create_list('name','name')
		response = make_response(jsonify(tissues))
		response.content_type = 'application/json'
		return response

@app.route('/add_tissue', methods=["POST"])
def add_tissue():
	form = AddTissueForm()
	text_tissue = request.form['text_tissue']
	if form.validate_on_submit():
		if Lists('Tissue').find_node(text_tissue):
			return ("Tissue already found: " + str(text_tissue))
		else:
			Samples().add_tissue(text_tissue)
			return ("Tissue submitted: " + str(text_tissue))
	else:
		return form.errors["text_tissue"][0]

class storage_methods(MethodView):
	def get(self):
		storage_methods = Lists('Storage').create_list('name','name')
		response = make_response(jsonify(storage_methods))
		response.content_type = 'application/json'
		return response

@app.route('/add_storage', methods=["POST"])
def add_storage():
	form = AddStorageForm()
	text_storage = request.form['text_storage']
	if form.validate_on_submit():
		if Lists('Storage').find_node(text_storage):
			return ("Storage already found: " + str(text_storage))
		else:
			Samples().add_storage(text_storage)
			return ("Storage submitted: " + str(text_storage))
	else:
		return form.errors["text_storage"][0]

@app.route('/add_samples', methods = ['GET','POST'])
def add_samples():
	form=SampleRegForm().update()
	if form.validate_on_submit():
		plotID = int(request.form['plot'])
		start = int(request.form['trees_start'])
		end = int(request.form['trees_end'])
		replicates = int(request.form['replicates'])
		tissue = request.form['tissue']
		storage = request.form['storage']
		date = request.form['date_collected']
		samples_csv = Samples().add_samples(plotID, start, end, replicates, tissue, storage, date)
		recipients=[User(session['username']).find('')['email']]
		subject = "BreedCAFS: Samples registered"
		body = "You sucessfully registered " + str(replicates) + " replicates of " + str(tissue)
		" samples stored in " + str(storage) + " on " + str(date) + " for trees from " + str(start) + " to " + str(end) + " in plotID: " + str(plotID)  + "." 
		" These samples now have unique IDs (UID) and are described in the attached file." 
		" Please use these ID's to label samples, track their movement and register data from analyses."
		html = render_template('emails/add_samples.html', 
			plotID=plotID,
			start=start,
			end=end,
			replicates=replicates,
			tissue=tissue,
			storage=storage,
			date=date)
		send_attachment(subject,
			app.config['ADMINS'][0],
			recipients,
			body,
			html,
			u'BreedCAFS_samples_' + datetime.now().strftime('%Y%m%d') + '.csv',
			'text/csv',
			samples_csv)
		return jsonify({"submitted" : str(tissue) + " samples submitted and samples.csv sent to your registered email address"})
	else:
		errors = jsonify(form.errors)
		return errors
