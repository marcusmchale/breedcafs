from app import app
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
	AddBlock, 
	FieldsForm, 
	AddTreesForm, 
	CustomTreesForm, 
	CustomSampleForm,
	SampleRegForm, 
	AddTissueForm, 
	AddStorageForm, 
	CreateTreeTraits, 
	CreateBlockTraits)
from app.emails import send_attachment, send_static_attachment, send_email
from flask.views import MethodView
from datetime import datetime

@app.route('/create', methods=['GET'])
def create():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		return render_template('create.html', title='Create')

#Locations as tuples for forms
class countries(MethodView):
	def get(self):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			countries = Lists('Country').create_list_tup('name','name')
			response = make_response(jsonify(countries))
			response.content_type = 'application/json'
			return response

class regions(MethodView):
	def get(self, country):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			REGIONS = Lists('Country').get_connected('name', country, 'IS_IN')
			response = make_response(jsonify([(REGIONS[i],REGIONS[i]) for i, items in enumerate(REGIONS)]))
			response.content_type = 'application/json'
			return response

class farms(MethodView):
	def get(self, country, region):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			FARMS = Fields(country).get_farms(region)
			response = make_response(jsonify([(FARMS[i],FARMS[i]) for i, items in enumerate(FARMS)]))
			response.content_type = 'application/json'
			return response

class plots(MethodView):
	def get(self, country, region, farm):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			plots = Fields(country).get_plots_tup(region, farm)
			response = make_response(jsonify(plots))
			response.content_type = 'application/json'
			return response

class blocks(MethodView):
	def get(self, plotID):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			blocks = Fields.get_blocks_tup(plotID)
			response = make_response(jsonify(blocks))
			response.content_type = 'application/json'
			return response

@app.route('/add_country', methods=["POST"])
def add_country():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
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
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
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
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
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
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
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
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
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
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		location_form = LocationForm().update()
		fields_form = FieldsForm()
		if all([location_form.validate_on_submit(), fields_form.validate_on_submit()]):
			plotID = int(request.form['plot'])
			#make the file and return a dictionary with filename, file_path and file_size
			file_details = Fields.make_blocks_csv(session['username'], plotID)
			#create a download url for the file
			download_url = url_for('download_file', 
				username = session['username'], 
				filename=file_details['filename'], 
				_external = True)
			#if selected send an email copy of the file (or link to download if greater than ~5mb)
			if request.form.get('email_checkbox'):
				recipients=[User(session['username']).find('')['email']]
				subject = "BreedCAFS: blocks.csv"
				body = ("You requested a blocks.csv file for blocks in plotID: " + str(plotID) +
					+ " These are described in the attached file (if less than 5mb) that is also available for download at the following address: "
					+ download_url )
				html = render_template('emails/generate_blocks.html',
					plotID=plotID,
					download_url = download_url)
				if file_details['file_size'] < 5000000:
					send_static_attachment(subject, 
						app.config['ADMINS'][0], 
						recipients, 
						body, 
						html,
						file_details['filename'],
						'text/csv',
						file_details['file_path'])
					return jsonify({'submitted' : 'Your file is ready for download and a copy has been sent to your email as an attachment:'
						+ '"<a href="' + download_url + '">' + file_details['filename'] + '</a>"'})
				else:
					send_email(subject,
						app.config['ADMINS'][0],
						recipients,
						body,
						html)
					return jsonify({'submitted' : 'Your file is ready for download and a link has been sent to your email address:'
						+ '"<a href="' + download_url + '">' + file_details['filename'] + '</a>"'})
			#return as jsonify so that can be interpreted the same way as error message
			else:
				return jsonify({'submitted' : 'Your file is ready for download: "<a href="' + download_url + '">' + file_details['filename'] + '</a>"'})
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
		add_trees_form = AddTreesForm()
		add_country = AddCountry()
		add_region = AddRegion()
		add_farm = AddFarm()
		add_plot = AddPlot()
		custom_trees_form = CustomTreesForm()
		return render_template('location_trees.html', 
			location_form = location_form,
			add_trees_form = add_trees_form, 
			custom_trees_form = custom_trees_form,
			title = 'Register trees and create trees.csv')

@app.route('/add_trees', methods=["POST"])
def add_trees():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		location_form = LocationForm().update()
		add_trees_form = AddTreesForm()
		if all([location_form.validate_on_submit(), add_trees_form.validate_on_submit()]):
			plotID = int(request.form['plot'])
			count = int(request.form['count'])
			blockUID = request.form['block']
			#register trees, make a file and return filename etc.
			if blockUID == "":
				file_details = Fields.add_trees(plotID, count)
			else:
				file_details = Fields.add_trees(plotID, count, blockUID)
			#create a download url
			download_url = url_for('download_file', 
				username = session['username'], 
				filename = file_details['filename'], 
				_external = True)
			if request.form.get('email_checkbox_add'):
				recipients=[User(session['username']).find('')['email']]
				subject = "BreedCAFS: Trees registered"
				body = ( "You successfully registered " + str(count) + " trees in plotID: " + str(plotID) + "."
					+ " These trees were assigned IDs from " + str(file_details['first_tree_id']) + " to " + str(file_details['last_tree_id'])
					+ " and are described in a file (attached if less than 5mb) and available for download at the following address: " + download_url )
				html = render_template('emails/add_trees.html',
					count=count,
					plotID=plotID,
					download_url = download_url)
				if file_details['file_size'] < 5000000:
					send_static_attachment(subject, 
						app.config['ADMINS'][0], 
						recipients, 
						body, 
						html,
						file_details['filename'],
						'text/csv',
						file_details['file_path'])
					return jsonify({'submitted' : ('You successfully registered ' + str(count) + ' trees in plotID: ' + str(plotID) + '.'
						+ ' These trees were assigned IDs from ' + str(file_details['first_tree_id']) + " to " + str(file_details['last_tree_id'])
						+ ' and are described in this file:'
						+ '"<a href="' + download_url + '">' + file_details['filename'] + '</a>'
						+ ' This file has also been sent to your email address.')})
				else:
					send_email(subject,
						app.config['ADMINS'][0],
						recipients,
						body,
						html)
					return jsonify({'submitted' : ('You successfully registered ' + str(count) + ' trees in plotID: ' + str(plotID) + '.'
						+ ' These trees were assigned IDs from ' + str(file_details['first_tree_id']) + " to " + str(file_details['last_tree_id'])
						+ ' and are described in this file:'
						+ '"<a href="' + download_url + '">' + file_details['filename'] + '</a>'
						+ ' This link has also been sent to your email address.')})
			else:
				return jsonify({'submitted' : 'You successfully registered ' + str(count) + ' trees in plotID ' + str(plotID)
					+ ' These trees were assigned IDs from ' + str(file_details['first_tree_id']) + " to " + str(file_details['last_tree_id'])
					+ ' and are described in this file: '
					+ '<a href="' + download_url + '">' + file_details['filename'] + '</a>'})
		else:
			errors = jsonify([location_form.errors, add_trees_form.errors])
			return errors

@app.route('/custom_trees_csv', methods=["POST"])
def custom_trees_csv():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		location_form = LocationForm().update()
		custom_trees_form = CustomTreesForm()
		if all([location_form.validate_on_submit(), custom_trees_form.validate_on_submit()]):
			plotID = int(request.form['plot'])
			start = int(request.form['trees_start'])
			end = int(request.form['trees_end'])
			#make the file and return filename, path, size
			file_details = Fields.get_trees(plotID, start, end)
			#create a download url
			download_url = url_for('download_file', 
				username = session['username'], 
				filename = file_details['filename'], 
				_external = True)
			#send email if requested, as link if >5mb
			if request.form.get('email_checkbox_add'):
				recipients = [User(session['username']).find('')['email']]
				subject = "BreedCAFS: Custom trees.csv"
				body = ("You requested a custom trees.csv file for trees " + str(start) + " to " + str(end) + " in PlotID: " + str(plotID)
					+ " This file is attached (if less than 5mb) and available for download at the following address: " + download_url )
				html = render_template('emails/custom_trees.html',
					start=start,
					end=end,
					plotID=plotID)
				if file_details['file_size'] < 5000000:
					send_static_attachment(subject, 
						app.config['ADMINS'][0], 
						recipients, 
						body, 
						html, 
						file_details['filename'], 
						'text/csv',
						file_details['file_path'])
					return jsonify({'submitted' : ( 'Your file is ready for download: '
						+ '"<a href="' + download_url + '">' + file_details['filename'] + '</a>'
						+ ' This file has also been sent to your email address.')})
				else:
					send_email(subject, 
						app.config['ADMINS'][0], 
						recipients, 
						body, 
						html)
					return jsonify({'submitted' : ('Your file is ready for downoad: '
						+ '"<a href="' + download_url + '">' + file_details['filename'] + '</a>'
						+ ' This link has also been sent to your email address.')})
			else:
				return jsonify({'submitted' : ('Your file is ready for download : '
					+ '<a href="' + download_url + '">' + file_details['filename'] + '</a>')})
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
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
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
			#make a list of selected traits
			selection = gen + agro + morph + photo + metab + b_agro + b_gen
			#make the trt file and return it's details (filename, file_path, file_size)
			file_details = Lists(Level + 'Trait').create_trt(session['username'], selection, 'name', level)
			download_url = url_for('download_file', 
				username = session['username'], 
				filename = file_details['filename'], 
				_external = True)
			#send email if requested and include as attachment if less than ~5mb
			if request.form.get('email_checkbox'):
				recipients=[User(session['username']).find('')['email']]
				subject = "BreedCAFS: traits.trt"
				body = ("You requested a " + level + ".trt file from the BreedCAFS database. "
					" The file is attached (if less than 5mb) and available for download at the following address:"
					+ download_url)
				html = render_template('emails/create_traits.html', 
					level = level,
					download_url = download_url)
				if file_details['file_size'] < 5000000:
					send_static_attachment(subject, 
						app.config['ADMINS'][0], 
						recipients, 
						body, 
						html, 
						file_details['filename'], 
						'text/csv', 
						file_details['file_path'])
					return jsonify({'submitted' : 'Your file is ready for download and a copy has been sent to your email as an attachment:'
						+ '"<a href="' + download_url + '">' + file_details['filename'] + '</a>"'})
				else:
					send_email(subject,
						app.config['ADMINS'][0],
						recipients,
						body,
						html)
					return jsonify({'submitted' : 'Your file is ready for download and a link has been sent to your email address:'
						+ '"<a href="' + download_url + '">' + file_details['filename'] + '</a>"'})
			#return as jsonify so that can be interpreted the same way as error message
			else:
				return jsonify({'submitted' : 'Your file is ready for download: "<a href="' + download_url + '">' + file_details['filename'] + '</a>"'})
		else:
			return jsonify(form.errors)

#Samples
@app.route('/sample_reg', methods =['GET','POST'])
def sample_reg():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		location_form = LocationForm().update()
		add_tissue_form = AddTissueForm()
		add_storage_form = AddStorageForm()
		sample_reg_form = SampleRegForm().update()
		custom_sample_form = CustomSampleForm()
		return render_template('sample_reg.html', 
			location_form = location_form,
			add_tissue_form = add_tissue_form,
			add_storage_form = add_storage_form,
			sample_reg_form = sample_reg_form,
			custom_sample_form = custom_sample_form,
			title = 'Sample registration')

class tissues(MethodView):
	def get(self):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			tissues = Lists('Tissue').create_list_tup('name','name')
			response = make_response(jsonify(tissues))
			response.content_type = 'application/json'
			return response

@app.route('/add_tissue', methods=["POST"])
def add_tissue():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
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
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			storage_methods = Lists('Storage').create_list_tup('name','name')
			response = make_response(jsonify(storage_methods))
			response.content_type = 'application/json'
			return response

@app.route('/add_storage', methods=["POST"])
def add_storage():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
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
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		location_form = LocationForm().update()
		sample_form=SampleRegForm().update()
		if all([location_form.validate_on_submit(), sample_form.validate_on_submit()]):
			plotID = int(request.form['plot'])
			start = int(request.form['trees_start'])
			end = int(request.form['trees_end'])
			replicates = int(request.form['replicates'])
			tissue = request.form['tissue']
			storage = request.form['storage']
			date = request.form['date_collected']
			#register samples, make file describing index information and return filename etc.
			file_details = Samples().add_samples(plotID, start, end, replicates, tissue, storage, date)
			#create a download url
			download_url = url_for('download_file', 
				username = session['username'], 
				filename = file_details['filename'], 
				_external = True)
			#if requested create email (and send as attachment if less than ~5mb)
			if request.form.get('email_checkbox'):
				recipients=[User(session['username']).find('')['email']]
				subject = "BreedCAFS: Samples registered"
				body = ("You registered " + str(replicates) + " replicates of " + str(tissue)
					+ " samples stored in " + str(storage) + " on " + str(date) + " for trees from " + str(start) + " to " + str(end) + " in plotID: " + str(plotID)  + "." 
					+ " These samples are described in a file available for download at the following address: " + download_url)
				html = render_template('emails/add_samples.html', 
					plotID = plotID,
					start = start,
					end = end,
					replicates = replicates,
					tissue = tissue,
					storage = storage,
					date = date,
					download_url = download_url)
				if file_details['file_size'] < 5000000:
					send_static_attachment(subject,
						app.config['ADMINS'][0],
						recipients,
						body,
						html,
						file_details['filename'],
						'text/csv',
						file_details['file_path'])
					return jsonify({'submitted' : ('You successfully registered ' + str(tissue) + ' samples. '
						+ 'These are described in the following file: '
						+ '<a href="' + download_url + '">' + file_details['filename'] + '</a>.'
						+ ' This file has also been sent to your email address ')})
				else:
					send_static_attachment(subject,
						app.config['ADMINS'][0],
						recipients,
						body,
						html,
						file_details['filename'],
						'text/csv',
						file_details['file_path'])
					return jsonify({'submitted' : ('You successfully registered ' + str(tissue) + ' samples. '
						+ 'These are described in the following file : ' 
						+ '<a href="' + download_url + '">' + file_details['filename'] + '</a>.'
						+ ' This link has also been sent to your email address. ')})
			else:
				return jsonify({'submitted' : 'You successfully registered ' + str(tissue) + ' samples. '
					+ 'These are described in the following file : ' 
					+ '<a href="' + download_url + '">' + file_details['filename'] + '</a>'})
		else:
			errors = jsonify([location_form.errors, sample_form.errors])
			return errors


@app.route('/get_samples', methods = ['GET','POST'])
def get_samples():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		location_form = LocationForm().update(optional = True)
		sample_form = SampleRegForm().update(optional = True)
		custom_sample_form=CustomSampleForm()
		if all([location_form.validate_on_submit(), sample_form.validate_on_submit(), custom_sample_form.validate_on_submit()]):
			#get form values to filter by
			country = request.form['country']
			region = request.form['region']
			farm = request.form['farm']
			plotID = int(request.form['plot']) if request.form['plot'] else ""
			trees_start = int(request.form['trees_start']) if request.form['trees_start'] else ""
			trees_end = int(request.form['trees_end']) if request.form['trees_end'] else ""
			replicates = request.form['replicates']
			tissue = request.form['tissue']
			storage = request.form['storage']
			date_from = request.form['date_from']
			date_to = request.form['date_to']
			samples_start = int(request.form['samples_start']) if request.form['samples_start'] else ""
			samples_end = int(request.form['samples_end']) if request.form['samples_end'] else ""
			if date_from: 
				start_time = int((datetime.strptime(date_from, '%Y-%m-%d')-datetime(1970,1,1)).total_seconds()*1000)
			else:
				start_time = ""
			if date_to:
				end_time = int((datetime.strptime(date_to, '%Y-%m-%d')-datetime(1970,1,1)).total_seconds()*1000)
			else: end_time = ""
			#build the file and return filename etc.
			file_details = Samples().get_samples(country, 
				region, 
				farm,
				plotID, 
				trees_start, 
				trees_end,
				replicates, 
				tissue, 
				storage, 
				start_time,
				end_time,
				samples_start,
				samples_end)
			#create a download url
			download_url = url_for('download_file', 
				username = session['username'], 
				filename = file_details['filename'], 
				_external = True)
			#if requested create email (and send as attachment if less than ~5mb)
			if request.form.get('email_checkbox_custom'):
				recipients=[User(session['username']).find('')['email']]
				subject = "BreedCAFS: Samples registered"
				body = ("You requested a custom list of samples. This file is attached (if less than 5mb) and " 
					+ " available for download at the following address: " + download_url)
				html = render_template('emails/custom_samples.html', 
					download_url = download_url)
				if file_details['file_size'] < 5000000:
					send_static_attachment(subject,
						app.config['ADMINS'][0],
						recipients,
						body,
						html,
						file_details['filename'],
						'text/csv',
						file_details['file_path'])
					return jsonify({'submitted' : ('Your custom list of samples is ready for download: '
						+ '<a href="' + download_url + '">' + file_details['filename']+ '</a>.'
						+ ' This file has also been sent to your email address. ')})
				else:
					send_static_attachment(subject,
						app.config['ADMINS'][0],
						recipients,
						body,
						html,
						file_details['filename'],
						'text/csv',
						file_details['file_path'])
					return jsonify({'submitted' : ('Your custom list of samples is ready for download: '
						+ '<a href="' + download_url + '">' + file_details['filename']+ '</a>.'
						+ ' This link has also been sent to your email address. ')})
			else:
				return jsonify({'submitted' : ('Your custom list of samples is ready for download: '
					+ '<a href="' + download_url + '">' + file_details['filename'] + '</a>')})
