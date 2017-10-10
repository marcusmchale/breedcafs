import os
from app import app
from flask import (redirect, 
	url_for, 
	request, 
	session, 
	render_template, 
	jsonify,
	send_file)
from app.models import (User, 
	Download)
from app.forms import (DownloadForm,
	OptionalLocationForm, 
	CreateTreeTraits, 
	CreateBlockTraits)
from app.emails import send_attachment
from datetime import datetime

@app.route('/download', methods=['GET', 'POST'])
def download():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		location_form = OptionalLocationForm().update()
		download_form = DownloadForm()
		tree_traits_form = CreateTreeTraits()
		block_traits_form = CreateBlockTraits()
		return render_template('download.html', 
			download_form = download_form,
			location_form = location_form,
			tree_traits_form = tree_traits_form,
			block_traits_form = block_traits_form,
			title='Download')

@app.route('/download/csv', methods=['GET', 'POST'])
def download_csv():
	username = session['username']
	download_form = DownloadForm()
	location_form = OptionalLocationForm().update()
	level = request.form['trait_level']
	if level == 'tree':
		traits_form = CreateTreeTraits()
	elif level == 'block':
		traits_form = CreateBlockTraits()
	if all([download_form.validate_on_submit(), traits_form.validate_on_submit(), location_form.validate_on_submit()]):
		country = request.form['country']
		region = request.form['region']
		farm = request.form['farm']
		plotID = request.form['plot']
		if plotID != "" :
			plotID = int(plotID)
		blockUID = request.form['block']
		#tree traits
		gen = request.form.getlist('general')
		agro = request.form.getlist('agronomic')
		morph = request.form.getlist('morphological')
		photo = request.form.getlist('photosynthetic')
		metab = request.form.getlist('metabolomic')
		#block traits
		b_gen = request.form.getlist('block_general')
		b_agro = request.form.getlist('block_agronomic')
		#concatenate lists of traits
		if level == 'tree':
			traits = gen + agro + morph + photo + metab 
		if level == 'block':
			traits = b_gen + b_agro
		#make the file
		data_csv = Download(username).get_csv(country, region, farm, plotID, blockUID, level, traits)
		recipients=[User(session['username']).find('')['email']]
		subject = "BreedCAFS: Data requested"
		body = "The requested data is attached"
		html = render_template('emails/data_file.html')
		send_attachment(subject, 
			app.config['ADMINS'][0],  
			recipients, 
			body, 
			html, 
			u'BreedCAFS_data_' + datetime.now().strftime('%Y%m%d') + '.csv', 
			'text/csv', 
			data_csv)
		#return send_file(data_csv,
		#	attachment_filename='BreedCAFS_data.csv',
		#	as_attachment=True,
		#	mimetype=('txt/csv'))
		return jsonify({"submitted":"did you get it?"})
	else:
		errors = jsonify([download_form.errors, traits_form.errors])
		return errors
