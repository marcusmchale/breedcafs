import os
from app import app
from flask import (redirect, 
	flash,
	url_for, 
	request, 
	session, 
	render_template, 
	jsonify,
	send_from_directory)
from app.models import (User, 
	Download)
from app.forms import (DownloadForm,
	OptionalLocationForm, 
	CreateTreeTraits, 
	CreateBlockTraits)
from app.emails import send_email, send_static_attachment
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


@app.route('/download/generate_csv', methods=['POST'])
def generate_csv():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		username = session['username']
		download_form = DownloadForm()
		location_form = OptionalLocationForm().update()
		level = request.form['trait_level']
		start_date = request.form['date_from']
		end_date = request.form['date_to']
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
			#get selected data format
			data_format = request.form['data_format']
			#convert the date to epoch time (ms)
			if start_date != "":
				start_time = int((datetime.strptime(start_date, '%Y-%m-%d')-datetime(1970,1,1)).total_seconds()*1000)
			else:
				start_time = ""
			if end_date != "":
				end_time = int((datetime.strptime(end_date, '%Y-%m-%d')-datetime(1970,1,1)).total_seconds()*1000)
			else:
				end_time = ""
			#make the file and return file details
			file_details = Download(username).get_csv(country, region, farm, plotID, blockUID, level, traits, data_format, start_time, end_time)
			#create a download url
			download_url = url_for('download_file', 
				username = session['username'], 
				filename=file_details['filename'],
				_external = True)
			if request.form.get('email_checkbox'):
				recipients=[User(session['username']).find('')['email']]
				subject = "BreedCAFS: Data file generated"
				if file_details['file_size'] < 5000000:
					body = ('The attached file contains data you requested from the BreedCAFS database.'
						+ 'This file is also available at the following address: ' + download_url )
					html = render_template('emails/data_file_attachment.html', 
						download_url = download_url)
					send_static_attachment(subject, 
						app.config['ADMINS'][0],
						recipients, 
						body, 
						html,
						file_details['filename'],
						'text/csv',
						file_details['file_path'])
					return jsonify({'submitted':'Your file is ready for download: "<a href="' + download_url + '">' + file_details['filename'] + '</a>"'
						+ ' A copy of this file has been sent to your email address'})
				else:
					body = ('The data you requested from the BreedCAFS database is available at the following address: ' + download_url )
					html = render_template('emails/data_file_link.html', 
						download_url = download_url)
					send_email(subject,
						app.config['ADMINS'][0],
						recipients, 
						body, 
						html)
					return jsonify({'submitted':'Your file is ready for download: "<a href="' + download_url + '">' + file_details['filename'] + '</a>"'
						+ ' A copy of this link has been sent to your email address'})
			else:
				return jsonify({'submitted':'Your file is ready for download: "<a href="' + download_url + '">' + file_details['filename'] + '</a>"'})
		else:
			errors = jsonify([download_form.errors, traits_form.errors])
			return errors

@app.route('/download/<username>/<filename>', methods=['GET', 'POST'])
def download_file(username, filename):
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	elif username != session['username']:
		flash('Files are currently only accessible to the user that generated them')
		return redirect(url_for('index'))
	else:
		if os.path.isfile(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], username, filename)):
			return send_from_directory(os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], username), filename, as_attachment = True)
		else:
			flash('File no longer exists on the server, please generate a new file for download')
			return redirect(url_for('download'))