from app import app, ServiceUnavailable, SecurityError
from flask import (
	session,
	flash, 
	request, 
	redirect, 
	url_for, 
	render_template,
	jsonify,
)
from app.models import (
	AddLocations,
	FindLocations,
	FindFieldItems,
	AddFieldItems,
	Download,
	User
)
from app.forms import (
	LocationForm,
	AddCountry, 
	AddRegion, 
	AddFarm, 
	AddField,
	AddBlock,
	AddTreesForm
)

from app.emails import send_email


@app.route('/create', methods=['GET', 'POST'])
def create():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			location_form=LocationForm.update()
			add_country_form = AddCountry()
			add_region_form = AddRegion()
			add_farm_form = AddFarm()
			add_field_form = AddField()
			add_block_form = AddBlock()
			add_trees_form = AddTreesForm()
			return render_template(
				'create.html',
				location_form=location_form,
				add_country_form=add_country_form,
				add_region_form=add_region_form,
				add_farm_form=add_farm_form,
				add_field_form=add_field_form,
				add_block_form=add_block_form,
				add_trees_form=add_trees_form,
				title='Register fields and submit details'
			)
		except (ServiceUnavailable, SecurityError):
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
				found = FindLocations(text_country).find_country()
				if found:
					return jsonify({"found": found[0]['name']})
				else:
					result = AddLocations(session['username'], text_country).add_country()
					return jsonify({"submitted": result[0]})
			else:
				return jsonify([form.errors])
		except (ServiceUnavailable, SecurityError):
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
			text_region = request.form['text_region'].strip()
			if form.validate_on_submit():
				if country in ['', 'None']:
					return jsonify([{"country": ["Please select a country to add a new region"]}])
				else:
					found = FindLocations(country).find_region(text_region)
					if found:
						return jsonify({"found": found[0]['name']})
					else:
						result = AddLocations(session['username'], country).add_region(text_region)
						return jsonify({"submitted": result[0]})
			else:
				return jsonify([form.errors])
		except (ServiceUnavailable, SecurityError):
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
					found = FindLocations(country).find_farm(region, text_farm)
					if found:
						return jsonify({"found": found[0]['name']})
					else:
						result = AddLocations(session['username'], country).add_farm(region, text_farm)
						return jsonify({"submitted": result[0]})
			else:
				return jsonify([form.errors])
		except (ServiceUnavailable, SecurityError):
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
					found = FindLocations(country).find_field(region, farm, text_field)
					if found:
						return jsonify({"found": {'uid': found[0]['uid'], 'name': found[0]['name']}})
					else:
						result = AddLocations(session['username'], country).add_field(region, farm, text_field)
						return jsonify({"submitted": {'uid': result[0]['uid'], 'name': result[0]['name']}})
			else:
				return jsonify([form.errors])
		except (ServiceUnavailable, SecurityError):
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
					found = FindFieldItems(field_uid).find_block(text_block)
					if found:
						return jsonify({"found": {'uid': found[0]['uid'], 'name': found[0]['name']}})
					else:
						result = AddFieldItems(session['username'], field_uid).add_block(text_block)
						return jsonify({"submitted": {'uid': result[0]['uid'], 'name': result[0]['name']}})
			else:
				errors = jsonify([location_form.errors, add_block_form.errors])
				return errors
		except (ServiceUnavailable, SecurityError):
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
				request_email = True if request.form.get('email_checkbox') else False
				download_object = Download(session['username'], request_email)
				if download_object.register_trees(
						field_uid,
						block_uid,
						count
				):
					download_object.item_level = "tree"
					download_object.set_features(
						'tree',
						'property',
						feature_group="Registration"
					)
					download_object.id_list_to_xlsx_template(
						base_filename="Tree Registration Template",
					)
					file_list = download_object.file_list
					file_list_html = download_object.get_file_list_html()
					if request.form.get('email_checkbox'):
						recipients = [User(session['username']).find('')['email']]
						subject = "BreedCAFS files requested"
						body = (
								" You recently registered trees in the BreedCAFS database. "
								" Unique identifiers (UIDs) have been generated for these trees"
								" for future reference. "
								" A spreadsheet file (.xlsx) has been generated containing these UIDs"
								" and other relevant information about the parent field/block."
								" This same file contains a 'Template' sheet that can be completed"
								" and uploaded to this site to record specific details for these trees. "
								" The file is available at the following address: "
								+ file_list_html
						)
						html = render_template(
							'emails/generate_files.html',
							file_list=[i['url'] for i in file_list]
						)
						send_email(
							subject,
							app.config['ADMINS'][0],
							recipients,
							body,
							html
						)
						return jsonify(
							{
								'submitted': (
										' Your trees are registered and a submission template is available'
										' for download.'
										' A link to this file has been sent to your email address:'
										+ file_list_html
								)
							}
						)
					return jsonify(
						{
							'submitted': (
									' Your trees are registered and a submission template is available'
									' for download.'
									+ file_list_html
							)
						}
					)
				else:
					return jsonify({
						'submitted': 'No field/block found that matches your selection, please try again'
					})
			else:
				return jsonify({
					'errors': [add_trees_form.errors, location_form.errors]
				})
		except (ServiceUnavailable, SecurityError):
			flash("Database unavailable")
			return redirect(url_for('index'))


