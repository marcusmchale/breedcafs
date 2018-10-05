from app import (
	app,
	ServiceUnavailable,
	AuthError
)
from flask import (
	session,
	flash,
	redirect,
	url_for,
	render_template,
	make_response,
	jsonify,
	request
)
from app.models import (
	MatchNode,
	SelectionList,
	User,
	Download,
	Samples
)
from app.forms import (
	LocationForm,
	RecordForm,
	CreateTraits,
	SampleRegForm,
	AddTissueForm,
	AddStorageForm,
)
from app.emails import send_email
from datetime import datetime

from flask.views import MethodView


@app.route('/collect', methods=['GET', 'POST'])
def collect():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			record_form = RecordForm()
			location_form = LocationForm.update()
			field_traits_form = CreateTraits().update('field')
			block_traits_form = CreateTraits().update('block')
			tree_traits_form = CreateTraits().update('tree')
			branch_traits_form = CreateTraits().update('branch')
			leaf_traits_form = CreateTraits().update('leaf')
			sample_traits_form = CreateTraits().update('sample')
			sample_reg_form = SampleRegForm.update()
			return render_template(
				'collect.html',
				location_form=location_form,
				record_form=record_form,
				level='all',
				field_traits_form=field_traits_form,
				block_traits_form=block_traits_form,
				tree_traits_form=tree_traits_form,
				branch_traits_form=branch_traits_form,
				leaf_traits_form=leaf_traits_form,
				sample_traits_form=sample_traits_form,
				sample_reg_form=sample_reg_form,
				title='Record'
			)
		except (ServiceUnavailable, AuthError):
			flash("Database unavailable")
			return redirect(url_for('index'))


@app.route('/collect/generate_files', methods=['GET', 'POST'])
def generate_files():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			record_form = RecordForm.update()
			if record_form.validate_on_submit():
				level = request.form['trait_level']
				create_new_items = True if request.form.get('create_new_items') == 'new' else False
				request_email = True if request.form.get('email_checkbox') else False
				traits_form = CreateTraits().update(level)
				# sample_reg_form = SampleRegForm().update(optional=True)
				if level == 'field':
					location_form = LocationForm.update(optional=True)
				else:
					location_form = LocationForm.update(optional=False)
				if all([location_form.validate_on_submit(), traits_form.validate_on_submit()]):
					# Parse the form data
					template_format = request.form['template_format']
					# reduce the traits to those selected from the relevant list
					# drop out boxes checked from other levels and similarly named csrf tokens
					traits = [
						item for sublist in [
							request.form.getlist(i) for i in request.form if
							all(['csrf_token' not in i, level + '-' in i])
						]
						for item in sublist
					]
					# if no traits are selected there is no point continuing
					if len(traits) == 0:
						return jsonify({'submitted': "Please select traits to include"})
					#  - no selection is empty string, convert to None
					#  - convert integers stored as strings to integers
					country = request.form['country'] if request.form['country'] != '' else None
					region = request.form['region'] if request.form['region'] != '' else None
					farm = request.form['farm'] if request.form['farm'] != '' else None
					field_uid = int(request.form['field']) if request.form['field'].isdigit() else None
					block_uid = request.form['block'] if request.form['block'] != '' else None
					per_tree_replicates = int(
						request.form['per_tree_replicates']
					) if request.form['per_tree_replicates'].isdigit() else None
					trees_start = int(
						request.form['trees_start']
					) if request.form['trees_start'].isdigit() else None
					trees_end = int(
						request.form['trees_end']
					) if request.form['trees_end'].isdigit() else None
					branches_start = int(
						request.form['branches_start']
					) if request.form['branches_start'].isdigit() else None
					branches_end = int(
						request.form['branches_end']
					) if request.form['branches_end'].isdigit() else None
					leaves_start = int(
						request.form['leaves_start']
					) if request.form['leaves_start'].isdigit() else None
					leaves_end = int(
						request.form['leaves_end']
					) if request.form['leaves_end'].isdigit() else None
					samples_start = int(
						request.form['samples_start']
					) if request.form['samples_start'].isdigit() else None
					samples_end = int(
						request.form['samples_end']
					) if request.form['samples_end'].isdigit() else None
					tissue = request.form['tissue'] if request.form['tissue'] != '' else None
					storage = request.form['storage'] if request.form['storage'] != '' else None
					per_sample_replicates = int(
						request.form['per_sample_replicates']
					) if request.form['per_sample_replicates'].isdigit() else None
					samples_pooled = True if request.form['samples_pooled'] == 'multiple' else False
					samples_count = request.form['samples_count'] if request.form['samples_count'] else None
					try:
						start_time = int((datetime.strptime(
								request.form['date_from'], '%Y-%m-%d') - datetime(1970, 1, 1)
							).total_seconds() * 1000
						) if request.form['date_from'] != '' else None
					except ValueError:
						start_time = None
					try:
						end_time = int((datetime.strptime(
							request.form['date_to'], '%Y-%m-%d') - datetime(1970, 1, 1)
							).total_seconds() * 1000
						) if request.form['date_to'] != '' else None
					except ValueError:
						end_time = None
					# now generate the files
					download_object = Download(session['username'], request_email)
					download_object.template_files(
						template_format,
						create_new_items,
						level,
						traits,
						country,
						region,
						farm,
						field_uid,
						block_uid,
						per_tree_replicates,
						trees_start,
						trees_end,
						branches_start,
						branches_end,
						leaves_start,
						leaves_end,
						samples_start,
						samples_end,
						tissue,
						storage,
						per_sample_replicates,
						samples_pooled,
						samples_count,
						start_time,
						end_time
					)
					file_list = download_object.get_file_list()
					if not file_list:
						return jsonify(
							{
								'submitted': (
										'No files generated'
								)
							}
						)
					file_list_html = ''
					for i in file_list:
						file_list_html = file_list_html + str("<ul><a href=" + i['url'] + ">" + i['filename'] + "</a></ul>")
					# if selected send an email copy of the file (or link to download if greater than ~5mb)
					if request.form.get('email_checkbox'):
						recipients = [User(session['username']).find('')['email']]
						subject = "BreedCAFS files requested"
						body = (
								"You requested the attached file/s from the BreedCAFS database tools. "
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
										'Your files are ready for download '
										'and a link has been sent to your email address:'
										+ file_list_html
								)
							}
						)
					# return as jsonify so that can be interpreted the same way as error message
					else:
						return jsonify(
							{
								'submitted': (
									'Your files are ready for download: '
									+ file_list_html
								)
							}
						)
				else:
					errors = jsonify([record_form.errors, location_form.errors])
					return errors
			else:
				errors = jsonify([record_form.errors])
				return errors
		except (ServiceUnavailable, AuthError):
			flash("Database unavailable")
			return redirect(url_for('index'))


# Samples
@app.route('/sample_reg', methods=['GET', 'POST'])
def sample_reg():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			location_form = LocationForm.update()
			add_tissue_form = AddTissueForm()
			add_storage_form = AddStorageForm()
			sample_reg_form = SampleRegForm.update()
			# custom_sample_form = CustomSampleForm()
			return render_template(
				'sample_reg.html',
				location_form=location_form,
				add_tissue_form=add_tissue_form,
				add_storage_form=add_storage_form,
				sample_reg_form=sample_reg_form,
				# custom_sample_form=custom_sample_form,
				title='Sample registration'
			)
		except (ServiceUnavailable, AuthError):
			flash("Database unavailable")
			return redirect(url_for('index'))


class Tissues(MethodView):
	@staticmethod
	def get():
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				tissues = SelectionList.get_tissues()
				response = make_response(jsonify(tissues))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable, AuthError):
				flash("Database unavailable")
				return redirect(url_for('index'))


@app.route('/add_tissue', methods=["POST"])
def add_tissue():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			form = AddTissueForm()
			text_tissue = request.form['text_tissue'].strip()
			if form.validate_on_submit():
				find_tissue = MatchNode.tissue(text_tissue)
				if find_tissue:
					return jsonify({"found": find_tissue[1]})
				else:
					new_tissue = Samples().add_tissue(text_tissue)
					return jsonify({"submitted": new_tissue[0]['name'].title()})
			else:
				return jsonify([form.errors])
		except (ServiceUnavailable, AuthError):
			flash("Database unavailable")
			return redirect(url_for('index'))


class StorageMethods(MethodView):
	@staticmethod
	def get():
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				storage_methods = SelectionList.get_storage_types()
				response = make_response(jsonify(storage_methods))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable, AuthError):
				flash("Database unavailable")
				return redirect(url_for('index'))


@app.route('/add_storage', methods=["POST"])
def add_storage():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			form = AddStorageForm()
			text_storage = request.form['text_storage'].strip()
			if form.validate_on_submit():
				find_storage = MatchNode.storage(text_storage)
				if find_storage:
					return jsonify({"found": find_storage[1]})
				else:
					new_storage = Samples().add_storage(text_storage)
					return jsonify({"submitted": new_storage[0]['name'].title()})
			else:
				return jsonify([form.errors])
		except (ServiceUnavailable, AuthError):
			flash("Database unavailable")
			return redirect(url_for('index'))
