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
	FindFieldItems,
	AddFieldItems,
	Parsers
)
from app.forms import (
	LocationForm,
	CollectForm,
	CreateTraits
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
			collect_form = CollectForm()
			location_form = LocationForm.update()
			return render_template(
				'collect.html',
				location_form=location_form,
				collect_form=collect_form,
				title='Collect'
			)
		except (ServiceUnavailable, AuthError):
			flash("Database unavailable")
			return redirect(url_for('index'))


@app.route('/collect/register_samples', methods=['GET', 'POST'])
def register_samples():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			collect_form = CollectForm()
			location_form = LocationForm.update(optional=True)
			if all([
				collect_form.validate_on_submit(),
				location_form.validate_on_submit()
			]):
				level = request.form['level'] if request.form['level'] != '' else None
				country = request.form['country'] if request.form['country'] != '' else None
				region = request.form['region'] if request.form['region'] != '' else None
				farm = request.form['farm'] if request.form['farm'] != '' else None
				field_uid = int(request.form['field']) if request.form['field'].isdigit() else None
				block_uid = request.form['block'] if request.form['block'] != '' else None
				tree_id_list = (
					Parsers.parse_range_list(request.form['tree_id_list']) if request.form['tree_id_list'] != ''
					else None
				)
				sample_id_list = (
					Parsers.parse_range_list(request.form['sample_id_list']) if request.form['sample_id_list'] != ''
					else None
				)
				per_item_count = int(
					request.form['per_item_count']
				) if request.form['per_item_count'].isdigit() else 1
				request_email = True if request.form.get('email_checkbox') else False
				download_object = Download(session['username'], request_email)
				download_object.register_samples(
					level,
					country,
					region,
					farm,
					field_uid,
					block_uid,
					tree_id_list,
					sample_id_list,
					per_item_count
				)







@app.route('/collect/generate_files', methods=['GET', 'POST'])
def generate_files():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			collect_form = CollectForm
			if collect_form.validate_on_submit():
				level = request.form['trait_level']
				create_new_items = True if request.form.get('create_new_items') == 'new' else False
				request_email = True if request.form.get('email_checkbox') else False
				traits_form = CreateTraits().update(level)
				if level == 'field':
					location_form = LocationForm.update(optional=True)
				else:
					if not create_new_items:
						location_form = LocationForm.update(optional=True)
					else: location_form = LocationForm.update(optional=False)
				if all([
					location_form.validate_on_submit(),
					traits_form.validate_on_submit(),
				]):
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
					tree_id_list = (Parsers.parse_range_list(request.form['tree_id_list'])
						if request.form['tree_id_list'] else None)
					branch_id_list = (Parsers.parse_range_list(request.form['branch_id_list'])
						if request.form['branch_id_list'] else None)
					leaf_id_list = (Parsers.parse_range_list(request.form['leaf_id_list'])
						if request.form['leaf_id_list'] else None)
					sample_id_list = (Parsers.parse_range_list(request.form['sample_id_list'])
						if request.form['sample_id_list'] else None)
					tissue = request.form['tissue'] if request.form['tissue'] != '' else None
					harvest_condition = request.form['harvest_condition'] if request.form['harvest_condition'] != '' else None
					per_sample_replicates = int(
						request.form['per_sample_replicates']
					) if request.form['per_sample_replicates'].isdigit() else 1
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
						tree_id_list,
						branch_id_list,
						leaf_id_list,
						sample_id_list,
						tissue,
						harvest_condition,
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
										'No items found that match your selection'
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
					errors = jsonify({
						'errors': [collect_form.errors, location_form.errors]
						})
					return errors
			else:
				errors = jsonify({
					'errors': [collect_form.errors]
				})
				return errors
		except (ServiceUnavailable, AuthError):
			flash("Database unavailable")
			return redirect(url_for('index'))

