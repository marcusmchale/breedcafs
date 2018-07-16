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
	jsonify,
	request
)
from app.models import (
	Fields,
	User,
	Download,
)
from app.forms import (
	LocationForm,
	RecordForm,
	CreateTraits,
	SampleRegForm,
)
from app.emails import send_email


@app.route('/record', methods=['GET', 'POST'])
def record():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			record_form = RecordForm()
			location_form = LocationForm.update()
			trial_traits_form = CreateTraits().update('trial')
			block_traits_form = CreateTraits().update('block')
			tree_traits_form = CreateTraits().update('tree')
			branch_traits_form = CreateTraits().update('branch')
			leaf_traits_form = CreateTraits().update('leaf')
			sample_traits_form = CreateTraits().update('sample')
			sample_reg_form = SampleRegForm.update()
			return render_template(
				'record.html',
				location_form = location_form,
				record_form = record_form,
				level = 'all',
				trial_traits_form = trial_traits_form,
				block_traits_form = block_traits_form,
				tree_traits_form = tree_traits_form,
				branch_traits_form = branch_traits_form,
				leaf_traits_form = leaf_traits_form,
				sample_traits_form=sample_traits_form,
				sample_reg_form = sample_reg_form,
				title = 'Record'
			)
		except (ServiceUnavailable, AuthError):
			flash("Database unavailable")
			return redirect(url_for('index'))


@app.route('/record/generate_files', methods=['GET', 'POST'])
def generate_files():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			record_form = RecordForm()
			if record_form.validate_on_submit():
				# first ensure selection of trait level and load the appropriate forms
				form_data = request.form
				trait_level = request.form['trait_level']
				traits_form = CreateTraits().update(trait_level)
				# sample_reg_form = SampleRegForm().update(optional=True)
				if trait_level == 'trial':
					location_form = LocationForm.update(optional = True)
				else:
					location_form = LocationForm.update(optional = False)
				if all([location_form.validate_on_submit(), traits_form.validate_on_submit()]):
					# first check traits were selected
					trait_selection = [
						item for sublist in [
							form_data.getlist(i) for i in form_data if
							all(['csrf_token' not in i, form_data['trait_level'] + '-' in i])
						]
						for item in sublist
					]
					if len(trait_selection) == 0:
						return jsonify({'submitted': "Please select traits to include"})
					trial_uid = int(form_data['trial']) if form_data['trial'] else None
					start = int(form_data['trees_start']) if form_data['trees_start'] else 1
					end = int(form_data['trees_end']) if form_data['trees_end'] else 999999
					replicates = int(form_data['replicates']) if form_data['replicates'] else 1
					if trait_level in ['branch', 'leaf']:
						if form_data['old_new_ids'] == 'new':  # create new IDs
							if trait_level == 'branch':
								id_list = Fields.add_branches(trial_uid, start, end, replicates)
							else:  # trait_level == 'leaf':
								id_list = Fields.add_leaves(trial_uid, start, end, replicates)
						else:
							id_list = None
					else:
						id_list = None
					if form_data['template_format'] == 'fb':
						csv_file_details = Download(session['username']).get_index_csv(form_data, id_list)
						if not csv_file_details:
							return jsonify({'submitted': "No entries found that match your selection"})
						# then create the trait files
						trt_file_details = Download(session['username']).create_trt(form_data)
						if not trt_file_details:
							return jsonify({'submitted': "Please select traits to include"})
						file_details_dict = {
							'table': csv_file_details,
							'details': trt_file_details
						}
					else:  # form_data['template_format'] == 'csv'
						file_details_dict = Download(session['username']).get_table_csv(form_data, id_list)
						if not file_details_dict:
							return jsonify({'submitted': "No entries found that match your selection"})
					# create html block with urls
					url_list_html = ''
					for i in file_details_dict:
						item = str(
							'<dt><a href="'
							+ file_details_dict[i]['url']
							+ '">'
							+ file_details_dict[i]['filename']
							+ '</a></dt>'
						)
						url_list_html += item
					# if selected send an email copy of the file (or link to download if greater than ~5mb)
					if request.form.get('email_checkbox'):
						recipients = [User(session['username']).find('')['email']]
						subject = "BreedCAFS files requested"
						body = (
								"You requested the attached file/s from the BreedCAFS database tools. "
								+ str([file_details_dict[i]['url'] for i in file_details_dict])
						)
						html = render_template(
							'emails/generate_files.html',
							file_list = [file_details_dict[i]['url'] for i in file_details_dict]
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
										+ url_list_html
								)
							}
						)
					# return as jsonify so that can be interpreted the same way as error message
					else:
						return jsonify(
							{
								'submitted': (
									'Your files are ready for download: '
									+ url_list_html
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
