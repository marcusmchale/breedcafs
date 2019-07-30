from app import (
	app,
	ServiceUnavailable,
	SecurityError
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
	User,
	Download,
	Parsers
)
from app.forms import (
	LocationForm,
	CollectForm
)
from app.emails import send_email


@app.route('/collect', methods=['GET', 'POST'])
def collect():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			collect_form = CollectForm.update()
			location_form = LocationForm.update()
			return render_template(
				'collect.html',
				location_form=location_form,
				collect_form=collect_form,
				title='Collect'
			)
		except (ServiceUnavailable, SecurityError):
			flash("Database unavailable")
			return redirect(url_for('index'))


@app.route('/collect/register_samples', methods=['GET', 'POST'])
def register_samples():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			collect_form = CollectForm.update()
			location_form = LocationForm.update(optional=True)
			if all([
				collect_form.validate_on_submit(),
				location_form.validate_on_submit()
			]):
				level = request.form['item_level'] if request.form['item_level'] != '' else None
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
				if download_object.register_samples(
					level,
					country,
					region,
					farm,
					field_uid,
					block_uid,
					tree_id_list,
					sample_id_list,
					per_item_count
				):
					download_object.item_level = "sample"
					download_object.set_inputs(
						'sample',
						input_group="Registration",
						sample_level=level
					)
					# drop some items from the self.inputs list to avoid confusion when they aren't relevant
					download_object.id_list_to_xlsx_template(
						base_filename="Sample Registration Template",
					)
					file_list = download_object.file_list
					file_list_html = download_object.get_file_list_html()
					if request.form.get('email_checkbox'):
						recipients = [User(session['username']).find('')['email']]
						subject = "BreedCAFS files requested"
						body = (
								" You recently registered samples in the BreedCAFS database. "
								" Unique identifiers (UIDs) have been generated for these samples"
								" for future reference. "
								" A spreadsheet file (.xlsx) has been generated containing these UIDs"
								" and other relevant information about the parent field/block/tree/sample."
								" This same file contains a 'Template' sheet that can be completed"
								" and uploaded to this site to record specific details for these samples. "
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
										' Your samples are registered and a submission template is available'
										' for download. '
										' A link to this file has been sent to your email address:'
										+ file_list_html
								)
							}
						)
					return jsonify(
						{
							'submitted': (
									' Your samples are registered and a submission template is available'
									' for download. '
									+ file_list_html
							)
						}
					)
				else:
					return jsonify({
						'submitted': 'No items found that match your selection'
					})
			else:
				return jsonify({
					'errors': [collect_form.errors, location_form.errors]
				})
		except (ServiceUnavailable, SecurityError):
			flash("Database unavailable")
			return redirect(url_for('index'))
