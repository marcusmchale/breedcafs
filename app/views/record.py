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
	request,
)

from wtforms import (
	SelectField,
	DateField,
	IntegerField,
	StringField,
	BooleanField,
	DecimalField,
)

from wtforms.validators import (
	InputRequired,
	NumberRange,
	Length,
	Regexp
)

from app.forms import (
	LocationForm,
	RecordForm,
	range_list_check
)

from app.models import (
	Record,
	Parsers,
	Download,
	User,
	InputList,
	SelectionList
)

from app.emails import send_email

from datetime import datetime, timedelta


@app.route('/record', methods=['GET', 'POST'])
def record():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			record_form = RecordForm().update()
			location_form = LocationForm.update(optional=True)
			return render_template(
				'record.html',
				title='Record',
				record_form=record_form,
				location_form=location_form
			)
		except (ServiceUnavailable, SecurityError):
			flash("Database unavailable")
			return redirect(url_for('index'))


@app.route('/record/input_group_management', methods=['GET', 'POST'])
def input_group_management():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	elif all([
		'global_admin' not in session['access'],
		'partner_admin' not in session['access']
	]):
		flash('You attempted to access a restricted page')
		return redirect(url_for('index'))
	else:
		try:
			return render_template(
				'input_group_management.html',
				title='Input variable group management',
			)
		except (ServiceUnavailable, SecurityError):
			flash("Database unavailable")
			return redirect(url_for('index'))


@app.route('/record/generate_template', methods=['POST'])
def generate_template():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			record_form = RecordForm.update()
			location_form = LocationForm.update(optional=True)
			if all([
				record_form.validate_on_submit(),
				location_form.validate_on_submit()
			]):
				username = session['username']
				record_type = request.form['record_type'] if request.form['record_type'] != '' else None
				time_points = int(request.form['time_points']) if request.form['time_points'] != '' else None
				if record_type in [None, 'trait', 'curve']:
					replicates = int(request.form['replicates']) if request.form['replicates'] != '' else None
				else:
					replicates = None
				item_level = request.form['item_level'] if request.form['item_level'] != '' else None
				country = request.form['country'] if request.form['country'] != '' else None
				region = request.form['region'] if request.form['region'] != '' else None
				farm = request.form['farm'] if request.form['farm'] != '' else None
				field_uid = int(request.form['field']) if request.form['field'].isdigit() else None
				block_uid = request.form['block'] if request.form['block'] != '' else None
				tree_id_list = (
					Parsers.parse_range_list(request.form['tree_id_list']) if request.form['tree_id_list'] != '' else None
				)
				sample_id_list = (
					Parsers.parse_range_list(request.form['sample_id_list']) if request.form['sample_id_list'] != '' else None
				)
				input_group = request.form['input_group']
				if 'selected_inputs' in request.form:
					selected_inputs = request.form.getlist('select_inputs')
				else:
					selected_inputs = []

				inputs_dict = {}
				for input_variable in selected_inputs:
					inputs_dict[input_variable] = request.form[input_variable]
				template_format = request.form['template_format']
				record_data = {
					'record_type': record_type,
					'time_points': time_points,
					'replicates': replicates,
					'item_level': item_level,
					'country': country,
					'region': region,
					'farm': farm,
					'field_uid': field_uid,
					'block_uid': block_uid,
					'tree_id_list': tree_id_list,
					'sample_id_list': sample_id_list,
					'input_group': input_group,
					'selected_inputs': selected_inputs,
					'inputs_dict': inputs_dict,
					'template_format': template_format
				}
				download_object = Download(username)
				if download_object.record_form_to_template(record_data):
					file_list = download_object.file_list
					file_list_html = download_object.get_file_list_html()
					if request.form.get('email_checkbox'):
						recipients = [User(username).find('')['email']]
						subject = "BreedCAFS files requested"
						body = (
								" You recently requested a template from the BreedCAFS database. "
								" A spreadsheet file (.xlsx) is attached containing a 'Template' sheet "
								" for data recording. Enter data into this spreadsheet and and upload "
								" the file on the 'Upload' page to record data corresponding to the listed items. "
								" The template file is available at the following address: "
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
										' Your template is available for download. '
										' A link to this file has been sent to your email address:'
										+ file_list_html
								)
							}
						)
					return jsonify(
						{
							'submitted': (
									' Your template is available'
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
					'errors': [record_form.errors, location_form.errors]
				})
		except (ServiceUnavailable, SecurityError):
			flash("Database unavailable")
			return redirect(url_for('index'))


@app.route('/record/submit_records', methods=['POST'])
def submit_records():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		record_form = RecordForm.update()
		location_form = LocationForm.update(optional=True)
		try:
			if all([
				record_form.validate_on_submit(),
				location_form.validate_on_submit()
			]):
				class InputFormDetailed(RecordForm):
					pass

				item_level = request.form['item_level'] if request.form['item_level'] not in ['', 'None'] else None
				record_type = request.form['record_type'] if request.form['record_type'] not in ['', 'None'] else None
				inputs_details = InputList(
					item_level,
					record_type
				).get_inputs(inputs=request.form.getlist('select_inputs') if 'select_inputs' in request.form else None)
				for input_variable in inputs_details:
					if input_variable['format'] in ["numeric", "percent"]:
						min_value = input_variable['minimum'] if 'minimum' in input_variable else None
						max_value = input_variable['maximum'] if 'maximum' in input_variable else None
						if all([min_value, max_value]):
							validator_message = (
									'Must be between ' +
									str(min_value) +
									' and ' +
									str(max_value)
							)
						elif min_value:
							validator_message = (
									'Must be greater than ' +
									str(min_value)
							)
						elif max_value:
							validator_message = (
									'Must be less than ' +
									str(max_value)
							)
						else:
							validator_message = "Number range error"
						setattr(
							InputFormDetailed,
							input_variable['name_lower'],
							DecimalField(
								validators=[
									InputRequired(),
									NumberRange(
										min=min_value,
										max=max_value,
										message=validator_message
									)
								],
								description=input_variable['details']
							)
						)
					elif input_variable['format'] == "date":
						setattr(
							InputFormDetailed,
							input_variable['name_lower'],
							DateField(
								validators=[InputRequired()],
								description=input_variable['details']
							)
						)
					elif input_variable['format'] == "categorical":
						categories_list = [
							(category, category) for category in input_variable['category_list']
						]
						setattr(
							InputFormDetailed,
							input_variable['name_lower'],
							SelectField(
								validators=[InputRequired()],
								choices=categories_list,
								description=input_variable['details']
							)
						)
					elif input_variable['format'] == "boolean":
						setattr(
							InputFormDetailed,
							input_variable['name_lower'],
							BooleanField(
								validators=[InputRequired()],
								description=input_variable['details']
							)
						)
					elif input_variable['format'] == "text":
						if 'time' in input_variable['name_lower']:
							setattr(
								InputFormDetailed,
								input_variable['name_lower'],
								StringField(
									validators=[
										InputRequired(),
										Regexp(
											"^([0-9]|0[0-9]|1[0-9]|2[0-3]):[0-5][0-9]$",
											message='Please use time format HH:mm e.g. 13:00 '
										)
									],
									description=input_variable['details']
								)
							)
						elif input_variable['name_lower'] == 'assign to block':
							setattr(
								InputFormDetailed,
								input_variable['name_lower'],
								IntegerField(
									validators=[
										InputRequired()
									],
									description=input_variable['details']
								)
							)
						elif input_variable['name_lower'] == 'assign to trees':
							setattr(
								InputFormDetailed,
								input_variable['name_lower'],
								StringField(
									validators=[
										InputRequired(),
										Regexp(
											"^[0-9,-]*$",
											message='List should be comma separated with hyphens for ranges, e.g. "1,2-5"'
										),
										range_list_check
									],
									description=input_variable['details']
								)
							)
						else:
							setattr(
								InputFormDetailed,
								input_variable['name_lower'],
								StringField(
									validators=[
										InputRequired(),
										Length(min=1, max=100, message='Maximum 100 characters')
									],
									description=input_variable['details']
								)
							)
				detailed_record_form = InputFormDetailed()
				detailed_record_form.record_type.choices = [('', 'Any')] + SelectionList.get_record_types()
				detailed_record_form.item_level.choices = SelectionList.get_item_levels()
				item_level = request.form['item_level'] if request.form['item_level'] not in ['', 'None'] else None
				record_type = request.form['record_type'] if request.form['record_type'] not in ['', 'None'] else None
				detailed_record_form.input_group.choices += SelectionList.get_input_groups(item_level, record_type)
				if record_type == 'trait':
					detailed_record_form.template_format.choices.append(('fb', 'Field Book'))
					detailed_record_form.record_time.validators = [InputRequired()]
				selected_input_group = (
					request.form['input_group'] if request.form['input_group'] not in [
						'',	'None'
					] else None
				)
				if selected_input_group:
					inputs_details = InputList(
						item_level,
						record_type
					).get_inputs(input_group=selected_input_group)
					inputs_list = [(input_variable['name_lower'], input_variable['name']) for input_variable in inputs_details]
					detailed_record_form.select_inputs.choices = inputs_list
				inputs_list = [(input_variable['name_lower'], input_variable['name']) for input_variable in inputs_details]
				detailed_record_form.select_inputs.choices = inputs_list
				if all([
					detailed_record_form.validate_on_submit(),
					location_form.validate_on_submit()
				]):
					username = session['username']
					record_type = request.form['record_type'] if request.form['record_type'] != '' else None
					item_level = request.form['item_level'] if request.form['item_level'] != '' else None
					country = request.form['country'] if request.form['country'] != '' else None
					region = request.form['region'] if request.form['region'] != '' else None
					farm = request.form['farm'] if request.form['farm'] != '' else None
					field_uid = int(request.form['field']) if request.form['field'].isdigit() else None
					block_uid = request.form['block'] if request.form['block'] != '' else None
					tree_id_list = (
						Parsers.parse_range_list(request.form['tree_id_list']) if request.form['tree_id_list'] != '' else None
					)
					sample_id_list = (
						Parsers.parse_range_list(request.form['sample_id_list']) if request.form['sample_id_list'] != '' else None
					)
					record_time = int(
						(
							datetime.strptime(request.form['record_time'], '%Y-%m-%d') +
							timedelta(hours=12) -
							datetime(1970, 1, 1)
						).total_seconds() * 1000
					) if request.form['record_time'] != '' else None
					start_time = int(
						(datetime.strptime(request.form['record_start'], '%Y-%m-%d') - datetime(1970, 1, 1)).total_seconds()
						* 1000
					) if request.form['record_start'] != '' else None
					# end time defaults last millisecond of the day
					end_time = int(
						(
							datetime.strptime(request.form['record_end'], '%Y-%m-%d') +
							timedelta(days=1) -
							datetime(1970, 1, 1)
						).total_seconds() * 1000
					) if request.form['record_end'] != '' else None
					if all([record_type == 'condition', end_time, start_time >= end_time]):
						return jsonify({
							'submitted': 'Please make sure the start date is before the end date'
						})
					if 'select_inputs' in request.form:
						selected_inputs = request.form.getlist('select_inputs')
					else:
						selected_inputs = []
					inputs_dict = {}
					for input_variable in selected_inputs:
						inputs_dict[input_variable] = request.form[input_variable]
					record_data = {
						'record_type': record_type,
						'item_level': item_level,
						'country': country,
						'region': region,
						'farm': farm,
						'field_uid': field_uid,
						'block_uid': block_uid,
						'tree_id_list': tree_id_list,
						'sample_id_list': sample_id_list,
						'record_time': record_time,
						'start_time': start_time,
						'end_time': end_time,
						'selected_inputs': selected_inputs,
						'inputs_dict': inputs_dict
					}
					result = Record(username).submit_records(record_data)
					return result
				else:
					errors = jsonify({
						'errors': [detailed_record_form.errors, location_form.errors]
					})
					return errors
			else:
				errors = jsonify({
					'errors': [record_form.errors, location_form.errors]
				})
				return errors
		except (ServiceUnavailable, SecurityError):
			flash("Database unavailable")
			return redirect(url_for('index'))
