# -*- coding: utf-8 -*-

from wtforms import (
	Field,
	StringField,
	PasswordField,
	DecimalField,
	HiddenField,
	BooleanField, 
	SelectField, 
	SelectMultipleField, 
	IntegerField, 
	SubmitField, 
	DateField, 
	widgets,
	ValidationError
)
from wtforms.validators import (
	InputRequired,
	Optional,
	Email,
	NumberRange,
	Length,
	Regexp
)

import markupsafe
from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileRequired
# from app import app
from app.models import (
	SelectionList,
	Parsers,
	User
)
from flask import session

# from collections import defaultdict
from safe import check

from werkzeug.utils import secure_filename


from wtforms.widgets import (
	HTMLString,
	html_params
)

from wtforms.compat import (
	text_type
)


class CustomTableWidget(object):
	def __init__(self, with_table_tag=True):
		self.with_table_tag = with_table_tag

	def __call__(self, field, **kwargs):
		html = []
		if self.with_table_tag:
			kwargs.setdefault('id', field.id)
			html.append('<table %s>' % html_params(**kwargs))
		hidden = ''
		for subfield in field:
			if subfield.type in ('HiddenField', 'CSRFTokenField'):
				hidden += text_type(subfield)
			else:
				html.append('<tr>%s<td>%s%s</td></tr>' % (text_type(subfield.label.text), hidden, text_type(subfield)))
				hidden = ''
		if self.with_table_tag:
			html.append('</table>')
		if hidden:
			html.append(hidden)
		return markupsafe.Markup(''.join(html))


# custom validators
def safe_password_check(form, field):
	if bool(check(field.data)):
		pass
	else:
		raise ValidationError('Please choose a more complex password')


def range_list_check(form, field):
	try:
		Parsers.parse_range_list(field.data)
	except ValueError:
		raise ValidationError('List should be comma separated with hyphens for ranges, e.g. "1,2-5". ')


def secure_filename_check(form, field):
	if not field.data == secure_filename(field.data):
		raise ValueError(
			'This filename is not safe for the system. '
			'Please remove any special characters from the filename. '
		)


# custom filters
def strip_filter(x):
	return x.strip() if x else None


# user registration
class RegistrationForm(FlaskForm):
	partner = SelectField('Partner:', [InputRequired()])
	username = StringField(
		'Username:',
		[
			InputRequired(),
			Length(min=1, max=20, message='Maximum 20 characters'),
			Regexp("^[a-zA-Z]+$", message='Username contains illegal characters')
		],
		filters=[strip_filter],
		description="Enter a username"
	)
	email = StringField(
		'Email Address:',
		[
			InputRequired(),
			Email(),
			Length(min=1, max=254, message='Maximum 100 characters')
		],
		filters=[strip_filter],
		description="Enter your email address"
	)
	name = StringField(
		'Full Name:',
		[
			InputRequired(),
			Length(min=1, max=100, message='Maximum 100 characters')
		],
		filters=[strip_filter],
		description="Enter your full name"
	)
	password = PasswordField(
		'New Password:',
		[
			InputRequired(),
			Length(min=8, max=100, message='Passwords must be at least 8 characters'),
			safe_password_check
		],
		description="Please choose a secure password for this site"
	)

	@staticmethod
	def update():
		form = RegistrationForm()
		form.partner.choices = SelectionList.get_partners()
		return form


# allow unconfirmed affiliations to be removed, but not once confirmed
class AffiliationForm(FlaskForm):
	pending = SelectMultipleField(
		'Pending confirmation',
		[Optional()],
		option_widget = widgets.CheckboxInput(), 
		widget=widgets.ListWidget(prefix_label=False)
	)
	other = SelectMultipleField(
		'Other partners',
		[Optional()],
		option_widget=widgets.CheckboxInput(),
		widget=widgets.ListWidget(prefix_label=False)
	)
	submit_affiliations = SubmitField('Add/remove affiliations')

	@staticmethod
	def update(username):
		form = AffiliationForm()
		affiliations = User(username).get_user_affiliations()
		confirmed = set(affiliations['confirmed'])
		pending = set(affiliations['pending'])
		other = set(SelectionList.get_partners()) - confirmed - pending
		form.pending.choices = sorted(tuple(pending), key=lambda tup: tup[1])
		form.other.choices = sorted(tuple(other), key=lambda tup: tup[1])
		return form


class AddUserEmailForm(FlaskForm):
	user_email = StringField(
		'Add user email address:',
		[
			InputRequired(),
			Email(),
			Length(min=1, max=254, message='Maximum 100 characters')
		],
		description='Add new allowed email'
	)
	submit_user_email = SubmitField('+')


class RemoveUserEmailForm(FlaskForm):
	emails_list = SelectMultipleField(
		'Allowed emails:',
		[Optional()],
		option_widget = widgets.CheckboxInput(), 
		widget = widgets.ListWidget(prefix_label=False))
	remove_user_email = SubmitField('Remove selected emails')

	@staticmethod
	def update(username):
		form = RemoveUserEmailForm()
		form.emails_list.choices = [
			(i, i) for i in User(username).get_user_allowed_emails()
		]
		return form


# user administration
class UserAdminForm(FlaskForm):
	confirmed_users = SelectMultipleField(
		'Confirmed users',
		[Optional()],
		option_widget=widgets.CheckboxInput(),
		widget=CustomTableWidget(with_table_tag=False)
	)
	unconfirmed_users = SelectMultipleField(
		'Unconfirmed users',
		[Optional()],
		option_widget=widgets.CheckboxInput(),
		widget=CustomTableWidget(with_table_tag=False)
	)

	@staticmethod
	def update(
			user,
			access
	):
		form = UserAdminForm()
		form.confirmed_users.choices = []
		form.confirmed_users.checked = False
		form.unconfirmed_users.choices = []
		form.unconfirmed_users.checked = True
		users = User(user).get_users_for_admin(access)
		for user in users:
			user_table = "<td>" + user['Name'] + "</td><td>" + user['Partner'] + "</td>"
			if user['Confirmed']:
				form.confirmed_users.choices.append(
					('{"username":"' + user['Username'] + '", "partner":"' + user['Partner'] + '"}', user_table)
				)
			else:
				form.unconfirmed_users.choices.append(
					('{"username":"' + user['Username'] + '", "partner":"' + user['Partner'] + '"}', user_table)
				)
		return form


class PartnerAdminForm(FlaskForm):
	partner_admins = SelectMultipleField(
		'Partner admins',
		[Optional()],
		option_widget=widgets.CheckboxInput(),
		widget=CustomTableWidget(with_table_tag=False)
	)
	not_partner_admins = SelectMultipleField(
		'Not partner admins',
		[Optional()],
		option_widget=widgets.CheckboxInput(),
		widget=CustomTableWidget(with_table_tag=False)
	)

	@staticmethod
	def update():
		form = PartnerAdminForm()
		form.partner_admins.choices = []
		form.partner_admins.checked = False
		form.not_partner_admins.choices = []
		form.not_partner_admins.checked = False
		users = User.admin_get_partner_admins()
		for user in users:
			user_table = "<td>" + user['Name'] + "</td><td>" + user['Partner'] + "</td>"
			if user['Confirmed']:
				form.partner_admins.choices.append(
					('{"username":"' + user['Username'] + '", "partner":"' + user['Partner'] + '"}', user_table)
				)
			else:
				form.not_partner_admins.choices.append(
					('{"username":"' + user['Username'] + '", "partner":"' + user['Partner'] + '"}', user_table)
				)
		return form


class PasswordResetRequestForm(FlaskForm):
	email = StringField(
		'Email Address:',
		[
			InputRequired(),
			Email(),
			Length(min=1, max=254, message=' Maximum 254 characters')
		]
	)


class PasswordResetForm(FlaskForm):
	password = PasswordField(
		'New Password:',
		[
			InputRequired(),
			Length(min=8, max=100, message='Passwords must be at least 8 characters'),
			safe_password_check
		]
	)


class LoginForm(FlaskForm):
	username = StringField('Username:', [InputRequired()])
	password = PasswordField('Password:', [InputRequired()])


# Location (used on multiple pages)
# NB:
# all wtforms fields require names (e.g. 'Country text input', even though they are not displayed)
# for validation to work when multiple forms are on a page

class LocationForm(FlaskForm):
	id = "location_form"
	country = SelectField('Country')
	region = SelectField('Region')
	farm = SelectField('Farm')
	field = SelectField('Field')
	block = SelectField('Block',  [Optional()])

	@staticmethod
	def update(optional = False):
		form = LocationForm()
		country = form.country.data if form.country.data != '' else None
		region = form.region.data if form.region.data != '' else None
		farm = form.farm.data if form.farm.data != '' else None
		field_uid = form.field.data if form.field.data != '' else None
		countries = SelectionList.get_countries()
		regions = SelectionList.get_regions(country)
		farms = SelectionList.get_farms(country, region)
		fields = SelectionList.get_fields(country, region, farm)
		blocks = SelectionList.get_blocks(country, region, farm, field_uid)
		form.country.choices = [('', 'Select Country')] + countries
		form.region.choices = [('', 'Select Region')] + regions
		form.farm.choices = [('', 'Select Farm')] + farms
		form.field.choices = [('', 'Select Field')] + fields
		form.block.choices = [('', 'Select Block')] + blocks
		if optional:
			form.country.validators = [Optional()]
			form.region.validators = [Optional()]
			form.farm.validators = [Optional()]
			form.field.validators = [Optional()]
		else:
			form.country.validators = [InputRequired()]
			form.region.validators = [InputRequired()]
			form.farm.validators = [InputRequired()]
			form.field.validators = [InputRequired()]
		return form


class AddCountry(FlaskForm):
	id = "add_country"
	text_country = StringField(
		'Country text input',
		[
			InputRequired(),
			Regexp('([^\x00-\x7F]|\w|\s)+$', message='Country name contains illegal characters'),
			Length(min=1, max=50, message='Maximum 50 characters')
		],
		filters=[strip_filter],
		description="Add new country"
	)
	submit_country = SubmitField('+')


class AddRegion(FlaskForm):
	id = "add_region"
	text_region = StringField(
		'Region text input',
		[
			InputRequired(),
			Regexp('([^\x00-\x7F]|\w|\s)+$', message='Region name contains illegal characters'),
			Length(min=1, max=50, message='Maximum 50 characters')
		],
		filters=[strip_filter],
		description="Add new region"
	)
	submit_region = SubmitField('+')


class AddFarm(FlaskForm):
	id = "add_farm"
	text_farm = StringField(
		'Farm text input',
		[
			InputRequired(),
			Regexp('([^\x00-\x7F]|\w|\s)+$', message='Farm name contains illegal characters'),
			Length(min=1, max=50, message='Maximum 50 characters')
		],
		filters=[strip_filter],
		description="Add new farm"
	)
	submit_farm = SubmitField('+')


class AddField(FlaskForm):
	id = "add_field"
	text_field = StringField(
		'Field text input',
		[
			InputRequired(),
			Regexp('([^\x00-\x7F]|\w|\s)+$', message='Field name contains illegal characters'),
			Length(min=1, max=50, message='Maximum 50 characters')
		],
		filters = [strip_filter],
		description="Add new field"
	)
	submit_field = SubmitField('+')


# Fields
class FieldsForm(FlaskForm):  # get details of field
	id = "fields_csv_form"
	email_checkbox = BooleanField('Email checkbox')
	generate_fields_csv = SubmitField('Generate fields.csv')


# Blocks
class AddBlock(FlaskForm):
	id = "add_block"
	text_block = StringField(
		'Block text input',
		[
			InputRequired(),
			Regexp('([^\x00-\x7F]|\w|\s|[+-,])+$', message='Block name contains illegal characters'),
			Length(min=1, max=50, message='Maximum 50 characters')
		],
		filters=[strip_filter],
		description="Add new block")
	submit_block = SubmitField('+')


class BlocksForm(FlaskForm):  # get details of block
	id = "blocks_csv_form"
	email_checkbox = BooleanField('Email checkbox')
	generate_blocks_csv = SubmitField('Generate blocks.csv')


# Trees
class AddTreesForm(FlaskForm):
	id = "add_trees"
	email_checkbox_add = BooleanField('Email checkbox add')
	count = IntegerField(
		'Number of trees: ',
		[
			InputRequired(),
			NumberRange(min=1, max=10000, message='Register up to 10000 trees at a time')
		],
		description="Number of new trees")
	submit_trees = SubmitField('Register new trees')


# Trials
#class TrialForm(FlaskForm):
#	country = SelectField('Country')
#	region = SelectField('Region')
#	farm = SelectField('Farm')
#	field = SelectField('Field')
#	select_trial = SelectField(
#		'Trial',
#		[InputRequired()],
#		description="Trial to modify",
#		choices=[("", "Select Trial")]
#	)
#	# to display either fields or trees relevant fields for item selection
#	item_level = SelectField(
#		'Item Level',
#		[InputRequired()],
#		description="Item Level",
#		choices=[
#			('', 'Select level'),
#			('field', 'Field'),
#			('tree', 'Tree')
#		]
#	)
#	tree_id_list = StringField(
#		'Tree list',
#		[
#			Optional(),
#			Regexp("^[0-9,-]*$", message='List should be comma separated with hyphens for ranges, e.g. "1,2-5"'),
#			range_list_check
#		],
#		description="List of tree IDs, e.g. '1, 2-5' "
#	)
#	assign_to_trial = SubmitField('Assign to trial')
#
#	@staticmethod
#	def update():
#		form = TrialForm()
#		country = form.country.data if form.country.data != '' else None
#		region = form.region.data if form.region.data != '' else None
#		farm = form.farm.data if form.farm.data != '' else None
#		field_uid = form.field.data if form.field.data != '' else None
#		countries = SelectionList.get_countries()
#		regions = SelectionList.get_regions(country)
#		farms = SelectionList.get_farms(country, region)
#		fields = SelectionList.get_fields(country, region, farm)
#		form.country.choices = [('', 'Select Country')] + countries
#		form.region.choices = [('', 'Select Region')] + regions
#		form.farm.choices = [('', 'Select Farm')] + farms
#		form.field.choices = [('', 'Select Field')] + fields
#		form.select_trial.choices = [('', 'Select Trial')] + SelectionList.get_trials(
#			country,
#			region,
#			farm,
#			field_uid
#		)
#		return form


class AddTrial(FlaskForm):
	id = "add_trial"
	text_trial = StringField(
		'Trial text input',
		[
			InputRequired(),
			Regexp('([^\x00-\x7F]|\w|\s)+$', message='Trial name contains illegal characters'),
			Length(min=1, max=50, message='Maximum 50 characters')
		],
		filters=[strip_filter],
		description="Add new trial"
	)
	submit_trial = SubmitField('+')


# Collect
class CollectForm(FlaskForm):
	min = 1
	max = 1000000
	sampling_activity = SelectField(
		'Sampling strategy',
		[InputRequired()],
		description="Sampling activity",
		choices=[
			("sample registration (in situ)", "In situ characterisation"),
			("sample registration (harvest)", "Harvest tissues"),
			("sample registration (sub-sample)", "Sub-sampling")
		]
	)
	item_level = SelectField(
		'Item Level',
		[InputRequired()],
		description="Item Level",
		choices=[
			('', 'Select level')
		]
	)
	field_uid_list = StringField(
		'Field UID list',
		[
			Optional(),
			Regexp("^[0-9,-]*$", message='List should be comma separated with hyphens for ranges, e.g. "1,2-5"'),
			range_list_check
		],
		description="List of field UIDs, e.g. '1, 2-5' "
	)
	block_id_list = StringField(
		'Block ID list',
		[
			Optional(),
			Regexp("^[0-9,-]*$", message='List should be comma separated with hyphens for ranges, e.g. "1,2-5"'),
			range_list_check
		],
		description="List of block IDs, e.g. '1, 2-5' "
	)
	tree_id_list = StringField(
		'Tree list',
		[
			Optional(),
			Regexp("^[0-9,-]*$", message='List should be comma separated with hyphens for ranges, e.g. "1,2-5"'),
			range_list_check
		],
		description="List of tree IDs, e.g. '1, 2-5' "
	)
	sample_id_list = StringField(
		'Tree list',
		[
			Optional(),
			Regexp("^[0-9,-]*$", message='List should be comma separated with hyphens for ranges, e.g. "1,2-5"'),
			range_list_check
		],
		description="List of sample IDs, e.g. '1, 2-5' "
	)
	per_item_count = IntegerField(
		'Samples to register per item',
		[
			Optional(),
			NumberRange(min=1, max=100000, message='Maximum of 100000 ids per item per submission')
		],
		description="Samples to register per item"
	)
	submit_collect = SubmitField('Register samples')

	@staticmethod
	def update():
		form = CollectForm()
		form.item_level.choices = form.item_level.choices + SelectionList.get_item_levels()
		return form


# upload
class UploadForm(FlaskForm):
	id = "upload_form"
	submission_type = SelectField(
		'Submission type:',
		[InputRequired()],
		choices=[
			('table', 'Table (xlsx, csv)'),
			('seq', 'Sequencing data (.fastq, .gz, .zip)')
		]
	)
	filename = HiddenField(
		'filename',
		[
			InputRequired(),
			Length(min=1, max=200, message='Maximum 200 characters in filename')
			# secure_filename_check  # not a useful check here, we just use the secure filename on server side.
		],
		description="filename hidden field"
	)


# download
class DownloadForm(FlaskForm):
	record_type = SelectField(
		[Optional()],
		choices=[('', 'Any')],
		description="Record Type"
	)
	item_level = SelectField(
		'Item Level',
		[Optional()],
		choices=[('', 'Any')],
		description="Item Level"
	)
	submission_date_from = DateField(
		'Submission date start (YYYY-mm-dd): ',
		[Optional()],
		format='%Y-%m-%d',
		description='Submission start date'
	)
	submission_date_to = DateField(
		'Submission date end (YYYY-mm-dd): ',
		[Optional()],
		format='%Y-%m-%d',
		description='Submission end date'
	)
	record_date_from = DateField(
		'Record date start (YYYY-mm-dd): ',
		[Optional()],
		format='%Y-%m-%d',
		description='Record start date'
	)
	record_date_to = DateField(
		'Record date end (YYYY-mm-dd): ',
		[Optional()],
		format='%Y-%m-%d',
		description='Record end date'
	)
	tree_id_list = StringField(
		'Tree list',
		[
			Optional(),
			Regexp("^[0-9,-]*$", message='List should be comma separated with hyphens for ranges, e.g. "1,2-5"'),
			range_list_check
		],
		description="List of tree IDs, e.g. '1, 2-5' "
	)
	sample_id_list = StringField(
		'Sample list',
		[
			Optional(),
			Regexp("^[0-9,-]*$", message='List should be comma separated with hyphens for ranges, e.g. "1,2-5"'),
			range_list_check
		],
		description="List of sample IDs, e.g. '1, 2-5' "
	)
	replicate_id_list = StringField(
		'Sample list',
		[
			Optional(),
			Regexp("^[0-9,-]*$", message='List should be comma separated with hyphens for ranges, e.g. "1,2-5"'),
			range_list_check
		],
		description="List of replicate IDs, e.g. '1, 2-5' "
	)
	input_group = SelectField(
		'Input group',
		[Optional()],
		description="Select group of input variables for form/template",
		choices=[("", "Select group")]
	)
	select_inputs = SelectMultipleField(
		[Optional()],
		coerce=str,
		option_widget=widgets.CheckboxInput(),
		widget=widgets.ListWidget(prefix_label=False),
		description="Select individual input variables to include in the form/template",
		choices=[]
	)
	data_format = SelectField(
		'Data format',
		[InputRequired()],
		choices=[('', 'Select Format'), ('table', 'Table'), ('db', 'Database')]
	)
	file_type = SelectField(
		'Data format',
		[InputRequired()],
		choices=[('', 'Select File Type'), ('xlsx', 'xlsx'), ('csv', 'csv')]
	)
	submit_download = SubmitField('Generate file')

	@staticmethod
	def update():
		form = DownloadForm()
		form.record_type.choices = [('', 'Any')] + SelectionList.get_record_types()
		form.item_level.choices = [('', 'Any')] + SelectionList.get_item_levels()
		item_level = form.item_level.data if form.item_level.data else None
		record_type = form.record_type.data if form.record_type.data else None
		form.input_group.choices = [('', 'Any')] + SelectionList.get_input_groups(
			item_level=item_level,
			record_type=record_type,
			username=session['username']
		)
		selected_input_group = int(form.input_group.data) if form.input_group.data else None
		inputs_list = SelectionList.get_inputs(
			item_level=item_level,
			record_type=record_type,
			input_group=selected_input_group
		)
		form.select_inputs.choices = inputs_list
		return form


class AddInputGroupForm(FlaskForm):
	all_partners = SelectionList.get_partners()
	all_partners = [(i[0], i[0]) for i in all_partners]
	all_partners.insert(0, ("", "Default variable groups"))
	partner_to_copy = SelectField(
		'Select a partner from whom to copy an input group',
		[Optional()],
		description='Filter the groups to copy by partner',
		choices=all_partners
	)
	all_groups = SelectionList.get_input_groups()
	all_groups.insert(0, ("", ""))
	group_to_copy = SelectField(
		'Select input group to make a copy',
		[Optional()],
		description='Select input variable group to copy',
		choices=all_groups
	)
	input_group_name = StringField(
		'Create a new input variable group',
		[
			InputRequired(),
			Length(min=1, max=100, message='Maximum 100 characters')
		],
		description='Enter a label for a new input variable group'
	)
	submit_input_group_name = SubmitField('+')

	@staticmethod
	def update():
		form = AddInputGroupForm()
		partner_to_copy = form.partner_to_copy.data if form.partner_to_copy.data else None
		if partner_to_copy:
			partner_groups = SelectionList.get_input_groups(partner=partner_to_copy)
			partner_groups.insert(0, ("", ""))
			form.group_to_copy.choices = partner_groups
		return form


class ManageInputGroupForm(FlaskForm):
	record_type = SelectField(
		[Optional()],
		choices=[],
		description="Record Type"
	)
	group_filter = SelectField(
		[Optional()],
		choices=[],
		description="Input Group"
	)
	group_levels_select = SelectMultipleField(
		'Group levels',
		[InputRequired()],
		choices=SelectionList.get_item_levels(),
		description='Levels at which input group is available',
		option_widget=widgets.CheckboxInput(),
		widget=widgets.ListWidget(prefix_label=False)
	)
	item_level = SelectField(
		'Item Level',
		[Optional()],
		choices=[],
		description="Item Level"
	)
	input_group_select = SelectField(
		'Input group to manage',
		[
			InputRequired(),
		],
		description='Input group to manage',
		choices=[]
	)
	group_inputs = SelectMultipleField(
		'Group members',
		[Optional()],
		description='Selected inputs',
		widget=widgets.ListWidget(prefix_label=False),
		choices=[]
	)
	all_inputs = SelectMultipleField(
		'Other inputs',
		coerce=str,
		description='Unselected inputs',
		widget=widgets.ListWidget(prefix_label=False),
		choices=[]
	)
	add_to_group = SubmitField('Add to group')
	commit_group_changes = SubmitField('Commit changes to group')

	@staticmethod
	def update():
		form = ManageInputGroupForm()
		form.item_level.choices = [('', 'Any')] + SelectionList.get_item_levels()
		form.record_type.choices = [('', 'Any')] + SelectionList.get_record_types()
		form.group_filter.choices = [('', 'Any')] + SelectionList.get_input_groups(
			username=session['username'],
			include_defaults=True
		)
		partner_groups = SelectionList.get_input_groups(username=session['username'])
		form.input_group_select.choices = partner_groups
		selected_input_group = int(form.input_group_select.data) if form.input_group_select.data else None
		group_members = SelectionList.get_inputs(
			input_group=selected_input_group
		)
		form.group_inputs.choices = group_members
		return form

	@staticmethod
	def commit():
		form = ManageInputGroupForm()
		form.item_level.choices = [('', 'Any')] + SelectionList.get_item_levels()
		form.record_type.choices = [('', 'Any')] + SelectionList.get_record_types()
		form.group_filter.choices = [('', 'Any')] + SelectionList.get_input_groups(
			username=session['username'],
			include_defaults=True
		)
		partner_groups = SelectionList.get_input_groups(username=session['username'])
		form.input_group_select.choices = partner_groups
		form.group_inputs.choices = SelectionList.get_inputs()
		form.all_inputs.choices = SelectionList.get_inputs()
		return form


class RecordForm(FlaskForm):
	item_level = SelectField(
		'Item Level',
		[InputRequired()],
		choices=[],
		description="Item Level"
	)
	field_uid_list = StringField(
		'Field UID list',
		[
			Optional(),
			Regexp("^[0-9,-]*$", message='List should be comma separated with hyphens for ranges, e.g. "1,2-5"'),
			range_list_check
		],
		description="List of field UIDs, e.g. '1, 2-5' "
	)
	block_id_list = StringField(
		'Block ID list',
		[
			Optional(),
			Regexp("^[0-9,-]*$", message='List should be comma separated with hyphens for ranges, e.g. "1,2-5"'),
			range_list_check
		],
		description="List of block IDs, e.g. '1, 2-5' "
	)
	tree_id_list = StringField(
		'Tree list',
		[
			Optional(),
			Regexp("^[0-9,-]*$", message='List should be comma separated with hyphens for ranges, e.g. "1,2-5"'),
			range_list_check
		],
		description="List of tree IDs, e.g. '1, 2-5' "
	)
	sample_id_list = StringField(
		'Sample list',
		[
			Optional(),
			Regexp("^[0-9,-]*$", message='List should be comma separated with hyphens for ranges, e.g. "1,2-5"'),
			range_list_check
		],
		description="List of sample IDs, e.g. '1, 2-5' "
	)
	time_points = IntegerField(
		'Time Points',
		[
			Optional(),
			NumberRange(min=1, max=100, message='Currently limited to to a maximum of 100 time points per item')
		],
		description="Number of rows to include per item (or replicate)"
	)
	replicates = IntegerField(
		'Replicates',
		[
			Optional(),
			NumberRange(min=1, max=100, message='Currently limited to to a maximum of 100 replicates per item')
		],
		description="Number of replicated measurements or curves per item at each time point"
	)
	input_group = SelectField(
		'Input group',
		[InputRequired()],
		description="Input variable group to select fields for form/template",
		choices=[("", "Select group")]
	)
	select_inputs = SelectMultipleField(
		[InputRequired()],
		coerce=str,
		option_widget=widgets.CheckboxInput(),
		widget=widgets.ListWidget(prefix_label=False),
		description='Select input variables to include',
		choices=[]
	)
	record_time = DateField(
		'Record start',
		[Optional()],
		description='Time recorded'
	)
	record_start = DateField(
		'Record start',
		[Optional()],
		description='Start of period'
	)
	record_end = DateField(
		'Record end',
		[Optional()],
		description='End of period'
	)
	template_format = SelectField(
		'Template format',
		[Optional()],
		choices=[
			('xlsx', 'Table (xlsx)')
		],
		description="Type of template file/s to generate"
	)
	submit_records = SubmitField('Submit records')

	@staticmethod
	def update():
		form = RecordForm()
		form.item_level.choices = SelectionList.get_item_levels()
		item_level = form.item_level.data if form.item_level.data else None
		form.input_group.choices += SelectionList.get_input_groups(
			item_level=item_level,
			username=session['username']
		)
		# list is no longer all we need as no longer specifying record type.
		# need to get record types for format choices and for direct submission forms
		selected_input_group = form.input_group.data if form.input_group.data else None
		if selected_input_group:
			inputs_list = SelectionList.get_inputs(
				item_level=item_level,
				input_group=selected_input_group,
				details=True
			)
			for input_variable in inputs_list:
				form.select_inputs.choices.append((input_variable['name_lower'], input_variable['name']))
		return form


class CorrectForm(FlaskForm):
	id = "correct_form"
	submission_type = SelectField(
		'Submission type:',
		[InputRequired()],
		choices=[
			('db', 'Database (xlsx/csv)')
		]
	)
	file = FileField(
		'Select a file:',
		[FileRequired()]
	)
	correct_submit = SubmitField('Delete Records')

