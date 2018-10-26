# -*- coding: utf-8 -*-

from wtforms import (
	StringField,
	PasswordField,
	DecimalField,
	BooleanField, 
	SelectField, 
	SelectMultipleField, 
	IntegerField, 
	SubmitField, 
	DateField, 
	# DateTimeField,
	widgets, 
	# FieldList,
	ValidationError
)
from wtforms.validators import (
	InputRequired,
	Optional,
	Email,
	# EqualTo,
	NumberRange,
	Length,
	Regexp
)
from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileRequired
# from app import app
from app.models import (
	TraitList,
	TreatmentList,
	SelectionList,
	Fields,
	User
)
from collections import defaultdict
from safe import check


# custom validators
def safe_password_check(form, field):
	if bool(check(field.data)):
		pass
	else:
		raise ValidationError('Please choose a more complex password')


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
			Regexp("([^\x00-\x7F]|\w|\s)+$", message='Username contains illegal characters')
		],
		filters = [strip_filter],
		description = "Enter a username"
	)
	email = StringField(
		'Email Address:',
		[
			InputRequired(),
			Email(),
			Length(min=1, max=254, message='Maximum 100 characters')
		],
		filters = [strip_filter],
		description = "Enter your email address"
	)
	name = StringField(
		'Full Name:',
		[
			InputRequired(),
			Length(min=1, max=100, message='Maximum 100 characters')
		],
		filters = [strip_filter],
		description = "Enter your full name"
	)
	password = PasswordField(
		'New Password:',
		[
			InputRequired(),
			Length(min=8, max=100, message='Passwords must be at least 8 characters'),
			safe_password_check
		],
		description = "Please choose a secure password for this site"
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
		widget = widgets.ListWidget(prefix_label=False)
	)
	other = SelectMultipleField(
		'Other partners',
		[Optional()],
		option_widget = widgets.CheckboxInput(), 
		widget = widgets.ListWidget(prefix_label=False)
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
		description = 'Add new allowed email'
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
		option_widget = widgets.CheckboxInput(), 
		widget = widgets.TableWidget(with_table_tag=False)
	)
	unconfirmed_users = SelectMultipleField(
		'Unconfirmed users',
		[Optional()],
		option_widget = widgets.CheckboxInput(), 
		widget = widgets.TableWidget(with_table_tag=False)
	)

	@staticmethod
	def update(
			user,
			access
	):
		form = UserAdminForm()
		form.confirmed_users.choices = []
		form.unconfirmed_users.choices = []
		users = [record[0] for record in User(user).get_users_for_admin(access)]
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
		option_widget = widgets.CheckboxInput(), 
		widget = widgets.TableWidget(with_table_tag=False)
	)
	not_partner_admins = SelectMultipleField(
		'Not partner admins',
		[Optional()],
		option_widget = widgets.CheckboxInput(), 
		widget = widgets.TableWidget(with_table_tag=False)
	)

	@staticmethod
	def update():
		form = PartnerAdminForm()
		form.partner_admins.choices = []
		form.not_partner_admins.choices = []
		users = [record[0] for record in User.admin_get_partner_admins()]
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
		description = "Add new country"
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
		description = "Add new region"
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
		description = "Add new farm"
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
		description = "Add new field"
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
			Regexp('([^\x00-\x7F]|\w|\s|[+-])+$', message='Block name contains illegal characters'),
			Length(min=1, max=50, message='Maximum 50 characters')
		],
		filters=[strip_filter],
		description = "Add new block")
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
		description= "Number of new trees")
	submit_trees = SubmitField('Register new trees')


# Samples
class AddTissueForm(FlaskForm):
	id = "add_tissue_form"
	text_tissue = StringField(
		'Tissue type text',
		[
			InputRequired(),
			Regexp("([^\x00-\x7F]|\w|\s)+$", message='Tissue name contains illegal characters'),
			Length(min=1, max=100, message='Maximum 100 characters')
		],
		filters=[strip_filter],
		description = "Add new tissue")
	submit_tissue = SubmitField('+')


class AddStorageForm(FlaskForm):
	id = "add_storage_form"
	text_storage = StringField(
		'Storage type text',
		[
			InputRequired(),
			Regexp("([^\x00-\x7F]|\w|\s|[+-])+$", message='Storage name contains illegal characters'),
			Length(min=1, max=100, message='Maximum 100 characters')
		],
		filters=[strip_filter],
		description = "Add new storage method")
	submit_storage = SubmitField('+')


class SampleRegForm(FlaskForm):
	id = "sample_reg_form"
	email_checkbox = BooleanField('Email checkbox')
	trees_start = IntegerField(
		'Start TreeID',
		[
			NumberRange(min=1, max=1000000, message='Maximum tree ID is 1000000')
		],
		description= "Start TreeID"
	)
	trees_end = IntegerField(
		'End TreeID',
		[
			NumberRange(min=1, max=1000000, message='Maximum tree ID is 1000000')
		],
		description= "End TreeID"
	)
	replicates = IntegerField(
		'Replicates',
		[
			NumberRange(min=1, max=100, message='Maximum of 100 replicates per submission')
		],
		description= "Replicates (per tree)"
	)
	tissue = SelectField('Tissue: ')
	storage = SelectField('Storage: ')
	date_collected = DateField(
		'Date collected (YYYY-mm-dd): ',
		format='%Y-%m-%d',
		description= 'Date collected (YYYY-mm-dd)'
	)
	submit_samples = SubmitField('Register samples')

	@staticmethod
	def update(optional = False):
		form = SampleRegForm()
		tissues = SelectionList.get_tissues()
		storage_types = SelectionList.get_storage_types()
		form.tissue.choices = [('', 'Select Tissue')] + tissues
		form.storage.choices = [('', 'Select Storage')] + storage_types
		if optional:
			form.trees_start.validators.insert(0, Optional())
			form.trees_end.validators.insert(0, Optional())
			form.replicates.validators.insert(0, Optional())
			form.tissue.validators.insert(0, Optional())
			form.storage.validators.insert(0, Optional())
			form.date_collected.validators.insert(0, Optional())
		else:
			form.trees_start.validators.insert(0, Optional())
			form.trees_end.validators.insert(0, Optional())
			form.replicates.validators.append(InputRequired())
			form.tissue.validators.append(InputRequired())
			form.storage.validators.append(InputRequired())
			form.date_collected.validators.append(InputRequired())
		return form


class AddTreatment(FlaskForm):
	id = "add_treatment"
	text_treatment = StringField(
		'Treatment name',
		[
			InputRequired(),
			Regexp('([^\x00-\x7F]|\w|\s)+$', message='Treatment name contains illegal characters'),
			Length(min=1, max=50, message='Maximum 50 characters')
		],
		filters=[strip_filter],
		description="Add new treatment"
	)
	submit_treatment = SubmitField('+')


class AddTreatmentCategory(FlaskForm):
	id = "add_treatment_category"
	text_treatment_category = StringField(
		'Add treatment category',
		[
			InputRequired(),
			Regexp('([^\x00-\x7F]|\w|\s)+$', message='Treatment category name contains illegal characters'),
			Length(min=1, max=50, message='Maximum 50 characters')
		],
		filters=[strip_filter],
		description="Add new category to selected treatment"
	)
	submit_treatment = SubmitField('+')


# Record
class RecordForm(FlaskForm):
	record_type = SelectField (
		'Record level',
		[InputRequired()],
		description="Type of record",
		choices=[
			('', 'Select Record Type'),
			('weather', 'Weather'),
			('treatment', 'Treatment'),
			('input', 'Input'),
			('output', 'Output'),
			('labour', 'Labour'),
			('transaction', 'Transaction'),
		]
	)
	#trees_start = IntegerField(
	#	'Start TreeID',
	#	[
	#		Optional(),
	#		NumberRange(min=min, max=max, message='An integer (' + str(min) + ' to ' + str(max) + ')')
	#	],
	#	description="Start TreeID",
	#)
	#trees_end = IntegerField(
	#	'End TreeID',
	#	[
	#		Optional(),
	#		NumberRange(min=min, max=max, message='An integer (' + str(min) + ' to ' + str(max) + ')')
	#	],
	#	description="End TreeID",
	#)
	#treatment_name = SelectField(
	#	'Treatment name',
	#	[InputRequired()]
	#)
	#treatment_categories = SelectField(
	#	'Treatment categories',
	#	[Optional()],
	#	option_widget=widgets.CheckboxInput(),
	#	widget=widgets.ListWidget(prefix_label=False)
	#)
	submit_record = SubmitField('Assign items to treatment group')

	@staticmethod
	def update():
		form = RecordForm()
		#treatments = SelectionList.get_treatments()
		#form.treatment_name.choices = [('', 'Select Treatment')] + treatments
		#treatment_name = form.treatment_name.data if form.treatment_name.data != '' else None
		#treatment_details = TreatmentList.get_treatment_details('agroforestry')
		#treatment_categories = treatment_details[0]['categories']
		#form.treatment_categories.choices = [(i,i) for i in treatment_categories]
		return form


class WeatherForm(FlaskForm):
	weather_start = DateField(
		'Weather Start',
		[InputRequired()],
		description='Start of report period'

	)
	weather_end = DateField(
		'Weather End',
		[InputRequired()],
		description='End of report period'
	)
	wind_speed_max = DecimalField(
		'Peak wind speed',
		[Optional()],
		description='Peak wind speed in km/h'
	)
	wind_direction = SelectField(
		'Dominant wind direction',
		[Optional()],
		description='Dominant cardinal wind direction',
		choices=[
			('n','N'),('ne','NE'),('e','E'),('se','SE'),('s','S'),('sw','SW'),('w','W'),('nw','NW')]
	)
	temperature_max = DecimalField(
		'Maximum temperature',
		[Optional()],
		description='Maximum temperature (°C)'
	)
	temperature_min = DecimalField(
		'Minimum temperature',
		[Optional()],
		description='Minimum temperature (°C)'
	)
	solar_radiation = DecimalField(
		'Solar radiation',
		[Optional()],
		description='Solar radiation (W/m²)'
	)
	rainfall = DecimalField(
		'Rainfall',
		[Optional()],
		description='Rainfall (mm)'
	)
	humidity = DecimalField(
		'Relative humidity',
		[Optional()],
		description='Relative humidity (%)'
	)
	submit_weather = SubmitField('Submit weather record')


# Collect
class CollectForm(FlaskForm):
	min = 1
	max = 1000000
	trait_level = SelectField(
		'Trait level',
		[InputRequired()],
		choices=[
			('', 'Select Level'),
			('field', 'Field'),
			('block', 'Block'),
			('tree', 'Tree'),
			('branch', 'Branch'),
			('leaf', 'Leaf'),
			('sample', 'Sample')
		]
	)
	template_format = SelectField(
		'Template format',
		[InputRequired()],
		choices=[('', 'Select Format'), ('xlsx', 'Table (xlsx)'), ('csv', 'Table (CSV)'), ('fb', 'Field Book')]
	)
	trees_start = IntegerField(
		'Start TreeID',
		[
			Optional(),
			NumberRange(min=min, max=max, message='An integer (' + str(min) + ' to ' + str(max) + ')')
		],
		description="Start TreeID",
	)
	trees_end = IntegerField(
		'End TreeID',
		[
			Optional(),
			NumberRange(min=min, max=max, message='An integer (' + str(min) + ' to ' + str(max) + ')')
		],
		description="End TreeID",
	)
	branches_start = IntegerField(
		'Start BranchID',
		[
			Optional(),
			NumberRange(min=min, max=max, message='An integer (' + str(min) + ' to ' + str(max) + ')')
		],
		description="Start BranchID")
	branches_end = IntegerField(
		'End BranchID',
		[
			Optional(),
			NumberRange(min=min, max=max, message='An integer (' + str(min) + ' to ' + str(max) + ')')
		],
		description="End BranchID")
	leaves_start = IntegerField(
		'Start LeafID',
		[
			Optional(),
			NumberRange(min=min, max=max, message='An integer (' + str(min) + ' to ' + str(max) + ')')
		],
		description="Start LeafID")
	leaves_end = IntegerField(
		'End LeafID',
		[
			Optional(),
			NumberRange(min=min, max=max, message='An integer (' + str(min) + ' to ' + str(max) + ')')
		],
		description="End LeafID"
	)
	samples_start = IntegerField(
		'Start SampleID',
		[
			Optional(),
			NumberRange(min=min, max=max, message='An integer (' + str(min) + ' to ' + str(max) + ')')
		],
		description="Start SampleID",
	)
	samples_end = IntegerField(
		'End SampleID',
		[
			Optional(),
			NumberRange(min=min, max=max, message='An integer (' + str(min) + ' to ' + str(max) + ')')
		],
		description="End SampleID"
	)
	samples_pooled = SelectField(
		'Samples are pooled from multiple trees',
		[InputRequired()],
		choices=[('single', 'One per tree'), ('multiple', 'Pooled')]
	)
	samples_count = IntegerField(
		'Sample count',
		[NumberRange(min=1, max=10000, message='Register up to 10000 samples per field')],
		description="Sample count"
	)
	date_from = DateField(
		'Date start (YYYY-mm-dd): ',
		[Optional()],
		format='%Y-%m-%d',
		description='Start date'
	)
	date_to = DateField(
		'Date end (YYYY-mm-dd): ',
		[Optional()],
		format='%Y-%m-%d',
		description='End date'
	)
	per_tree_replicates = IntegerField(
		'Replicates per tree',
		[
			Optional(),
			NumberRange(min=1, max=1000, message='Maximum of 1000 replicates per tree per submission')
		],
		description="Replicates (number of branches/leaves/samples per tree)"
	)
	per_sample_replicates = IntegerField(
		'Measures per sample',
		[
			Optional(),
			NumberRange(min=1, max=10000, message='Maximum of 10000 measures per sample per submission')
		],
		description="Replicates (number of measures for each trait performed on each sample)"
	)
	date_collected = DateField(
		'Date samples collected (YYYY-mm-dd): ',
		[Optional()],
		format='%Y-%m-%d',
		description='Date samples collected'
	)
	create_new_items = SelectField(
		'Find existing or create new items',
		[InputRequired()],
		choices=[('existing', 'Find existing items'), ('new', 'Create new items')]
	)
	submit_collect = SubmitField('Generate file/s')

	@staticmethod
	def update():
		form = CollectForm()
		if form.samples_pooled == 'single':
			form.samples_count.validators.insert(0, InputRequired())
		else:
			form.samples_count.validators.insert(0, Optional())
		return form


# upload
class UploadForm(FlaskForm):
	id = "upload_form"
	submission_type = SelectField(
		'Submission type:',
		[InputRequired()],
		choices=[('table', 'Table (xlsx, csv)'), ('FB', 'Field Book (.csv)')]
	)
	file = FileField(
		'Select a file:',
		[FileRequired()]
	)
	upload_submit = SubmitField('Upload')


# download
class DownloadForm(FlaskForm):
	trait_level = SelectField(
		'Trait level',
		[InputRequired()],
		choices = [
			('', 'Select Level'),
			('field', 'Field'),
			('block', 'Block'),
			('tree', 'Tree'),
			('branch', 'Branch'),
			('leaf', 'Leaf'),
			('sample', 'Sample')
			]
	)
	date_from = DateField(
		'Date start (YYYY-mm-dd): ',
		[Optional()],
		format='%Y-%m-%d',
		description = 'Start date'
	)
	date_to = DateField(
		'Date end (YYYY-mm-dd): ',
		[Optional()],
		format='%Y-%m-%d',
		description = 'End date'
	)
	data_format = SelectField(
		'Data format',
		[InputRequired()],
		choices = [('', 'Select Format'), ('table', 'Table'), ('db', 'Database')]
	)
	submit_download = SubmitField('Generate file')


# traits
class CreateTraits(FlaskForm):
	email_checkbox = BooleanField('Email checkbox')

	def __init__(self, *args, **kwargs):
		super(CreateTraits, self).__init__(*args, **kwargs)
		# the updates are methods so that flask can load while the database is unavailable
		levels = ['field', 'block', 'tree', 'branch', 'leaf', 'sample']
		self.levels_groups_traits = defaultdict(lambda: defaultdict(list))
		# fill this dictionary
		for level in levels:
			# get a list of dictionaries of properties from each trait node at this level
			traits = sorted(TraitList.get_traits(level), key = lambda dict: dict['name'])
			# merge this into our nested defaultdict[level] with group as key and list of traits as value
			for trait in traits:
				self.levels_groups_traits[level][trait['group']].append((trait['name_lower'], trait['name']))
			# and create empty attributes for each group
			for group in self.levels_groups_traits[level]:
				setattr(CreateTraits, group, SelectMultipleField(group,
					option_widget = widgets.CheckboxInput(),
					widget = widgets.ListWidget(prefix_label=False)))

	def update(self, level):
		# create a form instance
		form = CreateTraits(prefix = level)
		# need to replacce the prefix that FlaskForm generates with nothing for matching to dictionary,
		# generate the string here to match for the replace
		prefix = level + '-'
		# give it a relevant ID
		id = level + "_traits_form"
		# dynamically add the group choices to the form instance
		for field in form:
			#the recursive part here is just to add the prefix to the item to compare against the fieldnames
			if field.name[len(prefix):] in self.levels_groups_traits[level]:
				field.choices = self.levels_groups_traits[level][field.name[len(prefix):]]
		return form
