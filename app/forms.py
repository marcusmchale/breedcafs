from flask import jsonify
from wtforms import (StringField, 
	PasswordField, 
	BooleanField, 
	SelectField, 
	SelectMultipleField, 
	IntegerField, 
	SubmitField, 
	DateField, 
	DateTimeField, 
	widgets, 
	FieldList, 
	ValidationError)
from wtforms.validators import InputRequired, Optional, Email, EqualTo, NumberRange, Length, Regexp
from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileRequired
from app import app
from app.models import Lists, Fields, User
from collections import defaultdict
from safe import check

#custom validators
def safe_password_check(form, field):
	if bool(check(field.data)):
		pass
	else:
		raise ValidationError('Please choose a more complex password')

#user 
class RegistrationForm(FlaskForm):
	strip_filter = lambda x: x.strip() if x else None
	partner = SelectField('Partner:', [InputRequired()])
	username = StringField('Username:', [InputRequired(), 
		Length(min=1, max=20, message='Maximum 20 characters'),
		Regexp('([^\x00-\x7F]|\w)+', message='Username contains illegal characters')],
		filters=[strip_filter],
		description = "Enter a username")
	email = StringField('Email Address:', [InputRequired(), Email(), Length(min=1, 
		max=254, message='Maximum 100 characters')],
		filters = [strip_filter],
		description = "Enter your email address")
	name = StringField('Full Name:', [InputRequired(), Length(min=1, 
		max=100, message='Maximum 100 characters')],
		filters=[strip_filter],
		description = "Enter your full name")
	password = PasswordField('New Password:', [InputRequired(), Length(min=8, max=100,
		message='Passwords must be at least 8 characters'), safe_password_check],
		description = "Please choose a secure password for this site")
	@staticmethod
	def update():
		form = RegistrationForm()
		PARTNERS = Lists('Partner').create_list_tup('name', 'fullname')
		form.partner.choices = sorted(tuple(PARTNERS), key=lambda tup: tup[1])
		return form

#allow unconfirmed affiliations to be removed, but not once confirmed
class AffiliationForm(FlaskForm):
	pending = SelectMultipleField('Pending confirmation',
		[Optional()],
		option_widget = widgets.CheckboxInput(), 
		widget = widgets.ListWidget(prefix_label=False))
	other = SelectMultipleField('Other partners',
		[Optional()],
		option_widget = widgets.CheckboxInput(), 
		widget = widgets.ListWidget(prefix_label=False))
	submit_affiliations = SubmitField('Add/remove affiliations')
	@staticmethod
	def update(username):
		form = AffiliationForm()
		affiliations = User(username).get_user_affiliations()
		confirmed = set(affiliations['confirmed'])
		pending = set(affiliations['pending'])
		other = set(Lists('Partner').create_list_tup('name', 'fullname')) - confirmed - pending
		form.pending.choices = sorted(tuple(pending), key=lambda tup: tup[1])
		form.other.choices = sorted(tuple(other), key=lambda tup: tup[1])
		return form

class AddUserEmailForm(FlaskForm):
	user_email = StringField('Add user email address:', [InputRequired(), Email(), Length(min=1, 
		max=254, message='Maximum 100 characters')],
		description = 'Add new allowed email')
	submit_user_email = SubmitField('+')


class RemoveUserEmailForm(FlaskForm):
	emails_list = SelectMultipleField('Allowed emails:',
		[Optional()],
		option_widget = widgets.CheckboxInput(), 
		widget = widgets.ListWidget(prefix_label=False))
	remove_user_email = SubmitField('Remove selected emails')
	@staticmethod
	def update(username):
		form = RemoveUserEmailForm()
		form.emails_list.choices = [(i,i) for i in User(username).get_user_allowed_emails()]
		return form

#user administration
class UserAdminForm(FlaskForm):
	confirmed_users = SelectMultipleField('Confirmed users', 
		[Optional()],
		option_widget = widgets.CheckboxInput(), 
		widget = widgets.TableWidget(with_table_tag=False))
	unconfirmed_users = SelectMultipleField('Unconfirmed users',
		[Optional()],
		option_widget = widgets.CheckboxInput(), 
		widget = widgets.TableWidget(with_table_tag=False))
	@staticmethod
	def update(user, access):
		form = UserAdminForm()
		form.confirmed_users.choices = []
		form.unconfirmed_users.choices =[]
		users = [record[0] for record in User.get_users_for_admin(user, access)]
		for user in users:
			user_table = "<td>" + user['Name'] + "</td><td>" + user['Partner'] + "</td>"
			if user['Confirmed'] == True:
				form.confirmed_users.choices.append(('{"username":"' + user['Username'] + '", "partner":"' + user['Partner'] + '"}', user_table))
			elif user['Confirmed'] == False:
				form.unconfirmed_users.choices.append(('{"username":"' + user['Username'] + '", "partner":"' + user['Partner'] + '"}', user_table))
		return form

class PartnerAdminForm(FlaskForm):
	partner_admins = SelectMultipleField('Partner admins',
		[Optional()],
		option_widget = widgets.CheckboxInput(), 
		widget = widgets.TableWidget(with_table_tag=False))
	not_partner_admins = SelectMultipleField('Not partner admins',
		[Optional()],
		option_widget = widgets.CheckboxInput(), 
		widget = widgets.TableWidget(with_table_tag=False))
	@staticmethod
	def update():
		form = PartnerAdminForm()
		form.partner_admins.choices = []
		form.not_partner_admins.choices =[]
		users = [record[0] for record in User.admin_get_partner_admins()]
		for user in users:
			user_table = "<td>" + user['Name'] + "</td><td>" + user['Partner'] + "</td>"
			if user['Confirmed'] == True:
				form.partner_admins.choices.append(('{"username":"' + user['Username'] + '", "partner":"' + user['Partner'] + '"}', user_table))
			elif user['Confirmed'] == False:
				form.not_partner_admins.choices.append(('{"username":"' + user['Username'] + '", "partner":"' + user['Partner'] + '"}', user_table))
		return form

class PasswordResetRequestForm(FlaskForm):
	email = StringField('Email Address:', [InputRequired(), Email(), Length(min=1, 
		max=254, message='Maximum 100 characters')])

class PasswordResetForm(FlaskForm):
	password = PasswordField('New Password:', [InputRequired(), Length(min=8, max=100, message='Passwords must be at least 8 characters'), safe_password_check])

class LoginForm(FlaskForm):
	username = StringField('Username:', [InputRequired()])
	password = PasswordField('Password:', [InputRequired()])

#Location (used on multiple pages)
#NB:
#all wtforms fields require names (e.g. 'Country text input', even though they are not displayed) 
#for validation to work when multiple forms are on a page

class LocationForm(FlaskForm):
	id = "location_form"
	country = SelectField('Country')
	region = SelectField('Region')
	farm = SelectField('Farm')
	plot = SelectField('Plot')	
	block = SelectField('Block',  [Optional()])
	@staticmethod
	def update(optional = False):
		form = LocationForm()
		COUNTRIES = sorted(set(Lists('Country').create_list_tup('name','name')), key=lambda tup: tup[1])
		REGIONS = sorted(set(Lists('Country').get_connected('name', form.country.data, 'IS_IN')), key=lambda tup: tup[1])
		FARMS = sorted(set(Fields(form.country.data).get_farms(form.region.data)), key=lambda tup: tup[1])
		PLOTS = sorted(set(Fields(form.country.data).get_plots_tup(form.region.data, form.farm.data)), key=lambda tup: tup[1])
		BLOCKS = sorted(set(Fields.get_blocks_tup(form.plot.data)), key=lambda tup: tup[1])
		form.country.choices = [('','Select Country')] + COUNTRIES
		form.region.choices = [('','Select Region')] + [(REGIONS[i], REGIONS[i]) for i, items in enumerate(REGIONS)]
		form.farm.choices = [('','Select Farm')] + [(FARMS[i], FARMS[i]) for i, items in enumerate(FARMS)]
		form.plot.choices = [('','Select Plot')] +  PLOTS
		form.block.choices = [('','Select Block')] + BLOCKS
		if optional == False:
			form.country.validators = [InputRequired()]
			form.region.validators = [InputRequired()]
			form.farm.validators = [InputRequired()]
			form.plot.validators = [InputRequired()]
		elif optional == True:
			form.country.validators = [Optional()]
			form.region.validators = [Optional()]
			form.farm.validators = [Optional()]
			form.plot.validators = [Optional()]
		return form

class AddCountry(FlaskForm):
	id = "add_country"
	strip_filter = lambda x: x.strip() if x else None
	text_country = StringField('Country text input', 
		[InputRequired(),
		Regexp('([^\x00-\x7F]|\w)+', message='Country name contains illegal characters'),
		Length(min=1, max=50, message='Maximum 50 characters')],
		filters=[strip_filter],
		description = "Add new country")
	submit_country = SubmitField('+')

class AddRegion(FlaskForm):
	id = "add_region"
	strip_filter = lambda x: x.strip() if x else None
	text_region = StringField('Region text input', 
		[InputRequired(),
		Regexp('([^\x00-\x7F]|\w)+', message='Region name contains illegal characters'),
		Length(min=1, max=50, message='Maximum 50 characters')],
		filters=[strip_filter],
		description = "Add new region")
	submit_region = SubmitField('+')

class AddFarm(FlaskForm):
	id = "add_farm"
	strip_filter = lambda x: x.strip() if x else None
	text_farm = StringField('Farm text input', 
		[InputRequired(),
		Regexp('([^\x00-\x7F]|\w)+', message='Farm name contains illegal characters'),
		Length(min=1, max=50, message='Maximum 50 characters')],
		filters=[strip_filter],
		description = "Add new farm")
	submit_farm = SubmitField('+')

class AddPlot(FlaskForm):
	id = "add_plot"
	strip_filter = lambda x: x.strip() if x else None
	text_plot = StringField('Plot text input',
		[InputRequired(),
		Regexp('([^\x00-\x7F]|\w)+', message='Plot name contains illegal characters'),
		Length(min=1, max=50, message='Maximum 50 characters')],
		filters=[strip_filter],
		description = "Add new plot")
	submit_plot = SubmitField('+')

#Plots
class PlotsForm(FlaskForm): #details of plot
	id = "plots_csv_form"
	email_checkbox = BooleanField('Email checkbox')
	generate_plots_csv = SubmitField('Generate plots.csv')

#Blocks
class AddBlock(FlaskForm):
	id = "add_block"
	strip_filter = lambda x: x.strip() if x else None
	text_block = StringField('Block text input',
		[InputRequired(),
		Regexp('([^\x00-\x7F]|\w)+', message='Block name contains illegal characters'),
		Length(min=1, max=50, message='Maximum 50 characters')],
		filters=[strip_filter],
		description = "Add new block")
	submit_block = SubmitField('+')

class BlocksForm(FlaskForm): #details of plot
	id = "blocks_csv_form"
	email_checkbox = BooleanField('Email checkbox')
	generate_blocks_csv = SubmitField('Generate blocks.csv')

#Trees
class AddTreesForm(FlaskForm):
	id = "add_trees"
	email_checkbox_add = BooleanField('Email checkbox add')
	count = IntegerField('Number of trees: ',[InputRequired(), 
		NumberRange(min=1, max=10000, message='Register up to 10000 trees at a time')],
		description= "Number of new trees")
	submit_trees = SubmitField('Register new trees')

class CustomTreesForm(FlaskForm):
	id = "custom_trees_Form"
	email_checkbox_custom = BooleanField('Email checkbox custom')
	trees_start = 	IntegerField('Start TreeID',[InputRequired(), 
		NumberRange(min=1, max=1000000, message='')],
		description= "Start TreeID")
	trees_end = IntegerField('End TreeID',[InputRequired(), 
		NumberRange(min=1, max=1000000, message='')],
		description= "End TreeID")
	custom_trees_csv = SubmitField('Custom trees.csv')

#Samples
class AddTissueForm(FlaskForm):
	id = "add_tissue_form"
	strip_filter = lambda x: x.strip() if x else None
	text_tissue = StringField('Tissue type text',	
		[InputRequired(), 
		Regexp('([^\x00-\x7F]|\w)+', message='Tissue name contains illegal characters'),
		Length(min=1, max=100, message='Maximum 100 characters')],
		filters=[strip_filter],
		description = "Add new tissue")
	submit_tissue = SubmitField('+')

class AddStorageForm(FlaskForm):
	id = "add_storage_form"
	strip_filter = lambda x: x.strip() if x else None
	text_storage = StringField('Storage type text',	
		[InputRequired(), 
		Regexp('([^\x00-\x7F]|\w)+', message='Storage name contains illegal characters'),
		Length(min=1, max=100, message='Maximum 100 characters')],
		filters=[strip_filter],
		description = "Add new storage method")
	submit_storage = SubmitField('+')

class SampleRegForm(FlaskForm):
	id = "sample_reg_form"
	email_checkbox = BooleanField('Email checkbox')
	trees_start = 	IntegerField('Start TreeID',
		[NumberRange(min=1, max=1000000, message='Maximum tree ID is 1000000')],
		description= "Start TreeID")
	trees_end = IntegerField('End TreeID',
		[NumberRange(min=1, max=1000000, message='Maximum tree ID is 1000000')],
		description= "End TreeID")
	replicates = IntegerField('Replicates', 
		[NumberRange(min=1, max=100, message='Maximum of 100 replicates per submission')],
		description= "Replicates")
	tissue = SelectField('Tissue: ')
	storage = SelectField('Storage: ')
	date_collected = DateField('Date collected (YYYY-mm-dd): ', 
		format='%Y-%m-%d',
		description= 'Date collected (YYYY-mm-dd)')
	submit_samples = SubmitField('Register samples')
	@staticmethod
	def update(optional = False):
		form = SampleRegForm()
		TISSUES = sorted(set(Lists('Tissue').create_list_tup('name', 'name')), key=lambda tup: tup[1])
		STORAGE_TYPES = sorted(set(Lists('Storage').create_list_tup('name', 'name')), key=lambda tup: tup[1])
		form.tissue.choices = [('','Select Tissue')] + TISSUES
		form.storage.choices = [('','Select Storage')] + STORAGE_TYPES
		if optional == False:
			form.trees_start.validators.append(InputRequired())
			form.trees_end.validators.append(InputRequired())
			form.replicates.validators.append(InputRequired())
			form.tissue.validators.append(InputRequired())
			form.storage.validators.append(InputRequired())
			form.date_collected.validators.append(InputRequired())
		else:
			form.trees_start.validators.insert(0,Optional())
			form.trees_end.validators.insert(0,Optional())
			form.replicates.validators.insert(0,Optional())
			form.tissue.validators.insert(0,Optional())
			form.storage.validators.insert(0,Optional())
			form.date_collected.validators.insert(0,Optional())
		return form

class CustomSampleForm(FlaskForm):
	id = "custom_sample_Form"
	email_checkbox_custom = BooleanField('Email checkbox custom')
	samples_start = IntegerField('Start SampleID', [Optional(), 
		NumberRange(min=1, max=1000000000, message='')],
		description= "Start SampleID")
	samples_end = IntegerField('End SampleID', [Optional(), 
		NumberRange(min=1, max=1000000000, message='')],
		description= "End SampleID")
	date_from = DateField('Date start (YYYY-mm-dd): ',  [Optional()],
		format='%Y-%m-%d',
		description = 'Start date')
	date_to = DateField('Date end (YYYY-mm-dd): ', [Optional()],
		format='%Y-%m-%d',
		description = 'End date')
	make_samples_csv = SubmitField('Custom samples.csv')


#upload
class UploadForm(FlaskForm):
	id = "upload_form"
	submission_type =  SelectField('Submission type:', [InputRequired()], 
		choices = sorted([('FB','Field Book Database')], key=lambda tup: tup[1]))
	file = FileField('Select a file:', [FileRequired()])
	upload_submit = SubmitField('Upload')

#download
class DownloadForm(FlaskForm):
	trait_level = SelectField('Trait level', [InputRequired()],
		choices = [('','Select Level'),('sample','Sample'),('tree','Tree'),('block','Block'),('plot','Plot')])
	date_from = DateField('Date start (YYYY-mm-dd): ',  [Optional()],
		format='%Y-%m-%d',
		description = 'Start date')
	date_to = DateField('Date end (YYYY-mm-dd): ', [Optional()],
		format='%Y-%m-%d',
		description = 'End date')
	data_format = SelectField('Data format', [InputRequired()],
		choices = [('', 'Select Format'),('db','Database'),('table','Table')])
	submit_download = SubmitField('Generate file')

#traits
class CreateTraits(FlaskForm):
	email_checkbox = BooleanField('Email checkbox')
	def __init__(self, *args, **kwargs):
		super(CreateTraits, self).__init__(*args, **kwargs)
		#the updates are methods so that flask can load while the database is unavailable
		#list the basic levels of trait
		levels = ['sample','tree','block','plot']
		#create an empty nested dictionary (level:group:[traits])
		self.levels_groups_traits = defaultdict(lambda: defaultdict(list))
		#fill this dictionary 
		for level in levels:
			if level == 'sample':
				node_label = 'SampleTrait'
			elif level == 'tree':
				node_label = 'TreeTrait'
			elif level == 'block':
				node_label = 'BlockTrait'
			elif level == 'plot':
				node_label = 'PlotTrait'
			#get a list of dictionaries of properties from each trait node at this level
			traits = Lists(node_label).get_nodes()
			#merge this into our nested defaultdict[level] with group as key and list of traits as value
			for trait in traits:
				self.levels_groups_traits[level][trait['group']].append((trait['name'], trait['details']))
			#and create empty attributes for each group
			for group in self.levels_groups_traits[level]:
				setattr(CreateTraits, group, SelectMultipleField(group,
					option_widget = widgets.CheckboxInput(),
					widget = widgets.ListWidget(prefix_label=False)))
	def update(self, level):
		#create a form instance
		form = CreateTraits(prefix = level)
		#need to replacce the prefix that FlaskForm generates with nothing for matching to dictionary, generate the string here to match for the replace
		prefix = level + '-'
		#give it a relevant ID
		id = level + "_traits_form"
		#dynamically add the group choices to the form instance
		for field in form:
			#the recursive part here is just to add the prefix to the item to compare against the fieldnames
			if field.name[len(prefix):] in self.levels_groups_traits[level]:
				field.choices = self.levels_groups_traits[level][field.name[len(prefix):]]
		return form

