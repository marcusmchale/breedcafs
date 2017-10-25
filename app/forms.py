from wtforms import StringField, PasswordField, BooleanField, SelectField, SelectMultipleField, IntegerField, SubmitField, DateField, DateTimeField, widgets
from wtforms.validators import InputRequired, Optional, Email, EqualTo, NumberRange, Length, Regexp
from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileRequired
from app import app
from app.models import Lists, Fields
from collections import defaultdict

#user 
class RegistrationForm(FlaskForm):
	strip_filter = lambda x: x.strip() if x else None
	PARTNERS = Lists('Partner').create_list_tup('name', 'fullname')
	partner = SelectField('Partner:', [InputRequired()], 
		choices = sorted(tuple(PARTNERS), key=lambda tup: tup[1]))
	username = StringField('Username:', [InputRequired(), 
		Length(min=1, max=20, message='Maximum 20 characters'),
		Regexp('([^\x00-\x7F]|\w)+', message='Username contains illegal characters')],
		filters=[strip_filter],
		description = "Enter a username")
	email = StringField('Email Address:', [InputRequired(), Email(), Length(min=1, 
		max=100, message='Maximum 100 characters')],
		filters = [strip_filter],
		description = "Enter your email address")
	name = StringField('Full Name:', [InputRequired(), Length(min=1, 
		max=100, message='Maximum 100 characters')],
		filters=[strip_filter],
		description = "Enter your full name")
	password = PasswordField('New Password:', [InputRequired(), Length(min=6, max=100,
		message='Passwords must be at least 6 characters')],
		description = "Please choose a secure password for this site")

class PasswordResetRequestForm(FlaskForm):
	email = StringField('Email Address:', [InputRequired(), Email(), Length(min=1, 
		max=100, message='Maximum 100 characters')])

class PasswordResetForm(FlaskForm):
	password = PasswordField('New Password:', [InputRequired(), EqualTo('confirm', 
		message='Passwords must match'), Length(min=1, max=100, message='Maximum 100 characters')])
	confirm = PasswordField('Repeat Password:')

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
		else:
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

#Fields/Plots
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

class FieldsForm(FlaskForm): #details of plot
	id = "plots_csv_form"
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

#Traits
class CreateBlockTraits(FlaskForm):
	id = "block_traits_form"
	TRAITS = Lists('BlockTrait').get_nodes()
	trait_dict = defaultdict(list)
	for trait in TRAITS:
		trait_dict[trait['group']].append((trait['name'], trait['details']))
	email_checkbox = BooleanField('Email checkbox')
	block_general = SelectMultipleField('general',
		choices = sorted(trait_dict['general'], 
		key=lambda tup: tup[1]), 
		default= ['location'],
		option_widget=widgets.CheckboxInput(),
		widget=widgets.ListWidget(prefix_label=False)
		)
	block_agronomic = SelectMultipleField('agronomic',
		choices = sorted(trait_dict['agronomic'], key=lambda tup: tup[1]), 
		option_widget=widgets.CheckboxInput(),
		widget=widgets.ListWidget(prefix_label=False)
		)

class CreateTreeTraits(FlaskForm):
	id = "tree_traits_form"
	TRAITS = Lists('TreeTrait').get_nodes()
	trait_dict = defaultdict(list)
	for trait in TRAITS:
		trait_dict[trait['group']].append((trait['name'], trait['details']))
	email_checkbox = BooleanField('Email checkbox')
	general = SelectMultipleField('general',
		choices = sorted(trait_dict['general'], 
		key=lambda tup: tup[1]), 
		default= ['location','variety','hybrid_parent1','hybrid_parent2','date'],
		option_widget=widgets.CheckboxInput(),
		widget=widgets.ListWidget(prefix_label=False)
		)
	agronomic = SelectMultipleField('agronomic',
		choices = sorted(trait_dict['agronomic'], key=lambda tup: tup[1]), 
		option_widget=widgets.CheckboxInput(),
		widget=widgets.ListWidget(prefix_label=False)
		)	
	morphological = SelectMultipleField('morphological',
		choices = sorted(trait_dict['morphological'], key=lambda tup: tup[1]), 
		option_widget=widgets.CheckboxInput(),
		widget=widgets.ListWidget(prefix_label=False)
		)	
	photosynthetic = SelectMultipleField('photosynthetic',
		choices = sorted(trait_dict['photosynthetic'], key=lambda tup: tup[1]), 
		option_widget=widgets.CheckboxInput(),
		widget=widgets.ListWidget(prefix_label=False)
		)
	metabolomic = SelectMultipleField('metabolomic',
		choices = sorted(trait_dict['metabolomic'], key=lambda tup: tup[1]), 
		option_widget=widgets.CheckboxInput(),
		widget=widgets.ListWidget(prefix_label=False)
		)	

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
		choices = [('','Select Level'),('block','Block'), ('tree','Tree')])
	date_from = DateField('Date start (YYYY-mm-dd): ',  [Optional()],
		format='%Y-%m-%d',
		description = 'Start date')
	date_to = DateField('Date end (YYYY-mm-dd): ', [Optional()],
		format='%Y-%m-%d',
		description = 'End date')
	data_format = SelectField('Data format', [InputRequired()],
		choices = [('', 'Select Format'),('db','Database'),('table','Table')])
	submit_download = SubmitField('Generate file')
