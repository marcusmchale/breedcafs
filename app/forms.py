from wtforms import StringField, PasswordField, SelectField, SelectMultipleField, IntegerField, SubmitField, DateField, DateTimeField, widgets
from wtforms.validators import InputRequired, Email, EqualTo, NumberRange, Length
from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileRequired
from app import app
from app.models import Lists, Fields
from collections import defaultdict

#user 
class RegistrationForm(FlaskForm):
	strip_filter = lambda x: x.strip() if x else None
	PARTNERS = Lists('Partner').create_list('name', 'fullname')
	partner = SelectField('Partner:', [InputRequired()], 
		choices = sorted(tuple(PARTNERS), key=lambda tup: tup[1]))
	username = StringField('Username:', [InputRequired(), Length(min=1, 
		max=20, message='Maximum 20 characters')],
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
	country = SelectField('Country: ', [InputRequired()])
	region = SelectField('Region: ', [InputRequired()])
	farm = SelectField('Farm: ', [InputRequired()])
	plot = SelectField('Plot: ', [InputRequired()])	
	@staticmethod
	def update():
		form = LocationForm()
		COUNTRIES = sorted(set(Lists('Country').create_list('name','name')), key=lambda tup: tup[1])
		REGIONS = sorted(set(Lists('Country').get_connected('name', form.country.data, 'IS_IN')), key=lambda tup: tup[1])
		FARMS = sorted(set(Fields(form.country.data).get_farms(form.region.data)), key=lambda tup: tup[1])
		PLOTS = sorted(set(Fields(form.country.data).get_plots(form.region.data, form.farm.data)), key=lambda tup: tup[1])
		form.country.choices = [('','Select Country')] + COUNTRIES
		form.region.choices = [('','Select Region')] + REGIONS
		form.farm.choices = [('','Select Farm')] + FARMS
		form.plot.choices = [('','Select Plot')] + PLOTS
		return form

class AddCountry(FlaskForm):
	id = "add_country"
	strip_filter = lambda x: x.strip() if x else None
	text_country = StringField('Country text input', 
		[InputRequired(),Length(min=1, max=50, message='Maximum 50 characters')],
		filters=[strip_filter],
		description = "Add new country")
	submit_country = SubmitField('+')

class AddRegion(FlaskForm):
	id = "add_region"
	strip_filter = lambda x: x.strip() if x else None
	text_region = StringField('Region text input', 
		[InputRequired(),Length(min=1, max=50, message='Maximum 50 characters')],
		filters=[strip_filter],
		description = "Add new region")
	submit_region = SubmitField('+')

class AddFarm(FlaskForm):
	id = "add_farm"
	strip_filter = lambda x: x.strip() if x else None
	text_farm = StringField('Farm text input', 
		[InputRequired(),Length(min=1, max=50, message='Maximum 50 characters')],
		filters=[strip_filter],
		description = "Add new farm")
	submit_farm = SubmitField('+')

class AddPlot(FlaskForm):
	id = "add_plot"
	strip_filter = lambda x: x.strip() if x else None
	text_plot = StringField('Plot text input',
		[InputRequired(),Length(min=1, max=50, message='Maximum 50 characters')],
		filters=[strip_filter],
		description = "Add new plot")
	submit_plot = SubmitField('+')

#Fields
class FieldsForm(FlaskForm): #details of plot
	id = "fields_form"
	strip_filter = lambda x: x.strip() if x else None
	soil = SelectField('soil type')
	shade_trees = SelectMultipleField('Select shade trees:', option_widget=widgets.CheckboxInput(), widget=widgets.ListWidget(prefix_label=False))
	submit_field_details = SubmitField('submit field details')
	@staticmethod
	def update():
		form = FieldsForm()
		SOIL_TYPES = sorted(set(Lists('Soil').create_list('name','name')), key=lambda tup: tup[1])
		SHADE_TREES = sorted(set(Lists('ShadeTree').create_list('name','name')), key=lambda tup: tup[1])
		form.soil.choices = [('','Select Soil Type')] + SOIL_TYPES
		form.shade_trees.choices = SHADE_TREES
		return form

class AddSoilForm(FlaskForm):
	id = "add_soil_form"
	strip_filter = lambda x: x.strip() if x else None
	text_soil = StringField('Soil text input',
		[InputRequired(),Length(min=1, max=50, message='Maximum 50 characters')],
		filters=[strip_filter],
		description = "Add new soil type")
	submit_soil = SubmitField('+')

class AddShadeTreeForm(FlaskForm):
	id = "add_shade_tree_form"
	strip_filter = lambda x: x.strip() if x else None
	text_shade_tree = StringField('Shade Tree text input',
		[InputRequired(),Length(min=1, max=50, message='Maximum 50 characters')],
		filters=[strip_filter],
		description = "Add new shade tree variety")
	submit_shade_tree = SubmitField('+')

#Trees
class AddTrees(FlaskForm):
	id = "add_trees"
	count = IntegerField('Number of trees: ',[InputRequired(), 
		NumberRange(min=1, max=1000, message='Register from 1-1000 plants at a time')],
		description= "Number of new trees")
	submit_trees = SubmitField('Register new trees')

class CustomTreesForm(FlaskForm):
	id = "custom_trees_Form"
	trees_start = 	IntegerField('Start TreeID',[InputRequired(), 
		NumberRange(min=1, max=100000, message='')],
		description= "Start TreeID")
	trees_end = IntegerField('End TreeID',[InputRequired(), 
		NumberRange(min=1, max=100000, message='')],
		description= "End TreeID")
	submit_fields = SubmitField('Custom Fields.csv')

#Traits
class CreateTraits(FlaskForm):
	id = "traits_form"
	TRAITS = Lists('Trait').get_nodes()
	trait_dict = defaultdict(list)
	for trait in TRAITS:
		trait_dict[trait['group']].append((trait['name'], trait['details']))
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
	# the below didn't work so repeated myself above - should look into programatic field generation
	#for group in trait_dict:
	#	group = SelectMultipleField(group, [InputRequired()], 
	#		choices = sorted(trait_dict[group], key=lambda tup: tup[1]), 
	#		option_widget=widgets.CheckboxInput(),
	#		widget=widgets.ListWidget(prefix_label=False)
	#		)

#Samples

class AddTissueForm(FlaskForm):
	id = "add_tissue_form"
	strip_filter = lambda x: x.strip() if x else None
	text_tissue = StringField('Tissue type text',	
		[InputRequired(), Length(min=1, max=100, message='Maximum 100 characters')],
		filters=[strip_filter],
		description = "Add new tissue")
	submit_tissue = SubmitField('+')

class AddStorageForm(FlaskForm):
	id = "add_storage_form"
	strip_filter = lambda x: x.strip() if x else None
	text_storage = StringField('Storage type text',	
		[InputRequired(), Length(min=1, max=100, message='Maximum 100 characters')],
		filters=[strip_filter],
		description = "Add new storage method")
	submit_storage = SubmitField('+')

class SampleRegForm(FlaskForm):
	id = "sample_reg_form"
	country = SelectField('Country: ', [InputRequired()])
	region = SelectField('Region: ', [InputRequired()])
	farm = SelectField('Farm: ', [InputRequired()])
	plot = SelectField('Plot: ', [InputRequired()])
	trees_start = 	IntegerField('Start TreeID',[InputRequired(), 
		NumberRange(min=1, max=100000, message='')],
		description= "Start TreeID")
	trees_end = IntegerField('End TreeID',[InputRequired(), 
		NumberRange(min=1, max=100000, message='')],
		description= "End TreeID")
	tissue = SelectField('Tissue: ', [InputRequired()])
	storage = SelectField('Tissue: ', [InputRequired()])
	date_collected = DateField('Date collected (YYYY-mm-dd): ', [InputRequired()], format='%Y-%m-%d')
	submit_samples = SubmitField('Register samples')
	@staticmethod
	def update():
		form = SampleRegForm()
		COUNTRIES = sorted(set(Lists('Country').create_list('name','name')), key=lambda tup: tup[1])
		REGIONS = sorted(set(Lists('Country').get_connected('name', form.country.data, 'IS_IN')), key=lambda tup: tup[1])
		FARMS = sorted(set(Fields(form.country.data).get_farms(form.region.data)), key=lambda tup: tup[1])
		PLOTS = sorted(set(Fields(form.country.data).get_plots(form.region.data, form.farm.data)), key=lambda tup: tup[1])
		TISSUES = sorted(set(Lists('Tissue').create_list('name', 'name')), key=lambda tup: tup[1])
		STORAGE_TYPES = sorted(set(Lists('Storage').create_list('name', 'name')), key=lambda tup: tup[1])
		form.country.choices = [('','Select Country')] + COUNTRIES
		form.region.choices = [('','Select Region')] + REGIONS
		form.farm.choices = [('','Select Farm')] + FARMS
		form.plot.choices = [('','Select Plot')] + PLOTS
		form.tissue.choices = [('','Select Tissue')] + TISSUES
		form.storage.choices = [('','Select Storage')] + STORAGE_TYPES
		return form

#upload
class UploadForm(FlaskForm):
	submission_type =  SelectField('Submission type:', [InputRequired()], 
		choices = sorted(app.config['SUBMISSION_TYPES'], key=lambda tup: tup[1]))
	file = FileField('Select a file:', [FileRequired()])
