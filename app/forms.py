from wtforms import StringField, PasswordField, SelectField, SelectMultipleField, IntegerField, SubmitField, widgets
from wtforms.validators import InputRequired, Email, EqualTo, NumberRange, Length
from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileRequired
from app import app
from app.models import Lists, Fields
from collections import defaultdict

class RegistrationForm(FlaskForm):
	PARTNERS = Lists('Partner').create_list('name', 'fullname')
	partner = SelectField('Partner:', [InputRequired()], 
		choices = sorted(tuple(PARTNERS), key=lambda tup: tup[1]))
	username = StringField('Username:', [InputRequired(), Length(min=1, 
		max=20, message='Maximum 20 characters')])
	email = StringField('Email Address:', [InputRequired(), Email(), Length(min=1, 
		max=100, message='Maximum 100 characters')])
	name = StringField('Full Name:', [InputRequired(), Length(min=1, 
		max=100, message='Maximum 100 characters')])
	password = PasswordField('New Password:', [InputRequired(), EqualTo('confirm', 
		message='Passwords must match'), Length(min=1, max=100, message='Maximum 100 characters')])
	confirm = PasswordField('Repeat Password:')

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

class UploadForm(FlaskForm):
	submission_type =  SelectField('Submission type:', [InputRequired()], 
		choices = sorted(app.config['SUBMISSION_TYPES'], key=lambda tup: tup[1]))
	file = FileField('Select a file:', [FileRequired()])

class RegisterTrees(FlaskForm):
	id = "register_trees"
	country = SelectField('Country: ', [InputRequired()])
	region = SelectField('Region: ', [InputRequired()])
	farm = SelectField('Farm: ', [InputRequired()])
	plot = SelectField('Plot: ', [InputRequired()])
	count = IntegerField('Number of trees: ',[InputRequired(), 
		NumberRange(min=1, max=1000, message='Register from 1-1000 plants at a time')],
		description= "Number of new trees")
	submit_trees = SubmitField('Register new trees')
	@staticmethod
	def update():
		form = RegisterTrees()
		COUNTRIES = sorted(set(Lists('Country').create_list('name','name')), key=lambda tup: tup[1])
		REGIONS = sorted(set(Lists('Country').get_connected('name', form.country.data, 'IS_IN')), key=lambda tup: tup[1])
		FARMS = sorted(set(Fields(form.country.data).get_farms(form.region.data)), key=lambda tup: tup[1])
		PLOTS = sorted(set(Fields(form.country.data).get_plots(form.region.data, form.farm.data)), key=lambda tup: tup[1])
		form.country.choices = [('','Select Country')] + COUNTRIES
		form.region.choices = [('','Select Region')] + REGIONS
		form.farm.choices = [('','Select Farm')] + FARMS
		form.plot.choices = [('','Select Plot')] + PLOTS
		return form

#these fields all require names (e.g. 'Country text input', even though they are not displayed) for validation to work
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

class CreateTraits(FlaskForm):
	TRAITS = Lists('Trait').get_nodes()
	trait_dict = defaultdict(list)
	for trait in TRAITS:
		trait_dict[trait['group']].append((trait['name'], trait['details']))
	general = SelectMultipleField('general', [InputRequired()], 
		choices = sorted(trait_dict['general'], key=lambda tup: tup[1]), 
		option_widget=widgets.CheckboxInput(),
		widget=widgets.ListWidget(prefix_label=False)
		)	
	agronomic = SelectMultipleField('agronomic', [InputRequired()], 
		choices = sorted(trait_dict['agronomic'], key=lambda tup: tup[1]), 
		option_widget=widgets.CheckboxInput(),
		widget=widgets.ListWidget(prefix_label=False)
		)	
	morphological = SelectMultipleField('morphological', [InputRequired()], 
		choices = sorted(trait_dict['morphological'], key=lambda tup: tup[1]), 
		option_widget=widgets.CheckboxInput(),
		widget=widgets.ListWidget(prefix_label=False)
		)	
	photosynthetic = SelectMultipleField('photosynthetic', [InputRequired()], 
		choices = sorted(trait_dict['photosynthetic'], key=lambda tup: tup[1]), 
		option_widget=widgets.CheckboxInput(),
		widget=widgets.ListWidget(prefix_label=False)
		)
	metabolomic = SelectMultipleField('metabolomic', [InputRequired()], 
		choices = sorted(trait_dict['metabolomic'], key=lambda tup: tup[1]), 
		option_widget=widgets.CheckboxInput(),
		widget=widgets.ListWidget(prefix_label=False)
		)	
	#couldn't get the below to work so repeated myself above, 
	#complains of no attribute, i think i need to 
	#instantiate with the fields, or something 
	#for group in trait_dict:
	#	group = SelectMultipleField(group, [InputRequired()], 
	#		choices = sorted(trait_dict[group], key=lambda tup: tup[1]), 
	#		option_widget=widgets.CheckboxInput(),
	#		widget=widgets.ListWidget(prefix_label=False)
	#		)