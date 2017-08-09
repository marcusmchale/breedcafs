from wtforms import StringField, PasswordField, SelectField, SelectMultipleField, IntegerField, SubmitField, widgets
from wtforms.validators import InputRequired, Email, EqualTo, NumberRange, Length
from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileRequired
from app import app
from app.models import Lists, Fields

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

class LoginForm(FlaskForm):
	username = StringField('Username:', [InputRequired()])
	password = PasswordField('Password:', [InputRequired()])

class UploadForm(FlaskForm):
	submission_type =  SelectField('Submission type:', [InputRequired()], 
		choices = sorted(app.config['SUBMISSION_TYPES'], key=lambda tup: tup[1]))
	file = FileField('Select a file:', [FileRequired()])

class CreateTraits(FlaskForm):
	TRAITS = Lists('Trait').create_list('name','details')
	traits = SelectMultipleField('Trait:', [InputRequired()], 
		choices = sorted(TRAITS, key=lambda tup: tup[1]), 
		option_widget=widgets.CheckboxInput(),
		widget=widgets.ListWidget(prefix_label=False)
		)

class RegisterTrees(FlaskForm):
	country = SelectField('Country:', [InputRequired()])
	region = SelectField('Region:', [InputRequired()])
	farm = SelectField('Farm:', [InputRequired()])
	plot = SelectField('Plot:', [InputRequired()])
	count = IntegerField('Number of trees:',[InputRequired(), NumberRange(min=1, 
		max=1000, message='At most 1000 plants may be registered per plot')])
	submit_trees = SubmitField('Generate fields.csv')
	@staticmethod
	def update():
		form = RegisterTrees()
		COUNTRIES = sorted(set(Lists('Country').create_list('name','name')), key=lambda tup: tup[1])
		REGIONS = sorted(set(Lists('Country').get_connected('name', form.country.data, 'IS_IN')), key=lambda tup: tup[1])
		FARMS = sorted(set(Fields(form.country.data).get_farms(form.region.data)), key=lambda tup: tup[1])
		PLOTS = sorted(set(Fields(form.country.data).get_plots(form.region.data, form.farm.data)), key=lambda tup: tup[1])
		empty = [('','')]
		form.country.choices = empty + COUNTRIES
		form.region.choices = REGIONS
		form.farm.choices = FARMS
		form.plot.choices = PLOTS
		return form

#these fields all require names (e.g. 'Country text input', even though they are not displayed) for validation to work
class AddCountry(FlaskForm):
		strip_filter = lambda x: x.strip() if x else None
		text_country = StringField('Country text input', [InputRequired(),Length(min=1, max=50, message='Maximum 50 characters')],
			filters=[strip_filter])
		submit_country = SubmitField('Add country')
class AddRegion(FlaskForm):
		strip_filter = lambda x: x.strip() if x else None
		text_region = StringField('Region text input', [InputRequired(),Length(min=1, max=50, message='Maximum 50 characters')],
			filters=[strip_filter])
		submit_region = SubmitField('Add region')
class AddFarm(FlaskForm):
		strip_filter = lambda x: x.strip() if x else None
		text_farm = StringField('Farm text input', [InputRequired(),Length(min=1, max=50, message='Maximum 50 characters')],
			filters=[strip_filter])
		submit_farm = SubmitField('Add farm')
class AddPlot(FlaskForm):
		strip_filter = lambda x: x.strip() if x else None
		text_plot = StringField('Plot text input', [InputRequired(),Length(min=1, max=50, message='Maximum 50 characters')],
			filters=[strip_filter])
		submit_plot = SubmitField('Add plot')
