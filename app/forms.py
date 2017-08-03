from wtforms import StringField, PasswordField, SelectField, SelectMultipleField, widgets
from wtforms.validators import DataRequired, Email, EqualTo
from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileRequired
from app import app
from app.models import List

class RegistrationForm(FlaskForm):
	PARTNERS = Lists('Partner').create_list('name', 'fullname')
	partner = SelectField('Partner:', [DataRequired()], choices = sorted(tuple(PARTNERS), key=lambda tup: tup[1]))
	username = StringField('Username:', [DataRequired()])
	email = StringField('Email Address:', [DataRequired(), Email()])
	name = StringField('Full Name:', [DataRequired()])
	password = PasswordField('New Password:', [DataRequired(), EqualTo('confirm', message='Passwords must match')])
	confirm = PasswordField('Repeat Password:')

class LoginForm(FlaskForm):
	username = StringField('Username:', [DataRequired()])
	password = PasswordField('Password:', [DataRequired()])

class UploadForm(FlaskForm):
	submission_type =  SelectField('Submission type:', [DataRequired()], choices = sorted(app.config['SUBMISSION_TYPES'], key=lambda tup: tup[1]))
	file = FileField('Select a file:', [FileRequired()])

class CreateTraits(FlaskForm):
	TRAITS = Lists('Trait').create_list('name','details')
	traits = SelectMultipleField('Trait:', [DataRequired()], 
		choices = sorted(TRAITS, key=lambda tup: tup[1]), 
		option_widget=widgets.CheckboxInput(),
		widget=widgets.ListWidget(prefix_label=False)
		)
class CreateFields(FlaskForm):
	country = SelectField('Country:', [DataRequired()])
	region = SelectField('Region:', [DataRequired()])
	farm = SelectField('Farm:', [DataRequired()])
	plot = SelectField('Plot:', [DataRequired()])
	@classmethod
	def update(cls):
		form=cls()
		COUNTRIES = Lists('Country').create_list('name','name')
		REGIONS = set(Lists('Country').get_connected('name', form.country.data, 'IS_IN'))
		FARMS = set(Lists('Region').get_connected('name', form.region.data, 'IS_IN'))
		PLOTS = set(Lists('Farm').get_connected('name', form.farm.data, 'IS_IN'))
		form = CreateFields()
		form.country.choices = sorted(COUNTRIES, key=lambda tup: tup[1])
		form.region.choices = sorted(REGIONS, key=lambda tup: tup[1])
		form.farm.choices = sorted(FARMS, key=lambda tup: tup[1])
		form.plot.choices = sorted(PLOTS, key=lambda tup: tup[1])
		form.country.default =  COUNTRIES[1]
		form.region.default = REGIONS[1]
		form.farm.default = FARMS[1]
		form.plot.default = PLOTS[1]
		return form
