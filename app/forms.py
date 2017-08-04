from wtforms import StringField, PasswordField, SelectField, SelectMultipleField, IntegerField, SubmitField, widgets
from wtforms.validators import DataRequired, Email, EqualTo
from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileRequired
from app import app
from app.models import Lists, Fields

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

class RegisterFields(FlaskForm):
	country = SelectField('Country:', [DataRequired()])
	region = SelectField('Region:', [DataRequired()])
	farm = SelectField('Farm:', [DataRequired()])
	plot = SelectField('Plot:', [DataRequired()])
	count = IntegerField('Count:',[DataRequired()])
	@staticmethod
	def update():
		form = RegisterFields()
		COUNTRIES = sorted(set(Lists('Country').create_list('name','name')), key=lambda tup: tup[1])
		REGIONS = sorted(set(Lists('Country').get_connected('name', form.country.data, 'IS_IN')), key=lambda tup: tup[1])
		FARMS = sorted(set(Fields(form.country.data).get_farms(form.region.data)), key=lambda tup: tup[1])
		PLOTS = sorted(set(Fields(form.country.data).get_plots(form.region.data, form.farm.data)), key=lambda tup: tup[1])
		empty = [('','')]
		form.country.choices = empty + COUNTRIES
		form.region.choices = empty + REGIONS
		form.farm.choices = empty + FARMS
		form.plot.choices = empty + PLOTS
		return form

class AddCountry(FlaskForm):
		text = StringField()
		submit = SubmitField('+')
class AddRegion(FlaskForm):
		text = StringField()
		submit = SubmitField('+')
class AddFarm(FlaskForm):
		text = StringField()
		submit = SubmitField('+')
class AddPlot(FlaskForm):
		text = StringField()
		submit = SubmitField('+')
#class CreateFields(FlaskForm):
#	country = SelectField('Country:', [DataRequired()])
#	region = SelectField('Region:', [DataRequired()])
#	farm = SelectField('Farm:', [DataRequired()])
#	plot = SelectField('Plot:', [DataRequired()])
#	@staticmethod
#	def update():
#		form = CreateFields()
#		COUNTRIES = sorted(set(Lists('Country').create_list('name','name')), key=lambda tup: tup[1])
#		REGIONS = sorted(set(Lists('Country').get_connected('name', form.country.data, 'IS_IN')), key=lambda tup: tup[1])
#		FARMS = sorted(set(Fields(form.country.data).get_farms(form.region.data)), key=lambda tup: tup[1])
#		PLOTS = sorted(set(Fields(form.country.data).get_plots(form.region.data, form.farm.data)), key=lambda tup: tup[1])
#		empty = [('','')]
#		form.country.choices = empty + COUNTRIES
#		form.region.choices = empty + REGIONS
#		form.farm.choices = empty + FARMS
#		form.plot.choices = empty + PLOTS
#		return form

