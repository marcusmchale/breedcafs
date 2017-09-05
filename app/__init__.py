from flask import Flask

app = Flask(__name__, instance_relative_config=True)
app.config.from_object('config')
app.config.from_pyfile('config.py')

from app import views

app.add_url_rule('/locations_trees/countries/', 
	view_func=views.countries.as_view('countries'), 
	methods=['GET'])

app.add_url_rule('/locations_trees/<country>/', 
	view_func=views.regions.as_view('regions'), 
	methods=['GET'])

app.add_url_rule('/locations_trees/<country>/<region>/', 
	view_func=views.farms.as_view('farms'), 
	methods=['GET'])

app.add_url_rule('/locations_trees/<country>/<region>/<farm>/', 
	view_func=views.plots.as_view('plots'), 
	methods=['GET'])