from app import app
from flask import session, flash, request, redirect, url_for, render_template, send_file
from app.models import Lists, Fields
from app.forms import CreateTraits, RegisterFields, AddCountry, AddRegion, AddFarm, AddPlot

@app.route('/create', methods=['GET'])
def create():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		return render_template('create.html', title='Create')

@app.route('/register_fields', methods=['GET', 'POST'])
def register_fields():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		form = RegisterFields.update()
		add_country = AddCountry(prefix='add_country')
		add_region = AddRegion(prefix='add_region')
		add_farm = AddFarm(prefix='add_farm')
		add_plot = AddPlot(prefix='add_plot')
		if request.method == 'POST':
			if add_country.submit.data:
				if Fields(add_country.text.data, session.username).find_country():
					flash('Country already found: ' + add_country.text.data)
				else: 
					Fields(add_country.text.data).add_country()
					flash('Country submitted: ' + add_country.text.data)
			if add_region.submit.data:
				if not form.country.data:
					flash('Please select a country to register a new region')
				elif Fields(form.country.data).find_region(add_region.text.data):
					flash('Region already found: ' + add_region.text.data + ' in ' + form.country.data)
				else:
					Fields(form.country.data).add_region(add_region.text.data)
					flash('Region submitted: ' + add_region.text.data + ' in ' + form.country.data)
			if add_farm.submit.data:
				if not form.country.data and form.region.data:
					flash ('Please select a country and region to register a new farm')
				elif Fields(form.country.data).find_farm(form.region.data, add_farm.text.data):
					flash('Farm already found: ' + add_farm.text.data + ' in ' 
						+ form.region.data + ' of ' + form.country.data )
				else:
					Fields(form.country.data).add_farm(form.region.data, add_farm.text.data)
					flash('Farm submitted: ' + add_farm.text.data + ' in ' 
						+ form.region.data + ' of ' + form.country.data )
			if add_plot.submit.data:
				if not form.country.data and form.region.data and form.farm.data:
					flash ('Please select a country, region and farm to register a new plot')
				elif Fields(form.country.data).find_plot(form.region.data, form.farm.data, add_plot.text.data):
					flash('Plot already found: ' + add_plot.text.data + ' in ' 
						+ form.farm.data + ' of ' + form.region.data + ' of ' + form.country.data )
				else:
					Fields(form.country.data).add_plot(form.region.data, form.farm.data, add_plot.text.data)
					flash('Farm submitted: ' + add_plot.text.data + ' in ' 
						+ form.farm.data + ' of ' + form.region.data + ' of ' + form.country.data )
			if form.submit.data and form.validate_on_submit():
				if not form.country.data  and form.region.data and form.farm.data and form.plot.data and form.count.data:
					flash ('Please select country, region, farm, plot from the dropdown boxes and enter the number of trees to register')
				else:
					csv=Fields(form.country.data).add_trees(form.region.data, form.farm.data, form.plot.data, form.count.data)
					return send_file(csv,
						attachment_filename='BreedCAFS_fields.csv', 
						as_attachment=True,
						mimetype=('txt/csv'))
					flash('Trees registered and fields.csv created - please save this file!')
	return render_template('register_fields.html', form=form, add_country=add_country, 
		add_region=add_region, add_farm=add_farm, add_plot=add_plot, title='Register fields.csv')

@app.route('/create_trt', methods=['GET', 'POST'])
def create_trt():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		form = CreateTraits()
		if request.method == 'POST' and form.validate_on_submit():
			selection = form.traits.data
			trt=Lists('Trait').create_trt(selection, 'name')
			return send_file(trt,
				attachment_filename='BreedCAFS_traits.trt', 
				as_attachment=True,
				mimetype=('txt/csv'))
			flash('Generated traits.trt')
	return render_template('create_trt.html', form=form, title='Create traits.trt')
