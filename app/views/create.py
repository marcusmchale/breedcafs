from app import app
from flask import session, flash, request, redirect, url_for, render_template, send_file
from app.models import Lists, Fields, User
from app.forms import CreateTraits, RegisterTrees, AddCountry, AddRegion, AddFarm, AddPlot
from app.emails import send_attachment

@app.route('/create', methods=['GET'])
def create():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		return render_template('create.html', title='Create')

@app.route('/register_trees', methods=['GET', 'POST'])
def register_trees():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		register_trees = RegisterTrees().update()
		country = register_trees.country.data
		region = register_trees.region.data
		farm = register_trees.farm.data
		plot = register_trees.plot.data
		count = register_trees.count.data
		submit_trees = register_trees.submit_trees.data
		add_country = AddCountry()
		add_region = AddRegion()
		add_farm = AddFarm()
		add_plot = AddPlot()
		if add_country.submit_country.data and add_country.validate_on_submit():
			if Fields(add_country.text_country.data).find_country():
				flash('Country already found: ' + add_country.text_country.data)
			else: 
				Fields(add_country.text_country.data).add_country()
				flash('Country submitted: ' + add_country.text_country.data)
		if add_region.submit_region.data and add_region.validate_on_submit():
			if not country:
				flash('Please select a country to register a new region')
			elif Fields(country).find_region(add_region.text_region.data):
				flash('Region already found: ' + add_region.text_region.data + ' in ' + country)
			else:
				Fields(country).add_region(add_region.text_region.data)
				flash('Region submitted: ' + add_region.text_region.data + ' in ' + country)
		if add_farm.submit_farm.data and add_farm.validate_on_submit():
			if not country or region=='None':
				flash ('Please select a country and region to register a new farm')
			elif Fields(country).find_farm(region, add_farm.text_farm.data):
				flash('Farm already found: ' + add_farm.text_farm.data + ' in ' 
					+ region + ' of ' + country )
			else:
				Fields(country).add_farm(region, add_farm.text_farm.data)
				flash('Farm submitted: ' + add_farm.text_farm.data + ' in ' 
					+ region + ' of ' + country )
		if add_plot.submit_plot.data and add_plot.validate_on_submit():
			if not country or any ((region=='None', farm=='None')):
				flash ('Please select a country, region and farm to register a new plot')
			elif Fields(country).find_plot(region, farm, add_plot.text_plot.data):
				flash('Plot already found: ' + add_plot.text_plot.data + ' in ' 
					+ farm + ' of ' + region + ' of ' + country )
			else:
				Fields(country).add_plot(region, farm, add_plot.text_plot.data)
				flash('Farm submitted: ' + add_plot.text_plot.data + ' in ' 
					+ farm + ' of ' + region + ' of ' + country )
		if submit_trees and register_trees.validate_on_submit():
			fields_csv=Fields(country).add_trees(region, farm, plot, count)
			#flash doesn't work since return isn't a render..need to fix with javascript
			#flash(str(count) + ' trees registered in: ' + plot + ' of '	+ 
			#	farm + ' of ' + region + ' of ' + country + '')
			recipients=[User(session['username']).find('')['email']]
			subject = "BreedCAFS: Trees registered"
			html = render_template('emails/register_trees.html', 
				count=count,
				plot=plot,
				farm=farm,
				region=region,
				country=country)
			send_attachment(subject, 
				app.config['ADMINS'][0], 
				recipients, 
				'copy of fields.csv', 
				html, 
				u'BreedCAFS_fields.csv', 
				'text/csv', 
				fields_csv)
			#flash('fields.csv has also been sent to your email address')
			return send_file(fields_csv,
				attachment_filename='BreedCAFS_fields.csv', 
				as_attachment=True,
				mimetype=('txt/csv'))
		return render_template('register_trees.html', register_trees=register_trees, add_country=add_country, 
		add_region=add_region, add_farm=add_farm, add_plot=add_plot, title='Register trees and create fields.csv')

@app.route('/create_trt', methods=['GET', 'POST'])
def create_trt():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		form = CreateTraits()
		if form.validate_on_submit():
			selection = form.traits.data
			trt=Lists('Trait').create_trt(selection, 'name')
			recipients=[User(session['username']).find('')['email']]
			subject = "BreedCAFS: traits.trt"
			html = render_template('emails/create_traits.html')
			send_attachment(subject, 
				app.config['ADMINS'][0], 
				recipients, 
				'copy of traits.trt', 
				html, 
				u'BreedCAFS_traits.trt', 
				'text/csv', 
				trt)
			return send_file(trt,
				attachment_filename='BreedCAFS_traits.trt', 
				as_attachment=True,
				mimetype=('txt/csv'))
			flash('Generated traits.trt')
	return render_template('create_trt.html', form=form, title='Create traits.trt')
