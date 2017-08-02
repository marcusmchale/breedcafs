from app import app
from flask import session, flash, request, redirect, url_for, render_template, send_file
from app.models import List
from app.forms import CreateTraits, CreateFields

@app.route('/create', methods=['GET'])
def create():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		return render_template('create.html', title='Create')

@app.route('/create_trt', methods=['GET', 'POST'])
def create_trt():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		form = CreateTraits()
		if request.method == 'POST' and form.validate_on_submit():
			selection = form.traits.data
			trt=List('Trait').create_trt(selection, 'name')
			return send_file(trt,
				attachment_filename='BreedCAFS_traits.trt', 
				as_attachment=True,
				mimetype=('txt/csv'))
	return render_template('create_trt.html', form=form, title='Create traits.trt')

@app.route('/create_fields', methods=['GET', 'POST'])
def create_fields():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		form = CreateFields.update()
		if request.method == 'POST' and form.validate_on_submit():
			pass
	return render_template('create_fields.html', form=form,	title='Create fields.csv')