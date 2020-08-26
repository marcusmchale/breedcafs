from app import app
from flask import flash, redirect, url_for, render_template, session
from instance import varieties

from dbtools.views.custom_decorators import login_required


@app.route('/help', methods=['GET'])
@login_required
def help():
	return render_template('help.html', title='Help')


@app.route('/resources', methods=['GET'])
def resources():
	variety_codes = varieties.el_frances_variety_codes
	return render_template(
		'resources.html',
		title='Resources',
		variety_codes=variety_codes
	)