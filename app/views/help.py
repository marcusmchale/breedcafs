from app import app
from flask import flash, redirect, url_for, render_template, session
from instance import varieties


@app.route('/help', methods=['GET'])
def help():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		return render_template('help.html', title='Help')


@app.route('/resources', methods=['GET'])
def resources():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		variety_codes = varieties.el_frances_variety_codes
		return render_template(
			'resources.html',
			title='Resources',
			variety_codes=variety_codes
		)