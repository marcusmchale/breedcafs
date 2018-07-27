from app import app
from flask import flash, redirect, url_for, render_template, session


@app.route('/help', methods=['GET'])
def help():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		return render_template('help.html', title='Help')


@app.route('/help/field_book', methods=['GET'])
def field_book_help():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		return render_template('field_book_help.html', title='Field Book Help')
