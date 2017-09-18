from app import app
from flask import flash, redirect, url_for, render_template, session

@app.route('/help', methods=['GET'])
def help():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		return render_template('help.html', title='Help')
