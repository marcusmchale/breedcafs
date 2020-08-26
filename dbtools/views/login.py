from dbtools import app, neo4j_driver

from flask import (
	request,
	session, 
	redirect, 
	url_for,
	render_template, 
	flash
)

from dbtools.models.forms import LoginForm

from dbtools.views.custom_decorators import redis_required, neo4j_required

from dbtools.controllers.user_session import Guest, LoginHandler


@app.route('/login', methods=['GET', 'POST'])
@neo4j_required
@redis_required
def login():
	# this view handles service unavailable at the model level (in User.login)
	form = LoginForm()
	if form.validate_on_submit():
		guest = Guest(form.username.data, form.password.data, request.remote_addr)
		login_handler = LoginHandler(guest)
		with neo4j_driver.session() as neo4j_session:
			if neo4j_session.read_transaction(login_handler.login):
				session['username'] = login_handler.registered_user.username
				session['access'] = login_handler.registered_user.access
				flash('Logged in.')
				return redirect(url_for('index'))
			else:
				error_message = '\n'.join(login_handler.errors)
				flash(error_message)
				return redirect(url_for('login'))
	return render_template('login.html', form = form, title = 'Login')


@app.route('/logout', methods=['GET'])
def logout():
	session.pop('username', None)
	session.pop('access', None)
	flash('Logged out')
	return redirect(url_for('index'))


