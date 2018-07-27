from app import app, ServiceUnavailable, AuthError
from flask import session, flash, redirect, url_for
from app.models import Chart
from datetime import datetime, timedelta


@app.route("/json_submissions")
def json_submissions():
	tomorrow = (datetime.utcnow()+timedelta(days=1)).strftime("%Y-%m-%d")
	yesterday = (datetime.utcnow()-timedelta(days=7)).strftime("%Y-%m-%d")
	return Chart().get_submissions_range(session['username'], yesterday, tomorrow)


@app.route("/json_fields_treecount")
def json_fields_treecount():
	try:
		return Chart().get_fields_treecount()
	except (ServiceUnavailable, AuthError):
		flash("Database unavailable")
		return redirect(url_for('index'))
