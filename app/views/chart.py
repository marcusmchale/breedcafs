from app import app, ServiceUnavailable
from flask import render_template, session, flash, redirect, url_for
from app.models import Chart
from datetime import datetime, timedelta


@app.route("/json_submissions")
def json_submissions():
	tomorrow=(datetime.now()+timedelta(days=1)).strftime("%Y-%m-%d")
	yesterday=(datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d")
	return Chart().get_submissions_range(session['username'],yesterday,tomorrow)


@app.route("/json_plots_treecount")
def json_plots_treecount():
	try:
		return Chart().get_plots_treecount()
	except (ServiceUnavailable):
		flash("Database unavailable")
		return redirect(url_for('index'))
