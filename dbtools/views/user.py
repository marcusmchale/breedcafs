from flask import session, render_template

from dbtools import app

from dbtools.models.forms import AffiliationForm
from dbtools.views.custom_decorators import login_required, neo4j_required


@app.route('/user_page', methods=['GET', 'POST'])
@login_required
@neo4j_required
def user_page():
	affiliation_form = AffiliationForm.update(session['username'])
	return render_template(
		'user_page.html',
		affiliation_form=affiliation_form
	)
