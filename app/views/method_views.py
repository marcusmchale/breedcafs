from app import (
	ServiceUnavailable,
	SecurityError
)

from flask import (
	session,
	flash,
	redirect,
	url_for,
	make_response,
	jsonify
)
from flask.views import MethodView

from app.models import (
	SelectionList,
	Chart
)


# endpoints to get locations as tuples for forms
class ListCountries(MethodView):
	def get(self):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				countries = SelectionList.get_countries()
				response = make_response(jsonify(countries))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable, SecurityError):
				flash("Database unavailable")
				return redirect(url_for('index'))


class ListRegions(MethodView):
	def get(self, country):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				regions = SelectionList.get_regions(country)
				response = make_response(jsonify(regions))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable, SecurityError):
				flash("Database unavailable")
				return redirect(url_for('index'))


class ListFarms(MethodView):
	def get(self, country, region):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				farms = SelectionList.get_farms(country, region)
				response = make_response(jsonify(farms))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable, SecurityError):
				flash("Database unavailable")
				return redirect(url_for('index'))


class ListFields(MethodView):
	def get(self, country, region, farm):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				fields = SelectionList.get_fields(country, region, farm)
				response = make_response(jsonify(fields))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable, SecurityError):
				flash("Database unavailable")
				return redirect(url_for('index'))


class ListBlocks(MethodView):
	def get(self, field_uid):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				blocks = SelectionList.get_blocks(field_uid=field_uid)
				response = make_response(jsonify(blocks))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable, SecurityError):
				flash("Database unavailable")
				return redirect(url_for('index'))


class TreeCount(MethodView):
	@staticmethod
	def get(uid):
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				tree_count = Chart.get_tree_count(uid)
				response = make_response(jsonify(tree_count))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable, SecurityError):
				flash("Database unavailable")
				return redirect(url_for('index'))
