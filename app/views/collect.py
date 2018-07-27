from app import (
	app,
	ServiceUnavailable,
	AuthError
)
from flask import (
	session,
	flash,
	redirect,
	url_for,
	render_template,
	make_response,
	jsonify,
	request
)
from app.models import (
	Lists,
	User,
	# Download,
	Samples,
)
from app.forms import (
	LocationForm,
	CreateTraits,
	SampleRegForm,
	AddTissueForm,
	AddStorageForm,
)
from app.emails import (
	# send_email,
	send_static_attachment
)
from flask.views import MethodView


@app.route('/collect', methods=['GET', 'POST'])
def collect():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			location_form = LocationForm.update()
			sample_traits_form = CreateTraits().update('sample')
			tree_traits_form = CreateTraits().update('tree')
			block_traits_form = CreateTraits().update('block')
			field_traits_form = CreateTraits().update('field')
			sample_reg_form = SampleRegForm().update()
			add_tissue_form = AddTissueForm()
			add_storage_form = AddStorageForm()
			return render_template(
				'collect.html',
				location_form = location_form,
				level = 'all',
				sample_traits_form = sample_traits_form,
				tree_traits_form = tree_traits_form,
				block_traits_form = block_traits_form,
				field_traits_form = field_traits_form,
				sample_reg_form = sample_reg_form,
				add_tissue_form = add_tissue_form,
				add_storage_form = add_storage_form,
				title = 'Collect'
			)
		except (ServiceUnavailable, AuthError):
			flash("Database unavailable")
			return redirect(url_for('index'))


# Samples
@app.route('/sample_reg', methods=['GET', 'POST'])
def sample_reg():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			location_form = LocationForm.update()
			add_tissue_form = AddTissueForm()
			add_storage_form = AddStorageForm()
			sample_reg_form = SampleRegForm.update()
			# custom_sample_form = CustomSampleForm()
			return render_template(
				'sample_reg.html',
				location_form=location_form,
				add_tissue_form=add_tissue_form,
				add_storage_form=add_storage_form,
				sample_reg_form=sample_reg_form,
				# custom_sample_form=custom_sample_form,
				title='Sample registration'
			)
		except (ServiceUnavailable, AuthError):
			flash("Database unavailable")
			return redirect(url_for('index'))


class Tissues(MethodView):
	@staticmethod
	def get():
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				tissues = Lists('Tissue').create_list_tup('name', 'name')
				response = make_response(jsonify(tissues))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable, AuthError):
				flash("Database unavailable")
				return redirect(url_for('index'))


@app.route('/add_tissue', methods=["POST"])
def add_tissue():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			form = AddTissueForm()
			text_tissue = request.form['text_tissue'].strip()
			if form.validate_on_submit():
				find_tissue = Lists('Tissue').find_node(text_tissue)
				if find_tissue:
					return jsonify({"found": find_tissue[0]['name'].title()})
				else:
					new_tissue = Samples().add_tissue(text_tissue)
					return jsonify({"submitted": new_tissue[0]['name'].title()})
			else:
				return jsonify([form.errors])
		except (ServiceUnavailable, AuthError):
			flash("Database unavailable")
			return redirect(url_for('index'))


class StorageMethods(MethodView):
	@staticmethod
	def get():
		if 'username' not in session:
			flash('Please log in')
			return redirect(url_for('login'))
		else:
			try:
				storage_methods = Lists('Storage').create_list_tup('name', 'name')
				response = make_response(jsonify(storage_methods))
				response.content_type = 'application/json'
				return response
			except (ServiceUnavailable, AuthError):
				flash("Database unavailable")
				return redirect(url_for('index'))


@app.route('/add_storage', methods=["POST"])
def add_storage():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			form = AddStorageForm()
			text_storage = request.form['text_storage'].strip()
			if form.validate_on_submit():
				find_storage = Lists('Storage').find_node(text_storage)
				if find_storage:
					return jsonify({"found": find_storage[0]['name'].title()})
				else:
					new_storage = Samples().add_storage(text_storage)
					return jsonify({"submitted": new_storage[0]['name'].title()})
			else:
				return jsonify([form.errors])
		except (ServiceUnavailable, AuthError):
			flash("Database unavailable")
			return redirect(url_for('index'))


@app.route('/add_samples', methods=['GET', 'POST'])
def add_samples():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			location_form = LocationForm.update()
			sample_form = SampleRegForm.update()
			if all([location_form.validate_on_submit(), sample_form.validate_on_submit()]):
				field_uid = int(request.form['field'])
				start = int(request.form['trees_start']) if request.form['trees_start'] else 0
				end = int(request.form['trees_end']) if request.form['trees_end'] else 999999
				replicates = int(request.form['replicates'])
				tissue = request.form['tissue']
				storage = request.form['storage']
				date = request.form['date_collected']
				# register samples, make file describing index information and return filename etc.
				file_details = Samples().add_samples(field_uid, start, end, replicates, tissue, storage, date, True)
				# if result = none then no data was found
				if 'error' in file_details:
					return jsonify({'submitted': file_details['error']})
				# create a download url
				download_url = url_for(
					'download_file',
					username = session['username'],
					filename = file_details['filename'],
					_external = True
				)
				# if requested create email (and send as attachment if less than ~5mb)
				if request.form.get('email_checkbox'):
					recipients = [User(session['username']).find('')['email']]
					subject = "BreedCAFS: Samples registered"
					body = (
							"You registered " + str(replicates) + " replicates of " + str(tissue)
							+ " samples stored in " + str(storage) + " on " + str(date) + " for trees from "
							+ str(start) + " to " + str(end) + " in Field #" + str(field_uid) + "."
							+ " These samples are described in a file available for download at the following address: "
							+ download_url
					)
					html = render_template(
						'emails/add_samples.html',
						field_uid = field_uid,
						start = start,
						end = end,
						replicates = replicates,
						tissue = tissue,
						storage = storage,
						date = date,
						download_url = download_url
					)
					if file_details['file_size'] < 5000000:
						send_static_attachment(
							subject,
							app.config['ADMINS'][0],
							recipients,
							body,
							html,
							file_details['filename'],
							'text/csv',
							file_details['file_path']
						)
						return jsonify({'submitted': (
								'You successfully registered ' + str(tissue) + ' samples.<br> '
								+ 'These are described in the following file: '
								+ '<a href="' + download_url + '">' + file_details['filename'] + '</a>.<br>'
								+ ' This file has also been sent to your email address ')}
						)
					else:
						send_static_attachment(
							subject,
							app.config['ADMINS'][0],
							recipients,
							body,
							html,
							file_details['filename'],
							'text/csv',
							file_details['file_path']
						)
						return jsonify({'submitted': (
								'You successfully registered ' + str(tissue) + ' samples.<br> '
								+ 'These are described in the following file : '
								+ '<a href="' + download_url + '">' + file_details['filename'] + '</a>.<br>'
								+ ' This link has also been sent to your email address. ')})
				else:
					return jsonify(
						{
							'submitted': (
									'You successfully registered ' + str(tissue) + ' samples.<br>'
									+ 'These are described in the following file : '
									+ '<a href="' + download_url + '">' + file_details['filename']
									+ '</a>'
							)
						}
					)
			else:
				errors = jsonify([location_form.errors, sample_form.errors])
				return errors
		except (ServiceUnavailable, AuthError):
			flash("Database unavailable")
			return redirect(url_for('index'))
