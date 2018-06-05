

@app.route('/traits/<level>/create_trt', methods=['GET', 'POST'])
def create_trt(level):
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			form = CreateTraits().update(level)
			if level == 'sample':
				node_label = 'SampleTrait'
			elif level == 'tree':
				node_label = 'TreeTrait'
			elif level == 'block':
				node_label = 'BlockTrait'
			elif level == 'plot':
				node_label = 'PlotTrait'
			if form.validate_on_submit():
				# selected traits
				# just a fancy flattening of the list
				selection = [
					item for sublist in [
						request.form.getlist(i) for i in request.form if i[len(level) + 1:] != 'csrf_token'
					]
					for item in sublist
				]
				# make the trt file and return it's details (filename, file_path, file_size)
				file_details = Lists(node_label).create_trt(session['username'], selection, 'name', level)
				# if result = none then no data was found
				if file_details is None:
					return jsonify({'submitted': "No entries found that match your selection"})
				download_url = url_for(
					'download_file',
					username=session['username'],
					filename=file_details['filename'],
					_external=True
				)
				# send email if requested and include as attachment if less than ~5mb
				if [i for i in request.form if i.endswith("email_checkbox")]:
					recipients = [User(session['username']).find('')['email']]
					subject = "BreedCAFS: traits.trt"
					body = (
						"You requested a " + level + ".trt file from the BreedCAFS database. "
						" The file is attached (if less than 5mb) and available for download at the following address:"
						+ download_url
					)
					html = render_template(
						'emails/create_traits.html',
						level=level,
						download_url=download_url
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
							file_details['file_path'])
						return jsonify(
							{
								'submitted': (
									'Your file is ready for download and a copy has been sent to your email as an attachment:'
									+ '"<a href="' + download_url + '">' + file_details['filename']
									+ '</a>"'
								)
							}
						)
					else:
						send_email(
							subject,
							app.config['ADMINS'][0],
							recipients,
							body,
							html
						)
						return jsonify(
							{
								'submitted': (
									'Your file is ready for download and a link has been sent to your email address:'
									+ '"<a href="' + download_url + '">' + file_details['filename']
									+ '</a>"'
								)
							}
						)
				# return as jsonify so that can be interpreted the same way as error message
				else:
					return jsonify(
						{
							'submitted': (
								'Your file is ready for download: "<a href="' + download_url + '">'
								+ file_details['filename'] + '</a>"'
							)
						}
					)
			else:
				return jsonify(form.errors)
		except ServiceUnavailable:
			flash("Database unavailable")
			return redirect(url_for('index'))