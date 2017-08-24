import os
from app import app
from flask import flash, redirect, url_for, request, session, render_template
from app.models import User, Upload, Chart
from app.forms import UploadForm
from werkzeug.utils import secure_filename
from datetime import datetime

@app.route('/upload', methods=['GET', 'POST'])
def upload():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		form = UploadForm()
		if request.method == 'POST' and form.validate_on_submit():
			uploads=app.config['UPLOAD_FOLDER']
			username=session['username']
			time=datetime.now().strftime('_%Y%m%d-%H%M%S_')
			file = form.file.data
			subtype=form.submission_type.data
			#If a user is deleted/username changed (while they are still logged in) they will be booted and asked to login/register when they try to submit
			if not User(username).find(''):
				session.pop('username', None)
				flash('Error: User not found - please login/register')
				return redirect(url_for('index'))
			if not file:
				flash('No file selected')
				return redirect(url_for('upload'))
			if file.filename == '':
				flash('No file selected')
				return redirect(url_for('upload'))
			if Upload(username, file.filename).allowed_file():
				filename = username+time+secure_filename(file.filename)
				file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
				count = Upload(username, os.path.join(uploads, filename)).submit(subtype)
				flash (str(count[0]) + ' new records submitted, ' + str(count[1]) + ' records already found')
			else:
				flash('Please submit CSV file')
			return redirect(url_for('upload'))
	return render_template('upload.html', form=form, title='Upload')

