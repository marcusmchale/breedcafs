import os
from app import app, ServiceUnavailable
from flask import ( flash, redirect, url_for, request, session, render_template, jsonify )
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
		try:
			form = UploadForm()
			return render_template('upload.html', 
				form=form,
				title='Upload')
		except:
			flash("Database unavailable")
			return redirect(url_for('index'))


@app.route('/upload_submit', methods=['POST'])
def upload_submit():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			form = UploadForm()
			if form.validate_on_submit():
				username = session['username']
				time = datetime.now().strftime('_%Y%m%d-%H%M%S_')
				file = form.file.data
				subtype = form.submission_type.data
				if Upload(username, file.filename).allowed_file():
					#create user upload path if not found
					if not os.path.isdir(os.path.join(app.instance_path, app.config['UPLOAD_FOLDER'], username)):
						os.mkdir(os.path.join(app.instance_path, app.config['UPLOAD_FOLDER'], username))
					#prepare a secure filename to save with
					filename = secure_filename(time+file.filename)
					file_path = os.path.join(app.instance_path, app.config['UPLOAD_FOLDER'], username, filename)
					file.save(file_path)
					#check the first row of data conforms to expectations of submission type
					with open(file_path) as file:
						for i, line in enumerate(file): #this is so don't have to load the whole file into memory, just line by line
							if i == 0: #first line check if contains UID header in first collumn (assuming quotation)
								if line[1:4] != "UID": 
									return jsonify({"submitted": "This file is not a regular BreedCAFS FieldBook database file"})
							if i == 1: #2nd line # check if Tree or Block level data
								if line[line.index("_") +1] == "T":
									level = "tree"
								elif line[line.index("_") +1] == "B":
									level = "block"
								else: 
									return jsonify({"submitted": "This file is not a regular BreedCAFS FieldBook database file"})
					count = Upload(username, filename).submit(subtype, level)
					return jsonify({"submitted": str(count[0]) + " new records submitted, " + str(count[1]) + " records already found"})
				else:
					return jsonify({"submitted":"Please select a valid file"})
			else:
				return jsonify(form.errors)
		except:
			flash("Database unavailable")
			return redirect(url_for('index'))



