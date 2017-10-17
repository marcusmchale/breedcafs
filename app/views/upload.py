import os
from app import app
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
		form = UploadForm()
		return render_template('upload.html', 
			form=form,
			title='Upload')

@app.route('/upload_submit', methods=['POST'])
def upload_submit():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		form = UploadForm()
		if form.validate_on_submit():
			username = session['username']
			time = datetime.now().strftime('_%Y%m%d-%H%M%S_')
			file = form.file.data
			subtype = form.submission_type.data
			if Upload(username, file.filename).allowed_file():
				filename = username+time+secure_filename(file.filename)
				file.save(os.path.join(app.instance_path, 
					app.config['UPLOAD_FOLDER'], 
					filename))
				#check the first row of data conforms to expectations of submission type
				with open(os.path.join(app.instance_path, 
					app.config['UPLOAD_FOLDER'], 
					filename)) as fp:
					for i, line in enumerate(fp): #this is so don't have to load the whole file into memory, just line by line
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
					#add a value 
				count = Upload(username, filename).submit(subtype, level)
				return jsonify({"submitted": str(count[0]) + " new records submitted, " + str(count[1]) + " records already found"})
			else:
				return jsonify({"submitted":"Please select a valid file"})
		else:
			return jsonify(form.errors)


