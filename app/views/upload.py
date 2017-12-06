import os
from app import app
#import celery
from flask import ( flash, redirect, url_for, request, session, render_template, jsonify )
from app.models import User, Upload, Chart, async_submit
from app.forms import UploadForm
from werkzeug.utils import secure_filename
from datetime import datetime
import unicodecsv as csv 
from itertools import islice
from cStringIO import StringIO

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
				#check the csv file conforms to expectations of submission type
					#using first 10 rows of data to check for consistency and to determine the "level" of data in this file (sample/tree/block)
					#avoiding reading in the whole file to check in case it is large
				with open(file_path) as file:
					#create a temporary file in memory sampling the first 10 rows from the uploaded file
					sample_file = StringIO()
					for line in islice(file, 10):
						sample_file.write(line)
					#to read it in we must return to the start of the file
					sample_file.seek(0)
					#now get the dialect and check it conforms to expectations
					dialect = csv.Sniffer().sniff(sample_file.read())
					if not all ((dialect.delimiter == ',', dialect.quotechar == '"')):
						return jsonify({"submitted": "This file is not a regular BreedCAFS FieldBook database file:"})
					#once again seek to the start of the file then prepare a dictreader object
					sample_file.seek(0)
					sample_dict = csv.DictReader(sample_file)
					required = set(['UID', 'trait', 'value', 'timestamp', 'person', 'location', 'number'])
					if not required.issubset(sample_dict.fieldnames):
						return jsonify({"submitted": "This file is not a regular BreedCAFS FieldBook database file"})
					#now check the last row of this sample to identify the sample type
					#note: to return to the start of the reader object you just seek(0) on the read file 
					sample_file.seek(0)
					sample_dict.next() #skip the header row
					uid = sample_dict.next()['UID']
					if uid.isdigit():
						level = "plot"
					else:
						lvl = uid[uid.index("_") +1]
						if lvl == "S":
							level = "sample"
						elif lvl == "T":
							level = "tree"
						elif lvl == "B":
							level = "block"
						else: 
							return jsonify({"submitted": "This file is not a regular BreedCAFS FieldBook database file"})
				#as an asynchonous function with celery, result will be stored in redis and accessible from the status/task_id endpoint
				task = async_submit.apply_async(args=[username, filename, subtype, level])
				return jsonify({'submitted': ('File has been uploaded and will be merged into the database. '
						' <br><br> Depending on the size of the file this may take some time so you will receive an email including a summary of the update. '
						' <br><br> If you wait here you will also get feedback on this page.'), 
					'task_id': task.id })
			else:
				return jsonify({'submitted':'Please select a valid file'})
		else:
			return jsonify(form.errors)


@app.route('/status/<task_id>/')
def taskstatus(task_id):
	task = async_submit.AsyncResult(task_id)
	return jsonify({'status': task.status, 'result':task.get()})
