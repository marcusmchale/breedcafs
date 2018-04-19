import os
from app import app
#import celery
from flask import ( flash, redirect, url_for, request, session, render_template, jsonify )
from app.models import (
	#User,
	Upload,
	#Chart,
	async_submit
)
from app.forms import UploadForm
from werkzeug.utils import secure_filename
from datetime import (
	datetime,
	time as datetime_time
)
import unicodecsv as csv 
#from itertools import islice
#from cStringIO import StringIO

@app.route('/upload', methods=['GET', 'POST'])
def upload():
	if 'username' not in session:
		flash('Please log in')
		return redirect(url_for('login'))
	else:
		try:
			form = UploadForm()
			return render_template('upload.html', 
				form = form,
				title = 'Upload')
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
				filename = secure_filename(time + file.filename)
				file_path = os.path.join(app.instance_path, app.config['UPLOAD_FOLDER'], username, filename)
				file.save(file_path)
				#check the csv file conforms to expectations of submission type
					#using first 10 rows of data to check for consistency and to determine the "level" of data in this file (sample/tree/block)
					#avoiding reading in the whole file to check in case it is large
				with open(file_path) as file:
					#try:
						#TODO implement CSV kit checks - in particular csvstat to check field length (avoid stray quotes)
						#create a temporary file in memory sampling the first 2 rows from the uploaded file
						#sample_file = StringIO()
						#for line in islice(file, 2):
						#	sample_file.write(line)
						#stopped doing the above sampling as really need to check the entire file
						#to read it in we ensure we have returned to the start of the file
						file.seek(0)
						#now get the dialect and check it conforms to expectations
						dialect = csv.Sniffer().sniff(file.read())
						if not all((dialect.delimiter == ',', dialect.quotechar == '"')):
							return jsonify(
								{"submitted": "This file does not appear to be comma separated with quoted fields"
											  "Please use only upload supported files"
								 })
						# seek to the start of the file then prepare a dictreader object
						file.seek(0)
						file_dict = csv.DictReader(file)
						#first simply check the file contains UIDs
						if not 'UID' in file_dict.fieldnames:
							return jsonify(
								{"submitted": "This file does not contain a 'UID' header. "
											  "Please use only upload supported files"
											  "Please use only upload supported files"
								 }
							)
						#now check UID's for consistency (already at second line after reading into file_dict)
						uid_list = [i['UID'] for i in file_dict]
						#check if UID field is empty or contains whitespace in any rows
						if True in [any([i.isspace(), not i]) for i in uid_list]:
							return jsonify(
								{"submitted": "Please delete any empty rows before submitting"}
							)
						#if UIDs are all integers then plot data, else classify by letter
						if not False in [uid.isdigit() for uid in uid_list]:
							level = "plot"
						else:
							level_list = [uid[uid.index("_") + 1] for uid in uid_list if any(
								[
								"_B" in uid,
								"_T" in uid,
								"_R" in uid,
								"_L" in uid,
								"_S" in uid
								])]
							#ensure only one level per file
							if len(set(level_list)) > 1:
								return jsonify(
									{"submitted": "This file appears to contains mixed trait levels. "
												  "Please use only upload supported files"}
								)
							if level_list[0] == "B":
								level = "block"
							elif level_list[0] == "T":
								level = "tree"
							elif level_list[0] == "R":
								level = "branch"
							elif level_list[0] == "L":
								level = "leaf"
							elif level_list[0] == "S":
								level = "sample"
							else:
								return jsonify({"submitted": "This file does not seem to contain BreedCAFS unique ID's"
															 "Please use only upload supported files"
												})
						if request.form['submission_type'] == 'FB':
							required = set(['UID', 'trait', 'value', 'timestamp', 'person', 'location', 'number'])
							if not required.issubset(file_dict.fieldnames):
								return jsonify({"submitted": "This file does not look like a FieldBook exported CSV file"
															 "Please use check the submission type"
												})
							# as an asynchonous function with celery, result will be stored in redis and accessible from the status/task_id endpoint
							task = async_submit.apply_async(args=[username, filename, subtype, level])
						elif request.form['submission_type'] == 'table':
							required = set(['UID'])
							if not required.issubset(file_dict.fieldnames):
								return jsonify({"submitted": "This file appears to be missing the UID collumn"})
							if not len(set(uid_list)) == len(uid_list):
								return jsonify({
									"submitted": "This file contains duplicate rows for unique ID's. "
												 "This is not supported in table format"
								})
							#note: to return to the start of the reader object you have to seek(0) on the read file
							file.seek(0)
							#just passing in the full list of keys as we later do a match in the database for which are relevant traits
							traits = [key for key in file_dict.next()]
							#get and format form data for person and time of data collection
							form_user = form.user.data if form.user.data != None else 'unknown'
							if all([form.date.data != None, form.time.data != None]):
								form_date = form.date.data
								form_time = form.time.data
								neo4j_time = int(
									(datetime.combine(
										form_date,
										datetime.time(form_time)
									)
									 - datetime(1970, 1, 1)).total_seconds() * 1000
								)
							elif all([form.date.data != None, form.time.data == None]):
								form_date = form.date.data
								form_time = 'unknown'
								#calculate for middle of the day if not noted
								neo4j_time = int(
									(datetime.combine(
										form_date,
										datetime_time(12, 00)
									)
									 - datetime(1970, 1, 1)).total_seconds() * 1000
								)
							else:
								# to ensure all data points have a time field give them a time of now if not specified
								neo4j_time = int((datetime.now() - datetime(1970, 1, 1)).total_seconds() * 1000)
								form_date = 'unknown'
								form_time = 'unknown'
							# as an asynchonous function with celery, result will be stored in redis and accessible from the status/task_id endpoint
							task = async_submit.apply_async(args=[
								username,
								filename,
								subtype,
								level,
								traits,
								form_user,
								form_date,
								form_time,
								neo4j_time
							])
					#except:
						#return jsonify({'submitted':'An unknown error has occurred please check the file format'})
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
