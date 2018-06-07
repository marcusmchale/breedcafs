import os, shutil
from app import app
#import celery
from flask import ( flash, redirect, url_for, request, session, render_template, jsonify )
from app.models import (
	Upload,
	async_submit,
	DictReaderInsensitive
)
from app.forms import UploadForm
from werkzeug.utils import secure_filename
from datetime import (
	datetime
)
import unicodecsv as csv 


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
				# create user upload path if not found
				if not os.path.isdir(os.path.join(app.instance_path, app.config['UPLOAD_FOLDER'], username)):
					os.mkdir(os.path.join(app.instance_path, app.config['UPLOAD_FOLDER'], username))
				# prepare a secure filename to save with
				filename = secure_filename(time + file.filename)
				file_path = os.path.join(app.instance_path, app.config['UPLOAD_FOLDER'], username, filename)
				file.save(file_path)
				try:
					with open(file_path) as file:
						# TODO implement CSV kit checks - in particular csvstat to check field length (avoid stray quotes)
						# now get the dialect and check it conforms to expectations
						file.seek(0)
						dialect = csv.Sniffer().sniff(file.read())
						if not all((dialect.delimiter == ',', dialect.quotechar == '"')):
							return jsonify(
								{'submitted': 'Please upload comma (,) separated file with quoted (") fields' })
						# clean up the csv file by passing through dict reader and rewriting
						temp_file_path = os.path.join(
							app.instance_path,
							app.config['UPLOAD_FOLDER'],
							username,
							filename + "_temp"
						)
						with open(temp_file_path, "w") as temp_file:
							file.seek(0)
							# this dict reader lowers case and trims whitespace on all headers
							file_dict = DictReaderInsensitive(
								file,
								skipinitialspace=True
							)
							temp_csv = csv.DictWriter(
								temp_file,
								fieldnames = file_dict.fieldnames,
								quoting = csv.QUOTE_ALL
							)
							temp_csv.writeheader()
							for row in file_dict:
								# remove rows without entries
								if any(field.strip() for field in row):
									for field in file_dict.fieldnames:
										row[field] = row[field].strip()
									temp_csv.writerow(row)
						# back up the original file
						shutil.move(file_path, file_path + ".bak")
						# replace the original file with the filtered version
						shutil.move(temp_file_path, file_path)
					# re-open the cleaned up file
					with open(file_path) as file:
						file_dict = DictReaderInsensitive(file)
						#first simply check the file contains UIDs
						if not 'uid' in file_dict.fieldnames:
							return jsonify(
								{"submitted": "This file does not contain a 'UID' header. "
											  "Please use only upload supported files"
								}
							)
						# now check UID's for consistency (already at second line after reading into file_dict)
						uid_list = [(i['uid']).strip() for i in file_dict]
						# check if all UIDs are valid (formatting only)
						for i in uid_list:
							i_split = i.split("_")
							# check if UID field is empty or contains whitespace in any rows
							if any(
								[
									i.isspace(),
									not i,
									not i_split[0].isdigit(),
								]
							):
								return jsonify(
									{"submitted": "Please ensure all rows have a valid UID"}
								)
							if len(i_split) > 1:
								if len(i_split) >2:
									return jsonify(
										{"submitted": "Please ensure all rows have a valid UID"}
									)
								if not any(
									[
										i_split[1][0].upper() in ["B","T","R","L","S"],
										i_split[1][1:].isdigit()
									]
								):
									return jsonify(
										{"submitted": "Please ensure all rows have a valid UID"}
									)
						if request.form['submission_type'] == 'FB':
							required = set(['uid', 'trait', 'value', 'timestamp', 'person', 'location'])
							if not required.issubset(file_dict.fieldnames):
								return jsonify({"submitted": "This file does not look like a FieldBook exported CSV file"
															 "Please use check the submission type"
									})
							# as an asynchonous function with celery, result will be stored in redis and accessible from the status/task_id endpoint
							task = async_submit.apply_async(args=[username, filename, subtype])
						elif request.form['submission_type'] == 'table':
							required = ['uid', 'date', 'time', 'person']
							if not set(required).issubset(file_dict.fieldnames):
								return jsonify({"submitted": "This file appears to be missing a required column (UID,Date,Time,Person)"})
							#note: to return to the start of the reader object you have to seek(0) on the read file
							file.seek(0)
							#just passing in the full list of keys as we later do a match in the database for which are relevant traits
							traits = [i for i in file_dict.fieldnames if not i in required]
							# as an asynchonous function with celery, result will be stored in redis and accessible from the status/task_id endpoint
							task = async_submit.apply_async(args=[
								username,
								filename,
								subtype,
								traits
							])
				except:
					return jsonify({'submitted':'An unknown error has occurred please check the file format'})
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
	if task.status != 'SUCCESS':
		return jsonify({'status': task.status})
	else:
		result = task.get()
		return jsonify({'status': task.status, 'result':result})
