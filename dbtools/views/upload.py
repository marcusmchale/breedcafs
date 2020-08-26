from app import app, ServiceUnavailable, SecurityError, logging
from flask import (
	flash, request, redirect, url_for, abort, session, render_template, jsonify
)
from app.models import Upload, Resumable
from app.forms import UploadForm

from dbtools.views.custom_decorators import neo4j_required, login_required


@app.route('/upload', methods=['GET', 'POST'])
@login_required
@neo4j_required
def upload():
	form = UploadForm()
	return render_template('upload.html', form=form, title='Upload')


@app.route('/resumable_upload', methods=['GET'])
@login_required
def resumable():
	username = session['username']
	raw_filename = request.args.get('resumableFilename', default='error', type=str)
	if not Resumable.allowed_file(raw_filename):
		abort(415, 'File type not supported')
	chunk_number = request.args.get('resumableChunkNumber', default=1, type=int)
	resumable_id = request.args.get('resumableIdentifier', default='error', type=str)
	total_chunks = request.args.get('resumableTotalChunks', type=int)
	if not resumable_id or not raw_filename or not chunk_number:
		abort(500, 'Parameter error')
	resumable_object = Resumable(username, raw_filename, resumable_id)
	if resumable_object.check_for_chunk(resumable_id, chunk_number):
		complete = resumable_object.complete(total_chunks)
		return jsonify({
			'status': complete,
			'total_chunks': total_chunks
		})
	else:
		return 'Not found', 204


@app.route('/resumable_upload', methods=['POST'])
@login_required
def resumable_post():
	raw_filename = request.form.get('resumableFilename', default='error', type=str)
	if not Resumable.allowed_file(raw_filename):
		abort(415, 'File type not supported')
	chunk_number = request.form.get('resumableChunkNumber', default=1, type=int)
	resumable_id = request.form.get('resumableIdentifier', default='error', type=str)
	total_chunks = request.args.get('resumableTotalChunks', type=int)
	chunk_data = request.files['file']
	resumable_object = Resumable(session['username'], raw_filename, resumable_id)
	resumable_object.save_chunk(chunk_data, chunk_number)
	complete = resumable_object.complete(total_chunks)
	return jsonify({
		'status': complete,
		'total_chunks': total_chunks
	})


@app.route('/assemble_upload', methods=['POST'])
@login_required
def resumable_assemble():
	raw_filename = request.form.get('fileName', default='error', type=str)
	if not Resumable.allowed_file(raw_filename):
		abort(415, 'File type not supported')
	size = request.form.get('size', type=int)
	resumable_id = request.form.get('uniqueIdentifier', default='error', type=str)
	total_chunks = request.form.get('total_chunks', type=int)
	resumable_object = Resumable(session['username'], raw_filename, resumable_id)
	if resumable_object.complete(total_chunks):
		if resumable_object.assemble(size):
			return 'ASSEMBLED'
	else:
		abort(400, 'File not complete - try again')


@app.route('/upload_submit', methods=['POST'])
@login_required
@neo4j_required
def upload_submit():
	form = UploadForm()
	if form.validate_on_submit():
		username = session['username']
		submission_type = form.submission_type.data
		raw_filename = request.form.get('filename', default='error', type=str)
		if not Resumable.allowed_file(raw_filename, submission_type=submission_type):
			return jsonify({
				'result': 'File type not supported for this submission type',
				'status': 'ERRORS'
			})
		upload_object = Upload.select(username, submission_type, raw_filename)
		# as an asynchronous function with celery
		# result is stored in redis and accessible from the status/task_id endpoint
		task = Upload.async_submit.apply_async(args=[username, upload_object])
		return jsonify({
			'result': (
				'Your file has been uploaded and is being checked before submission to the database.\n'
				'You will receive a report both here and via email that will either confirm your submission '
				'or describe any issues that require your attention.',

			),
			'status': 'SUCCESS',
			'task_id': task.id
		})
	else:
		return jsonify(form.errors)


@app.route('/status/<task_id>/')
@login_required
def task_status(task_id):
	task = Upload.async_submit.AsyncResult(task_id)
	if task.status != 'SUCCESS':
		return jsonify({'status': task.status})
	else:
		result = task.get()
		return jsonify({
			'status': task.status,
			'result': result
		})
