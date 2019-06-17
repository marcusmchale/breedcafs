from app import app, os
import grp
from app.cypher import Cypher
from neo4j_driver import (
	get_driver,
	bolt_result
)
from flask import (
	url_for,
	render_template
)

from app.models import(
	AddFieldItems,
	ItemList,
	FeatureList,
	User
)

from string import maketrans

from app.emails import send_email

import unicodecsv as csv

from datetime import datetime

from xlsxwriter import Workbook, utility


# User class related (all uploads are tied to a user) yet more specifically regarding uploads
class Download:
	def __init__(self, username, email_requested=False):
		self.username = username
		self.email_requested = email_requested
		self.id_list = None
		self.replicates = None
		self.time_points = 1
		self.item_fieldnames = None
		self.item_level = None
		self.features = {}
		for record_type in app.config['RECORD_TYPES']:
			self.features[record_type] = []
		self.file_list = []
		# create user download path if not found
		self.user_download_folder = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], username)
		if not os.path.isdir(self.user_download_folder):
			os.mkdir(self.user_download_folder)
			gid = grp.getgrnam(app.config['CELERYGRPNAME']).gr_gid
			os.chown(self.user_download_folder, -1, gid)
			os.chmod(self.user_download_folder, 0775)
		# prepare variables to write the file
		self.time = datetime.utcnow().strftime('%Y%m%d-%H%M%S')

	def register_samples(
			self,
			level,
			country,
			region,
			farm,
			field_uid,
			block_uid,
			tree_id_list,
			sample_id_list,
			per_item_count
	):
		id_list = AddFieldItems.add_samples(
			self.username,
			level,
			country,
			region,
			farm,
			field_uid,
			block_uid,
			tree_id_list,
			sample_id_list,
			per_item_count
		)
		if id_list.peek():
			self.id_list = id_list
			return True
		else:
			return False

	def register_trees(
			self,
			field_uid,
			block_uid,
			count
	):
		id_list = AddFieldItems(self.username, field_uid).add_trees(
			count,
			block_uid
		)
		if id_list.peek():
			self.id_list = id_list
			return True
		else:
			return False

	def set_item_fieldnames(self):
		fieldnames_order = [
			'Country',
			'Region',
			'Farm',
			'Field',
			'Field UID',
			'Block',
			'Blocks',
			'Block ID',
			'Block IDs',
			'Tree ID',
			'Tree IDs',
			'Tree Custom ID',
			'Tree Custom IDs',
			'Source Sample IDs',
			'Source Sample Custom IDs',
			'Sample Custom ID',
			'Harvest Time',
			'Tissue',
			'Variety',
			'UID'
		]
		self.item_fieldnames = [i for i in fieldnames_order if i in self.id_list.peek()[0].keys()]

	def get_file_list_html(self):
		if not self.file_list:
			return None
		file_list_html = ''
		for i in self.file_list:
			file_list_html = file_list_html + str("<ul><a href=" + i['url'] + ">" + i['filename'] + "</a></ul>")
		return file_list_html

	def get_file_path(
			self,
			file_extension,
			base_filename=None,
			with_timestamp=True
	):
		if base_filename and with_timestamp:
			filename = base_filename + '_' + self.time
		elif base_filename:
			filename = base_filename
		else:
			filename = self.time
		filename = filename + '.' + file_extension
		file_path = os.path.join(self.user_download_folder, filename)
		# prevent overwrite of existing file unless same path down to the second.
		if os.path.isfile(file_path):
			time_s = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
			if base_filename:
				filename = base_filename + '_' + time_s
			else:
				filename = time_s
			filename = filename + '.' + file_extension
			file_path = os.path.join(self.user_download_folder, filename)
			# and just in case add an incrementing appendix if this time_s filename exists
			filename_appendix = 1
			while os.path.isfile(file_path):
				if base_filename:
					filename = base_filename + '_' + time_s + '_' + filename_appendix
				else:
					filename = '_'.join([time_s, filename_appendix])
				filename = '.'.join([filename, file_extension])
				file_path = os.path.join(self.user_download_folder, filename)
		return file_path

	def set_features(
			self,
			item_level,
			record_type=None,
			feature_group=None,
			features=None,
			sample_level=None
	):
		if not record_type:
			record_types = app.config['RECORD_TYPES']
		else:
			record_types = [record_type]
		for rt in record_types:
			self.features[rt] = FeatureList(
				item_level,
				rt).get_features(
				feature_group=feature_group,
				features=features
			)
		# drop "assign to trees" and "assign to samples" from sample registration if not at field level
		if item_level == 'sample' and feature_group == "Registration" and sample_level != 'field':
			not_used_features = ["assign to trees", "assign to samples"]
			self.features['property'] = [
				i for i in self.features['property'] if i['name_lower'] not in not_used_features
			]

	def record_form_to_template(
		self,
		record_data
	):
		self.id_list = ItemList().generate_id_list(record_data)
		if not self.id_list:
			return False
		if record_data['replicates'] and record_data['replicates'] > 1:
			self.replicates = record_data['replicates']
		if record_data['time_points'] and record_data['time_points'] > 1:
			self.time_points = record_data['time_points']
		self.set_features(
			record_data['item_level'],
			record_data['record_type'],
			feature_group=record_data['feature_group'] if 'feature_group' in record_data else None,
			features=record_data['selected_features'] if 'selected_features' in record_data else None
		)
		if not any(self.features.values()):
			return False
		if record_data['template_format'] == 'fb':
			self.make_fb_template()
		else:
			self.id_list_to_xlsx_template(
				base_filename=record_data['item_level']
			)
		return True

	def id_list_to_xlsx_template(
			self,
			base_filename=None
	):
		if not self.id_list and self.item_level and any(self.features.values()):
			return False
		self.set_item_fieldnames()
		file_path = self.get_file_path(
			'xlsx',
			base_filename=base_filename,
			with_timestamp=False
		)
		wb = Workbook(file_path)
		# column < row < cell formatting in priority
		date_lb_format = wb.add_format({'num_format': 'yyyy-mm-dd', 'left': 1})
		time_format = wb.add_format({'num_format': 'hh:mm', 'right': 1})
		right_border = wb.add_format({'right': 1})
		header_format = wb.add_format({'bottom': 1})
		# set the formatting for the feature columns
		numeric_format = wb.add_format({'num_format': ''})
		date_format = wb.add_format({'num_format': 'yyyy-mm-dd'})
		text_format = wb.add_format({'num_format': '@'})
		# This converts the percent to a number i.e. 10% to 0.1, prefer not to use it, just store the number
		# percent_format = wb.add_format({'num_format': 9})
		location_format = wb.add_format({'num_format': '0.0000; 0.0000'})
		feature_formats = {
			"numeric": numeric_format,
			"date": date_format,
			"text": text_format,
			"percent": numeric_format,
			"multicat": text_format,
			"categorical": text_format,
			"location": location_format,
			"boolean": text_format
		}
		# core field name and format tuple in dictionary by record type
		core_fields_formats = {
			'property': [
				('UID', right_border),
				('Person', right_border)
				],
			'trait': [
				('UID', right_border),
				('Date', date_lb_format),
				('Time', time_format),
				('Person', right_border)
			],
			'condition': [
				('UID', right_border),
				('Start Date', date_lb_format),
				('Start Time', time_format),
				('End Date', date_lb_format),
				('End Time', time_format),
				('Person', right_border)
			],
			'curve': [
				('UID', right_border),
				('Date', date_lb_format),
				('Time', time_format),
				('Person', right_border)
			]
		}
		# Create worksheets
		# Write headers and set formatting on columns
		# Store worksheet in a dict by record type to later write values when iterating through id_list
		ws_dict = {
			'property': None,
			'trait': None,
			'condition': None,
			'item_details': wb.add_worksheet(app.config['WORKSHEET_NAMES']['item_details']),
			'feature_details': wb.add_worksheet(app.config['WORKSHEET_NAMES']['feature_details']),
			'hidden': wb.add_worksheet("hidden")
		}
		# We add a new worksheet for each "curve" feature
		for feature in self.features["curve"]:
			ws_dict[feature['name_lower']] = None
		# write the item_details header
		for i, j in enumerate(self.item_fieldnames):
			ws_dict['item_details'].write(0, i, j, header_format)
		feature_details_fieldnames = [
			'type',
			'name',
			'format',
			'minimum',
			'maximum',
			'details',
			'category_list'
		]
		# write the feature_details header
		for header in feature_details_fieldnames:
			column_number = feature_details_fieldnames.index(header)
			ws_dict['feature_details'].write(0, column_number, header, header_format)
		# write the core fields to the feature details page
		feature_details_row_num = 0
		core_fields_details = [
			("person", "Optional: Person responsible for determining these values")
		]
		if self.features['trait'] or self.features['curve']:
			core_fields_details += [
				("date", "Required: Date these values were determined (YYYY-MM-DD, e.g. 2017-06-01)"),
				("time", "Optional: Time these values were determined (24hr, e.g. 13:00. Defaults to 12:00")
			]
		if self.features['condition']:
			core_fields_details += [
				("start date", "Optional: Date this condition started (YYYY-MM-DD, e.g. 2017-06-01)"),
				("start time", "Optional: Time this condition started (24hr, e.g. 13:00. Defaults to 00:00"),
				("end date", "Optional: Date this condition ended (YYYY-MM-DD, e.g. 2017-06-01)"),
				(
					"end time",
					"Optional: Time this condition ended (24hr, e.g. 13:00. Defaults to 00:00 of the following day"
				)
			]
		if self.features['curve']:
			core_fields_details += [
				(
					"Curve worksheets",
					"X-values/independent variables as column headers, Y-Values/dependent variables in rows below"
				)
			]
		for field, details in core_fields_details:
			feature_details_row_num += 1
			ws_dict['feature_details'].write(feature_details_row_num, 1, field)
			ws_dict['feature_details'].write(feature_details_row_num, 5, details)
		# empty row to separate date/time/person from features
		categorical_features_count = 0
		for record_type in [i for i in app.config['RECORD_TYPES'] if i in self.features]:
			if all([
				self.features[record_type],
				record_type != 'curve'
			]):
				# create record type specific worksheet
				ws_dict[record_type] = wb.add_worksheet(app.config['WORKSHEET_NAMES'][record_type])
				# write headers:
				# - add the core fields (e.g. person, date, time)
				for i, field in enumerate(core_fields_formats[record_type]):
					ws_dict[record_type].set_column(i, i, None, cell_format=field[1])
				# - add the feature field names
				fieldnames = (
						[field[0] for field in core_fields_formats[record_type]]
						+ [feature['name'] for feature in self.features[record_type]]
				)
				for i, fieldname in enumerate(fieldnames):
					ws_dict[record_type].write(0, i, fieldname, header_format)
				# - set right border formatting on the last column of record type specific worksheet
				ws_dict[record_type].set_column(
					len(fieldnames) - 1, len(fieldnames) - 1, None, cell_format=right_border
				)
			if self.features[record_type] and record_type == 'curve':
				for feature in self.features[record_type]:
					# Need to ensure curve feature names do not contain "[]:*?/\" or excel can't use them as sheetnames
					ws_dict[feature['name_lower']] = wb.add_worksheet(feature['name'])
					for i, field in enumerate(core_fields_formats[record_type]):
						ws_dict[feature['name_lower']].set_column(i, i, None, cell_format=field[1])
					# - add the feature field names
					fieldnames = (
							[field[0] for field in core_fields_formats[record_type]]
					)
					for i, fieldname in enumerate(fieldnames):
						ws_dict[feature['name_lower']].write(0, i, fieldname, header_format)
					ws_dict[feature['name_lower']].set_column(
						len(fieldnames) - 1, len(fieldnames) - 1, None, cell_format=right_border
					)
			# write feature details into feature details sheet
			for feature in self.features[record_type]:
				feature['type'] = str(record_type)
				feature_details_row_num += 1
				for j, field in enumerate(feature_details_fieldnames):
					if field in feature:
						if isinstance(feature[field], list):
							value = ", ".join(value for value in feature[field])
							ws_dict['feature_details'].write(feature_details_row_num, j, value)
						else:
							ws_dict['feature_details'].write(feature_details_row_num, j, feature[field])
		# to handle inheritance of Variety.
		# we are writing the retrieved Variety value (if singular) to the input field.
		# so we need the 'Variety name' column if found
		variety_name_column = None
		for i, feature in enumerate(self.features['property']):
			if feature['name_lower'] == 'variety name':
				variety_name_column = i + 2
		# iterate through id_list and write to worksheets
		item_num = 0
		for record in self.id_list:
			item_num += 1
			# if there is a list (or nested lists) stored in this item
			# make sure it is returned as a list of strings
			for key, value in record[0].iteritems():
				if isinstance(value, list):
					value = ", ".join([str(i) for i in value])
				item_details_column_number = self.item_fieldnames.index(key)
				ws_dict['item_details'].write(item_num, item_details_column_number, value)
			for record_type in self.features.keys():
				if self.features[record_type]:
					if all([
						record_type == 'trait',
						self.replicates > 1
					]):
						replicate_ids = [str(i) for i in range(1, self.replicates + 1)]
						replicates_row_number = ((item_num - 1) * (self.replicates * self.time_points)) + 1
						for rep in replicate_ids:
							if self.time_points > 1:
								for j in range(0, self.time_points):
									ws_dict[record_type].write(
										replicates_row_number, 0, '.'.join([record[0]['UID'], str(rep)])
									)
									replicates_row_number += 1
							else:
								ws_dict[record_type].write(
									replicates_row_number, 0, '.'.join([record[0]['UID'], str(rep)])
								)
								replicates_row_number += 1
					else:
						if record_type in ['trait', 'condition', 'curve'] and self.time_points > 1:
							for i in range(0, self.time_points):
								time_points_row_number = ((item_num - 1) * self.time_points) + 1 + i
								if record_type == 'curve':
									for feature in self.features[record_type]:
										ws_dict[feature['name_lower']].write(
											time_points_row_number,
											0,
											str(record[0]['UID'])
										)
								else:
									ws_dict[record_type].write(
										time_points_row_number, 0, str(record[0]['UID'])
									)
						else:
							if record_type == 'curve':
								for feature in self.features[record_type]:
									ws_dict[feature['name_lower']].write(
										item_num,
										0,
										str(record[0]['UID'])
									)
							else:
								ws_dict[record_type].write(item_num, 0, str(record[0]['UID']))
					# to handle inheritance of Variety.
					# we are writing the retrieved Variety value (if singular) to the input field.
					if all([
						record_type == 'property',
						'Variety' in record[0] and record[0]['Variety'] and len(record[0]['Variety']) == 1,
						variety_name_column
						]):
							ws_dict[record_type].write(item_num, variety_name_column, record[0]['Variety'][0])
		# now that we know the item_num we can add the validation
		# currently no validation for curves as no definite columns
		for record_type in self.features.keys():
			if self.features[record_type] and record_type != 'curve':
				if record_type == 'trait' and self.replicates > 1:
					row_count = item_num * self.replicates * self.time_points
				else:
					row_count = item_num * self.time_points
				for i, field in enumerate(self.features[record_type]):
					col_num = len(core_fields_formats[record_type]) + i
					ws_dict[record_type].set_column(
						col_num, col_num, None, cell_format=feature_formats[field['format']]
					)
					if 'category_list' in field:
						categorical_features_count += 1
						ws_dict['hidden'].write((categorical_features_count - 1), 0, field['name_lower'])
						for j, category in enumerate(field['category_list']):
							ws_dict['hidden'].write((categorical_features_count -1), j + 1 , category)
						ws_dict[record_type].data_validation(1, col_num, row_count, col_num, {
							'validate': 'list',
							'source': (
								'=hidden!$B$' + str(categorical_features_count)
								+ ':$' + utility.xl_col_to_name(len(field['category_list']) + 1)
								+ '$' + str(categorical_features_count)
							)
						})
		ws_dict['hidden'].hide()
		wb.close()
		# add file to file_list
		self.file_list.append({
			"filename": os.path.basename(file_path),
			"file_path": file_path,
			"file_size": os.path.getsize(file_path),
			"url": url_for(
				'download_file',
				username=self.username,
				filename=os.path.basename(file_path),
				_external=True
			)
		})

	def make_csv_file(self, fieldnames, id_list, base_filename, with_timestamp=True, file_extension='csv'):
		file_path = self.get_file_path(
			file_extension,
			base_filename=base_filename,
			with_timestamp=with_timestamp
		)
		# make the file
		with open(file_path, 'w') as csv_file:
			writer = csv.DictWriter(
				csv_file,
				fieldnames=fieldnames,
				quoting=csv.QUOTE_ALL,
				extrasaction='ignore'
			)
			writer.writeheader()
			# We hand this function either query results (bolt iterable or lists, we need to distinguish them
			if isinstance(id_list, list):
				for row in id_list:
					for item in row:
						if isinstance(row[item], list):
							row[item] = ", ".join([i for i in row[item]])
					# for key, value in row:
					writer.writerow(row)
			else:
				for row in id_list:
					for item in row[0]:
						if isinstance(row[0][item], list):
							row[0][item] = ", ".join([i for i in row[0][item]])
					# for key, value in row:
					writer.writerow(row[0])
			# file_size = csv_file.tell()
		# return file details
		self.file_list.append({
			"filename": os.path.basename(file_path),
			"file_path": file_path,
			"file_size": os.path.getsize(file_path),
			"url": url_for(
				'download_file',
				username=self.username,
				filename=os.path.basename(file_path),
				_external=True
			)
		})

	def make_fb_template(
			self,
			base_filename=None,
			with_timestamp=True
	):
		self.set_item_fieldnames()
		self.make_csv_file(
			self.item_fieldnames,
			self.id_list,
			base_filename=base_filename,
			with_timestamp=with_timestamp
		)
		feature_fieldnames = [
			'name',
			'format',
			'defaultValue',
			'minimum',
			'maximum',
			'details',
			'categories',
			'isVisible',
			'realPosition'
		]
		for i, feature in enumerate(self.features['trait']):
			feature['realPosition'] = str(i + 1)
			feature['isVisible'] = 'True'
		self.make_csv_file(
			feature_fieldnames,
			self.features['trait'],
			base_filename=base_filename,
			with_timestamp=with_timestamp,
			file_extension='trt'
		)
		# TODO may need to replace 'name' with 'trait' in header but doesn't seem to affect Field-Book

	def make_csv_table_template(
			self,
			fieldnames,
			id_list,
			features,
			base_filename=None,
			with_timestamp=True
	):
		fieldnames += ['Date', 'Time', 'Person']
		fieldnames += [feature['name'] for feature in features]
		self.make_csv_file(
			fieldnames,
			id_list,
			base_filename=base_filename + '_data',
			with_timestamp=with_timestamp
		)
		# and a file that describes the feature details
		feature_fieldnames = [
			'name',
			'format',
			'minimum',
			'maximum',
			'details',
			'category_list'
		]
		self.make_csv_file(
			feature_fieldnames,
			features,
			base_filename=base_filename + '_details',
			with_timestamp=with_timestamp
		)

	@staticmethod
	def user_record_query(
			parameters,
			data_format='db'
	):
		# make sure you have a match for both "partner" and "user" before adding this statement
		statement = (
			' MATCH '
			'	(user)-[: SUBMITTED]->(: Submissions)'
			'	-[: SUBMITTED]->(: Records) '
			'	-[: SUBMITTED]->(: UserFieldFeature) '
			'	-[submitted: SUBMITTED]->(record: Record) '
			'	-[:RECORD_FOR]->(item_feature:ItemFeature) '
			'	-[:FOR_FEATURE*..2]->(feature:Feature)'
			'	-[:OF_TYPE]->(record_type:RecordType), '
			'	(item_feature) '
			'	-[:FOR_ITEM]->(item:Item) '
			'	-[:FROM | IS_IN *]->(farm: Farm) '
			'	-[:IS_IN]->(region: Region) '
			'	-[:IS_IN]->(country: Country) '
		)
		filters = []
		if parameters['selected_features']:
			filters.append(
				' feature.name_lower IN $selected_features '
			)
		if parameters['submission_start']:
			filters.append(' submitted.time >= $submission_start ')
		if parameters['submission_end']:
			filters.append(' submitted.time <= $submission_end ')
		# collect all the records that have start or end inside the selected ranges
		# not including unbounded records that surround the window
		# if include these would need to compare between records to characterise closures.
		if parameters['record_start'] and parameters['record_end']:
			filters.append(
				' ('
				'	( '
				'	CASE WHEN record.time IS NOT NULL THEN record.time ELSE $record_start END >= $record_start '
				'	AND '
				'	CASE WHEN record.time IS NOT NULL THEN record.time ELSE $record_end END <= $record_end '
				'	) OR ( '
				'	CASE WHEN record.start <> False THEN record.start ELSE $record_end END <= $record_end '
				'	AND '
				'	CASE WHEN record.end <> False THEN record.end ELSE $record_start END >= $record_start '
				'	) OR ( '
				'	record.start = False '
				'	AND '
				'	CASE WHEN record.end <> False THEN record.end ELSE $record_start END >= $record_start '
				'	AND '
				'	CASE WHEN record.end <> False THEN record.end ELSE $record_end END <= $record_end'
				'	) OR ( '
				'	CASE WHEN record.start <> False THEN record.start ELSE $record_end END <= $record_end '
				'	AND '
				'	CASE WHEN record.start <> False THEN record.start ELSE $record_start END >= $record_start '
				'	AND '
				'	record.end = FALSE '
				'	) '
				') '
			)
		elif parameters['record_start']:
			filters.append(
				' ( '
				'	CASE WHEN record.time IS NOT NULL THEN record.time ELSE $record_start END >= $record_start '
				'	OR '
				'	CASE WHEN record.end <> False THEN record.end ELSE $record_start END >= $record_start '
				'	OR '
				'	CASE WHEN record.start <> False THEN record.start ELSE $record_start END >= $record_start '
				' ) '
			)
		elif parameters['record_end']:
			filters.append(
				' ( '
				'	CASE WHEN record.time IS NOT NULL THEN record.time ELSE $record_end END < $record_end '
				'	OR '
				'	CASE WHEN record.end <> False THEN record.end ELSE $record_end END <= $record_end '
				'	OR '
				'	CASE WHEN record.start <> False THEN record.start ELSE $record_end END < $record_end '
				' ) '
			)
		if parameters['item_level']:
			# match item_level label, has title case
			# as we are string building here rather than using parameters we need to be sure we aren't allowing injection
			# so i do a quick check on the item_level to make sure it conforms to our expectations
			# this is already done at the form level but in case this constructor gets exposed elsewhere I will do it here too
			if parameters['item_level'].title() in ['Field', 'Block', 'Tree', 'Sample']:
				filters.append(
					' item: ' + parameters['item_level'].title()
				)
		if parameters['country']:
			filters.append(
				' country.name_lower = toLower(trim($country)) '
			)
		if parameters['region']:
			filters.append(
				' region.name_lower = toLower(trim($region)) '
			)
		if parameters['farm']:
			filters.append(
				' farm.name_lower = toLower(trim($farm)) '
			)
		if parameters['replicate_id_list']:
			filters.append(
				' record.replicate IN $replicate_id_list '
			)
		if filters:
			statement += (
				' WHERE '
			)
			statement += ' AND '.join(filters)
		statement += (
			' OPTIONAL MATCH (item)-[: FROM*]->(sample_sample: Sample) '
			' OPTIONAL MATCH (item)-[: FROM*]->(sample_tree: Tree) '
			' OPTIONAL MATCH (sample_tree)-[:IS_IN]->(:BlockTrees) '
			'	-[:IS_IN]->(sample_tree_block: Block) '
			' OPTIONAL MATCH (sample_tree)-[:IS_IN]->(:FieldTrees) '
			'	-[:IS_IN]->(sample_tree_field: Field) '
			' OPTIONAL MATCH (item)-[: FROM*]->(sample_field: Field) '
			' OPTIONAL MATCH (item)-[: IS_IN]->(: BlockTrees) '
			'	-[: IS_IN]->(tree_block: Block) '
			' OPTIONAL MATCH (item)-[: IS_IN]->(: FieldTrees) '
			'	-[: IS_IN]->(tree_field: Field) '
			' OPTIONAL MATCH (item)-[: IS_IN]->(: FieldBlocks) '
			'	-[: IS_IN]->(block_field: Field) '
			' WITH '
			'	record_type.name_lower as record_type, '
			'	feature.name as Feature, '
			'	partner.name as Partner, '
			'	user.name as `Submitted by`, '
			'	item, '
			'	item.uid as UID, '
			'	item.id as ID, '
			'	item.custom_id as `Custom ID`,'
			'	COLLECT(DISTINCT sample_sample.id) as `Source samples`, '
			'	COLLECT(DISTINCT sample_tree.id) as `Source trees`, '
			'	COALESCE( '
			'		CASE WHEN item: Block THEN item.name ELSE Null END, '
			'		tree_block.name, '
			'		COLLECT(DISTINCT sample_tree_block.name) '
			'	) as Block, '
			'	COALESCE( '
			'		CASE WHEN item: Block THEN item.id ELSE Null END, '
			'		tree_block.id, '
			'		COLLECT(DISTINCT sample_tree_block.id) '
			'	) as `Block ID`, '
			'	COALESCE ( '
			'		CASE WHEN item: Field THEN item.name ELSE Null END, '
			'		sample_tree_field.name, '
			'		sample_field.name, '
			'		tree_field.name, '
			'		block_field.name '
			'	) as Field, '
			'	COALESCE ( '
			'		CASE WHEN item: Field THEN item.uid ELSE Null END, '
			'		sample_tree_field.uid, '
			'		sample_field.uid, '
			'		tree_field.uid, '
			'		block_field.uid '
			'	) as `Field UID`, '
			'	farm.name as Farm, '
			'	region.name as Region, '
			'	country.name as Country, '
		)
		if data_format == 'db':
			statement += (
				'	submitted.time as `Submitted at`, '
				'	record.replicate as Replicate, '
				'	COALESCE( '
				'		record.value, '
				'	[i in range(0, size(record.x_values) - 1 ) | [record.x_values[i], record.y_values[i]]] '
				'	) as Value, '
				'	record.time as Time, '
				'	record.start as Start, '
				'	record.end as End, '
				'	record.person as `Recorded by` '
			)
		else:  # data_format == 'table'
			statement += (
				' COLLECT(DISTINCT(COALESCE('
				'	record.value, '
				'	[i in range(0, size(record.x_values) - 1 ) | [record.x_values[i], record.y_values[i]]] '
				' ))) '
				' as Values '
			)
		with_filters = []
		if parameters['field_uid']:
			with_filters.append(
				' ( '
				'	`Field UID` = $field_uid '
				' ) '
			)
		if parameters['block_uid']:
			with_filters.append(
				' ( '
				'	`Block ID` = toInteger(split($block_uid, "_")[1]) '
				' ) '
			)
		if parameters['tree_id_list']:
			with_filters.append(
				' ( '
				'	any(x IN `Source trees` WHERE x IN $tree_id_list) '
				'	OR '
				'	(	'
				'		item: Tree '
				'		AND '
				'		item.id IN $tree_id_list '
				'	) '
				' ) '
			)
		if parameters['sample_id_list']:
			with_filters.append(
				' (	'
				'	item: Sample '
				'	AND '
				'	item.id IN $sample_id_list '
				' ) '
			)
		if with_filters:
			statement += (
				' WHERE '
			)
			statement += ' AND '.join(with_filters)
		if data_format == 'db':
			statement += (
				' WITH { '
				'	record_type: record_type, '
				'	Feature: Feature, '
				'	Partner: Partner, '
				'	`Submitted by`: `Submitted by`, '
				'	`Submitted at`: `Submitted at`, '
				'	Value: Value, '
				'	Time: Time, '
				'	Period: [Start, End], '
				'	`Recorded by`: `Recorded by`, '
				'	UID: UID, '
				'	`Custom ID`: `Custom ID`,'
				'	Replicate: Replicate, '
				'	`Source samples`: `Source samples`, '
				'	`Source trees`: `Source trees`, '
				'	Block: Block, '
				'	`Block ID`: `Block ID`, '
				'	Field: Field, '
				'	`Field UID`: `Field UID`, '
				'	Farm: Farm, '
				'	Region: Region, '
				'	Country: Country, '
				'	ID: ID'
				' } as result'
				' RETURN result '
				' ORDER BY '
				'	result["Feature"], '
				'	CASE '
				'		WHEN result["Field UID"] IS NOT NULL THEN result["Field UID"] '
				'		ELSE result["UID"] END, '
				'	result["ID"], '
				'	result["Replicate"] '
			)
		else:
			statement += (
				' WITH { '
				'	Records: collect({'
				'		feature_name: Feature, '
				'		record_type: record_type, '
				'		values: Values '
				'	}), '
				'	UID: UID, '
				'	`Custom ID`: `Custom ID`,'
				'	`Source samples`: `Source samples`, '
				'	`Source trees`: `Source trees`, '
				'	Block: Block, '
				'	`Block ID`: `Block ID`, '
				'	Field: Field, '
				'	`Field UID`: `Field UID`, '
				'	Farm: Farm, '
				'	Region: Region, '
				'	Country: Country, '
				'	ID: ID'
				' } as result '
				' RETURN result '
				' ORDER BY '
				'	CASE '
				'		WHEN result["Field UID"] IS NOT NULL THEN result["Field UID"] '
				'		ELSE result["UID"] END, '
				'	result["ID"] '
			)
		return statement

	def collect_records(
			self,
			parameters,
			data_format
	):
		statement = (
			' MATCH '
			'	(: User {username_lower: toLower($username)}) '
			'	-[: AFFILIATED {confirmed: True}]->(partner: Partner) '
			' MATCH '
			'	(partner)'
			'	<-[: AFFILIATED {data_shared: True}]-(user: User) '
		)
		statement += self.user_record_query(parameters, data_format)
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				bolt_result,
				statement,
				parameters
			)
		return result

	@staticmethod
	def format_record(record, data_format='db'):
		if data_format == 'table':
			for f, feature in enumerate(record[0]['Records']):
				for v, value in enumerate(feature['values']):
					# flatten each value to string if it is a list
					if isinstance(value, list):
						if len(value) > 1:
							for i, j in enumerate(value):
								if isinstance(j, (float, int)):
									record[0]['Records'][f]['values'][v][i] = str(j)
								if isinstance(j, list):
									if len(j) > 1:
										for ji, jj in enumerate(j):
											if isinstance(jj, (float, int)):
												record[0]['Records'][f]['values'][v][i][ji] = str(jj)
										record[0]['Records'][f]['values'][v][i] = '[' + ', '.join(
											[jl.encode('utf8') for jl in record[0]['Records'][f]['values'][v][i]]
										) + ']'
							record[0]['Records'][f]['values'][v] = ', '.join(
								[l.encode('utf8') for l in record[0]['Records'][f]['values'][v]]
							)
						else:
							record[0]['Records'][f]['values'][v] = record[0]['Records'][f]['values'][v][0]
				# then flatten the list of values to a string stored in the record dict
				if len(record[0]['Records'][f]['values']) > 1:
					for v, value in enumerate(record[0]['Records'][f]['values']):
						if isinstance(value, (float, int)):
							record[0]['Records'][f]['values'][v] = str(value)
					record[0][feature['feature_name']] = ', '.join(
						[value.encode('utf8') for value in record[0]['Records'][f]['values']]
					)
				elif record[0]['Records'][f]['values']:
					record[0][feature['feature_name']] = record[0]['Records'][f]['values'][0]
		else:  # data_format == 'db'
			for key in record[0]:
				if key == "Period":
					if record[0]['record_type'] != 'condition':
						record[0][key] = None
					if record[0][key]:
						if record[0][key][0]:
							record[0][key][0] = datetime.utcfromtimestamp(record[0][key][0] / 1000).strftime(
								"%Y-%m-%d %H:%M")
						else:
							record[0][key][0] = 'Undefined'
						if record[0][key][1]:
							record[0][key][1] = datetime.utcfromtimestamp(record[0][key][1] / 1000).strftime(
								"%Y-%m-%d %H:%M")
						else:
							record[0][key][1] = 'Undefined'
						record[0][key] = ' - '.join(record[0][key])
				elif key == 'Time' and record[0][key]:
					record[0][key] = datetime.utcfromtimestamp(record[0][key] / 1000).strftime("%Y-%m-%d %H:%M")
				elif key == 'Submitted at':
					record[0][key] = datetime.utcfromtimestamp(record[0][key] / 1000).strftime("%Y-%m-%d %H:%M:%S")
		for key in record[0]:
			if isinstance(record[0][key], list):
				if not record[0][key]:
					if isinstance(record[0][key], list):
						record[0][key] = None
				else:
					if len(record[0][key]) > 1:
						for i, j in enumerate(record[0][key]):
							if isinstance(j, (float, int)):
								record[0][key][i] = str(j)
							if isinstance(j, list):
								for ii,jj in enumerate(j):
									if isinstance(jj, (float, int)):
										record[0][key][i][ii] = str(jj)
								record[0][key][i] = '[' + ', '.join([l.encode('utf8') for l in record[0][key][i]]) + ']'
						record[0][key] = ', '.join([str(i).encode('utf8') for i in record[0][key]])
					else:
						record[0][key] = record[0][key][0]
		return record

	def records_to_file(self, result, data_format, file_type):
		# check if any data found, if not return none
		first_result = result.peek()
		if not first_result:
			return {
				'status': 'SUCCESS',
				'result': 'No records found to match your filters'
			}
		# prepare the file
		base_filename = 'records'
		item_fieldnames = [
			'Country',
			'Region',
			'Farm',
			'Field',
			'Field UID',
			'Block',
			'Block ID',
			'Source trees',
			'Source samples',
			'Custom ID',
			'UID',
			'Replicate'
		]
		fieldnames = [i for i in item_fieldnames if i in first_result[0].keys()]
		if data_format == 'table':
			features = [i['feature_name'] for i in first_result[0]['Records']]
			fieldnames += features
		else:
			fieldnames += [
				'Feature',
				'Value',
				'Time',
				'Period',
				'Recorded by',
				'Submitted at',
				'Submitted by',
				'Partner'
			]
		if file_type == 'xlsx':
			file_path = self.get_file_path(
				'xlsx',
				base_filename
			)
			wb = Workbook(file_path)
			worksheet = wb.add_worksheet('Records')
			row_number = 0
			for i, j in enumerate(fieldnames):
				worksheet.write(row_number, i, j)
			# collect a set of used fields so can remove columns that don't contain data
			used_fields = set()
			for record in result:
				record = self.format_record(record, data_format)
				row_number += 1
				col_number = 0
				for field in fieldnames:
					if field in record[0]:
						if record[0][field]:
							used_fields.add(field)
						worksheet.write(row_number, col_number, record[0][field])
					col_number += 1
			# hide columns not written to,
			# if we want to actually delete them we need to move to using openpyxl instead of xlsxwriter
			for i, field in enumerate(fieldnames):
				if field not in used_fields:
					worksheet.set_column(i, i, None, None, {'hidden': True})
			wb.close()
		else:  # file_type == 'csv':
			file_path = self.get_file_path(
				'csv',
				base_filename
			)
			with open(file_path, 'w') as csv_file:
				writer = csv.DictWriter(
					csv_file,
					fieldnames=fieldnames,
					quoting=csv.QUOTE_ALL,
					extrasaction='ignore')
				writer.writeheader()
				for record in result:
					record = self.format_record(record, data_format)
					writer.writerow(record[0])
		download_url = url_for(
						"download_file",
						username=self.username,
						filename= os.path.basename(file_path),
						_external=True
			)
		self.file_list.append({
			"filename": os.path.basename(file_path),
			"file_path": file_path,
			"file_size": os.path.getsize(file_path),
			"url": download_url
		})
		if self.email_requested:
			subject = "BreedCAFS download"
			recipients = [User(self.username).find('')['email']]
			body = "Your data is available at the following address."
			html = render_template(
				'emails/generate_files.html',
				file_list=[download_url]
			)
			send_email(subject, app.config['ADMINS'][0], recipients, body, html)
			return {
				'status': 'SUCCESS',
				'result': (
						'<p>Your data is available for <a href= " ' + download_url + '">download</a> '
						'and this link has been sent to your email address.'
				)
			}
		return {
			'status': 'SUCCESS',
			'result': '<p>Your data is available for <a href= " ' + download_url + '">download</a></p> '
		}
