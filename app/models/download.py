from app import app
import os
from .neo4j_driver import (
	get_driver,
	list_records,
	single_record
)
from flask import (
	url_for,
	render_template
)

from app.models import(
	SelectionList,
	AddFieldItems,
	ItemList,
	User
)

from app.emails import send_email

import csv

from datetime import datetime

from xlsxwriter import Workbook, utility

import logging

logger = logging.getLogger(__name__)


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
		self.inputs = {}
		self.item_reference_details = []
		for record_type in app.config['RECORD_TYPES']:
			self.inputs[record_type] = []
		self.file_list = []
		# create user download path if not found
		self.user_download_folder = os.path.join(app.config['EXPORT_FOLDER'], username)
		if not os.path.isdir(self.user_download_folder):
			os.mkdir(self.user_download_folder, mode=app.config['EXPORT_FOLDER_PERMISSIONS'])
		# prepare variables to write the file
		self.time = datetime.utcnow().strftime('%Y%m%d-%H%M%S')

	def register_samples(
			self,
			level,
			country,
			region,
			farm,
			field_uid,
			field_list,
			block_uid,
			block_list,
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
			field_list,
			block_uid,
			block_list,
			tree_id_list,
			sample_id_list,
			per_item_count
		)
		if id_list:
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
		if id_list:
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
			'Tree Name',
			'Tree Names',
			'Source Sample IDs',
			'Source Sample Names',
			'Varieties',
			'Unit',
			'Name',
			'Row',
			'Column',
			'UID'
		]
		self.item_fieldnames = [i for i in fieldnames_order if i in list(self.id_list[0].keys())]

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

	def find_input_group_id(
			self,
			input_group_name
	):
		parameters = {
			'input_group_name': input_group_name,
			'username': self.username
		}
		statement = (
			' MATCH (ig: InputGroup { '
			'	name_lower: toLower($input_group_name) '
			' }) '
			' OPTIONAL MATCH (igs:InputGroups)-[:SUBMITTED]->(ig) '
			' WITH ig, igs '
			' MATCH '
			'	(:User {username_lower: $username})-[:AFFILIATED {data_shared: True}]->(partner: Partner) '
			' OPTIONAL MATCH '
			'	(partner) '
			'	<-[:AFFILIATED {data_shared: True}]-(user:User) '
			'	-[:SUBMITTED]->(:Submissions)'
			'	-[:SUBMITTED]->(igs) '
			' WITH ig, igs WHERE igs IS NULL OR (igs IS NOT NULL AND user IS NOT NULL) '
			' WITH ig ORDER BY igs LIMIT 1 '
			' RETURN ig.id '
		)
		with get_driver().session() as neo4j_session:
			return neo4j_session.read_transaction(
				single_record,
				statement,
				**parameters
			)

	def set_inputs(
			self,
			item_level,
			record_type=None,
			input_group=None,
			inputs=None
	):
		if not record_type:
			record_types = app.config['RECORD_TYPES']
		else:
			record_types = [record_type]
		for rt in record_types:
			self.inputs[rt] = SelectionList.get_inputs(
				input_group=input_group,
				item_level=item_level,
				record_type=rt,
				details=True
			)

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
		self.set_inputs(
			record_data['item_level'],
			input_group=record_data['input_group'] if 'input_group' in record_data else None,
			inputs=record_data['selected_inputs'] if 'selected_inputs' in record_data else None
		)
		if not any(self.inputs.values()):
			return False
		if record_data['template_format'] == 'fb':
			self.make_fb_template()
		else:
			self.id_list_to_xlsx_template(
				base_filename=record_data['item_level']
			)
		return True

	def write_ids_to_row(
			self,
			worksheet,
			row_num,
			uid,
			item_reference_details_dict
	):
		for index, key in enumerate(self.item_reference_details):
			worksheet.write(
				row_num, index, item_reference_details_dict[key]
			)
		worksheet.write(
			row_num, len(self.item_reference_details), uid
		)

	def write_item_to_worksheet(
			self,
			record_type,
			worksheet,
			item_details,
			item_num,
			item_reference_details_dict
	):
		if self.replicates and record_type in app.config['REPLICATED_RECORD_TYPES']:
			replicate_list = ['.'.join([str(item_details['UID']), str(i)]) for i in range(1, self.replicates + 1)]
			row_num = ((item_num - 1) * (self.replicates * self.time_points)) + 1
		else:
			replicate_list = [str(item_details['UID'])]
			row_num = ((item_num - 1) * self.time_points) + 1
		time_points = self.time_points if record_type != 'property' else 1
		for uid in replicate_list:
			for time_point in range(0, time_points):
				self.write_ids_to_row(
					worksheet,
					row_num,
					uid,
					item_reference_details_dict
				)
				row_num += 1

	def id_list_to_xlsx_template(
			self,
			base_filename=None
	):
		if not self.id_list and self.item_level and any(self.inputs.values()):
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
		# set the formatting for the input columns
		numeric_format = wb.add_format({'num_format': ''})
		date_format = wb.add_format({'num_format': 'yyyy-mm-dd'})
		text_format = wb.add_format({'num_format': '@'})
		# This converts the percent to a number i.e. 10% to 0.1, prefer not to use it, just store the number
		# percent_format = wb.add_format({'num_format': 9})
		location_format = wb.add_format({'num_format': '0.0000; 0.0000'})
		input_formats = {
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
		item_details_keys = list(self.id_list[0].keys())
		if 'Column' in item_details_keys:
			self.item_reference_details.insert(0, 'Column')
			for i in list(core_fields_formats.keys()):
				core_fields_formats[i].insert(0, ('Column', right_border))
		if 'Row' in item_details_keys:
			self.item_reference_details.insert(0, 'Row')
			for i in list(core_fields_formats.keys()):
				core_fields_formats[i].insert(0, ('Row', right_border))
		if 'Name' in item_details_keys:
			self.item_reference_details.insert(0, 'Name')
			for i in list(core_fields_formats.keys()):
				core_fields_formats[i].insert(0, ('Name', right_border))
		# Create worksheets
		# Write headers and set formatting on columns
		# Store worksheet in a dict by record type to later write values when iterating through id_list
		ws_dict = {
			'property': None,
			'trait': None,
			'condition': None,
			'item_details': wb.add_worksheet(app.config['WORKSHEET_NAMES']['item_details']),
			'input_details': wb.add_worksheet(app.config['WORKSHEET_NAMES']['input_details']),
			'hidden': wb.add_worksheet("hidden")
		}
		# We add a new worksheet for each "curve" input
		for i in self.inputs["curve"]:
			ws_dict[i['name_lower']] = None
		# write the item_details header
		for i, j in enumerate(self.item_fieldnames):
			ws_dict['item_details'].write(0, i, j, header_format)
		input_details_fieldnames = [
			'type',
			'name',
			'format',
			'minimum',
			'maximum',
			'details',
			'category_list'
		]
		# write the input_details header
		for header in input_details_fieldnames:
			column_number = input_details_fieldnames.index(header)
			ws_dict['input_details'].write(0, column_number, header, header_format)
		# write the core fields to the input details page
		input_details_row_num = 0
		core_fields_details = [
			("name", "Pre-filled: If a name is registered for the item it will be here "),
			("row", "Pre-filled: If a row is registered for the item it will be here "),
			("column", "Pre-filled: If a column is registered for the item it will be here "),
			("uid", "Pre-filled: Unique identifier for the item"),
			("person", "Optional: Person responsible for determining these values")
		]
		if self.inputs['trait'] or self.inputs['curve']:
			core_fields_details += [
				("date", "Required: Date these values were determined (YYYY-MM-DD, e.g. 2017-06-01)"),
				("time", "Optional: Time these values were determined (24hr, e.g. 13:00. Defaults to 12:00")
			]
		if self.inputs['condition']:
			core_fields_details += [
				("start date", "Optional: Date this condition started (YYYY-MM-DD, e.g. 2017-06-01)"),
				("start time", "Optional: Time this condition started (24hr, e.g. 13:00. Defaults to 00:00"),
				("end date", "Optional: Date this condition ended (YYYY-MM-DD, e.g. 2017-06-01)"),
				(
					"end time",
					"Optional: Time this condition ended (24hr, e.g. 13:00. Defaults to 00:00 of the following day"
				)
			]
		if self.inputs['curve']:
			core_fields_details += [
				(
					"Curve worksheets",
					"X-values/independent variables as column headers, Y-Values/dependent variables in rows below"
				)
			]
		for field, details in core_fields_details:
			input_details_row_num += 1
			ws_dict['input_details'].write(input_details_row_num, 1, field)
			ws_dict['input_details'].write(input_details_row_num, 5, details)
		# empty row to separate date/time/person from inputs
		categorical_inputs_count = 0
		for record_type in [i for i in app.config['RECORD_TYPES'] if i in self.inputs]:
			if all([
				self.inputs[record_type],
				record_type != 'curve'
			]):
				# create record type specific worksheet
				ws_dict[record_type] = wb.add_worksheet(app.config['WORKSHEET_NAMES'][record_type])
				# write headers:
				# - add the core fields (e.g. person, date, time)
				for i, field in enumerate(core_fields_formats[record_type]):
					ws_dict[record_type].set_column(i, i, None, cell_format=field[1])
				# - add the input field names
				fieldnames = (
						[field[0] for field in core_fields_formats[record_type]]
						+ [i['name'] for i in self.inputs[record_type]]
				)
				for i, fieldname in enumerate(fieldnames):
					ws_dict[record_type].write(0, i, fieldname, header_format)
				# - set right border formatting on the last column of record type specific worksheet
				ws_dict[record_type].set_column(
					len(fieldnames) - 1, len(fieldnames) - 1, None, cell_format=right_border
				)
			if self.inputs[record_type] and record_type == 'curve':
				for input_type in self.inputs[record_type]:
					# Need to ensure curve input names do not contain "[]:*?/\" or excel can't use them as sheetnames
					ws_dict[input_type['name_lower']] = wb.add_worksheet(input_type['name'])
					for i, field in enumerate(core_fields_formats[record_type]):
						ws_dict[input_type['name_lower']].set_column(i, i, None, cell_format=field[1])
					# - add the input field names
					fieldnames = (
							[field[0] for field in core_fields_formats[record_type]]
					)
					for i, fieldname in enumerate(fieldnames):
						ws_dict[input_type['name_lower']].write(0, i, fieldname, header_format)
					ws_dict[input_type['name_lower']].set_column(
						len(fieldnames) - 1, len(fieldnames) - 1, None, cell_format=right_border
					)
			# write input details into input details sheet
			for input_type in self.inputs[record_type]:
				input_type['type'] = str(record_type)
				input_details_row_num += 1
				for j, field in enumerate(input_details_fieldnames):
					if field in input_type:
						if isinstance(input_type[field], list):
							value = ", ".join(value for value in input_type[field])
							ws_dict['input_details'].write(input_details_row_num, j, value)
						else:
							ws_dict['input_details'].write(input_details_row_num, j, input_type[field])
		if self.inputs['property']:
			inputs_properties = app.config['INPUTS_PROPERTIES']
			input_names_lower = [input_variable['name_lower'] for input_variable in self.inputs['property']]
		# iterate through id_list and write to worksheets
		item_num = 0
		for record in self.id_list:
			item_num += 1
			# construct a dict to pass the values for item reference details
			item_reference_details_dict = {}
			if self.item_reference_details:
				for key in self.item_reference_details:
					item_reference_details_dict[key] = record[key]
			# if there is a list (or nested lists) stored in this item
			# make sure it is returned as a list of strings
			for key, value in record.items():
				if isinstance(value, list):
					value = ", ".join([str(i) for i in value])
				if key in self.item_fieldnames:
					item_details_column_number = self.item_fieldnames.index(key)
					ws_dict['item_details'].write(item_num, item_details_column_number, value)
			for record_type in list(self.inputs.keys()):
				if self.inputs[record_type]:
					if record_type in ['property', 'trait', 'condition']:
						self.write_item_to_worksheet(
							record_type,
							ws_dict[record_type],
							record,
							item_num,
							item_reference_details_dict
						)
						if record_type == 'property':
							# We can pre-fill property templates with found values to present existing records to users
							# and also allow them to confirm inferred values
							for input_variable in self.inputs[record_type]:
								if input_variable['name_lower'] in inputs_properties:
									input_column = (
											input_names_lower.index(input_variable['name_lower']) +
											len(self.item_reference_details) + # o.e optional fields like name, row, column
											2  # shift from 0 based to 1 based indexing +  column for UID
									)
									# +3 for [Name/UID/Person] offset
									if inputs_properties[input_variable['name_lower']] == 'name':
										if 'Name' in record and record['Name']:
											name = record['Name']
											ws_dict[record_type].write(item_num, input_column, name)
									elif inputs_properties[input_variable['name_lower']] == "variety":
										if 'Varieties' in record and record['Varieties'] and len(record['Varieties']) == 1:
											variety = record['Varieties'][0]
											if 'name' in input_variable['name_lower']:
												ws_dict[record_type].write(item_num, input_column, variety)
											else:
												if 'el frances' in input_variable['name_lower']:
													code_system = 'el frances'
												else:
													code_system = None
												if code_system and code_system in app.config['SYSTEM_VARIETY_CODE']:
													if variety in app.config['SYSTEM_VARIETY_CODE'][code_system]:
														ws_dict[record_type].write(
															item_num,
															input_column,
															app.config['SYSTEM_VARIETY_CODE'][code_system][variety]
														)
									elif inputs_properties[input_variable['name_lower']] == "unit":
										if 'Unit' in record and record['Unit']:
											unit = record['Unit']
											ws_dict[record_type].write(item_num, input_column, unit)
									elif inputs_properties[input_variable['name_lower']] == "elevation":
										if 'Elevation' in record and record['Elevation']:
											elevation = record['Elevation']
											ws_dict[record_type].write(item_num, input_column, elevation)
									elif inputs_properties[input_variable['name_lower']] == "time":
										if 'Time' in record and record['Time']:
											time = record['Time']
											if 'date' in input_variable['name_lower']:
												date = time.split(' ')[0]
												ws_dict[record_type].write(item_num, input_column, date)
											else:  # time
												time = time.split(' ')[1]
												ws_dict[record_type].write(item_num, input_column, time)
									elif inputs_properties[input_variable['name_lower']] == "source":
										if 'assign tree' in input_variable['name_lower']:
											if 'by name' in input_variable['name_lower']:
												if 'Block' in record and record['Block']:
													block = record['Block']
													ws_dict[record_type].write(item_num, input_column, block)
											else:   # by code in input variable['name_lower']:
												if 'Block ID' in record and record['Block ID']:
													block = record['Block ID']
													ws_dict[record_type].write(item_num, input_column, block)
										else:  # 'assign sample' in input_variable['name_lower']
											if 'by name' in input_variable['name_lower']:
												if 'source_level' in record and record['source_level'] == 'Block':
													blocks = ', '.join([str(i) for i in record['Blocks']])
													ws_dict[record_type].write(item_num, input_column, blocks)
											else:  # by id
												if 'to block' in input_variable['name_lower']:
													if 'source_level' in record and record['source_level'] == 'Block':
														if 'Block IDs' in record and record['Block IDs']:
															blocks = ', '.join([str(i) for i in record['Block IDs']])
															ws_dict[record_type].write(item_num, input_column, blocks)
												elif 'to tree' in input_variable['name_lower']:
													if 'source_level' in record and record['source_level'] == 'Tree':
														if 'Tree IDs' in record and record['Tree IDs']:
															trees = ', '.join([str(i) for i in record['Tree IDs']])
															ws_dict[record_type].write(item_num, input_column, trees)
												elif 'to sample' in input_variable['name_lower']:
													if 'source_level' in record and record['source_level'] == 'Sample':
														if 'source_ids' in record and record['Source Sample IDs']:
															source_samples = ', '.join([str(i) for i in record['source_ids']])
															ws_dict[record_type].write(item_num, input_column, source_samples)
					else:  # i.e. record_type == 'curve', where worksheet is named after input
						for input_variable in self.inputs[record_type]:
							self.write_item_to_worksheet(
								record_type,
								ws_dict[input_variable['name_lower']],
								record,
								item_num,
								item_reference_details_dict
							)
		# now that we know the item_num we can add the validation
		# currently no validation for curves as no definite columns
		for record_type in list(self.inputs.keys()):
			if self.inputs[record_type] and record_type != 'curve':
				if record_type in ['trait', 'curve'] and self.replicates:
					row_count = item_num * self.replicates * self.time_points
				else:
					row_count = item_num * self.time_points
				for i, field in enumerate(self.inputs[record_type]):
					col_num = len(core_fields_formats[record_type]) + i
					ws_dict[record_type].set_column(
						col_num, col_num, None, cell_format=input_formats[field['format']]
					)
					if 'category_list' in field and field['category_list']:
						categorical_inputs_count += 1
						ws_dict['hidden'].write((categorical_inputs_count - 1), 0, field['name_lower'])
						for j, category in enumerate(field['category_list']):
							ws_dict['hidden'].write((categorical_inputs_count - 1), j + 1, category)
						ws_dict[record_type].data_validation(1, col_num, row_count, col_num, {
							'validate': 'list',
							'source': (
								'=hidden!$B$' + str(categorical_inputs_count)
								+ ':$' + utility.xl_col_to_name(len(field['category_list']) + 1)
								+ '$' + str(categorical_inputs_count)
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
				_external=True,
				_scheme="https"
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
				_external=True,
				_scheme="https"
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
		input_fieldnames = [
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
		for i, input_type in enumerate(self.inputs['trait']):
			input_type['realPosition'] = str(i + 1)
			input_type['isVisible'] = 'True'
		self.make_csv_file(
			input_fieldnames,
			self.inputs['trait'],
			base_filename=base_filename,
			with_timestamp=with_timestamp,
			file_extension='trt'
		)
		# TODO may need to replace 'name' with 'trait' in header but doesn't seem to affect Field-Book

	def make_csv_table_template(
			self,
			fieldnames,
			id_list,
			inputs,
			base_filename=None,
			with_timestamp=True
	):
		fieldnames += ['Date', 'Time', 'Person']
		fieldnames += [input_type['name'] for input_type in inputs]
		self.make_csv_file(
			fieldnames,
			id_list,
			base_filename=base_filename + '_data',
			with_timestamp=with_timestamp
		)
		# and a file that describes the input details
		input_fieldnames = [
			'name',
			'format',
			'minimum',
			'maximum',
			'details',
			'category_list'
		]
		self.make_csv_file(
			input_fieldnames,
			inputs,
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
			'	-[: SUBMITTED]->(: UserFieldInput) '
			'	-[submitted: SUBMITTED]->(record: Record) '
			'	-[:RECORD_FOR]->(item_input:ItemInput) '
			'	-[:FOR_INPUT*..2]->(input:Input) '
			'	-[:OF_TYPE]->(record_type:RecordType), '
			'	(item_input) '
			'	-[:FOR_ITEM]->(item:Item) '
			'	-[:FROM | IS_IN *]->(farm: Farm) '
			'	-[:IS_IN]->(region: Region) '
			'	-[:IS_IN]->(country: Country) '
		)
		filters = []
		if parameters['selected_inputs']:
			filters.append(
				' input.name_lower IN $selected_inputs '
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
			' OPTIONAL MATCH (item)-[: FROM*]->(sample_samples: Sample) '
			' OPTIONAL MATCH (item)-[: FROM*]->(sample_trees: Tree) '
			' OPTIONAL MATCH (item)-[: FROM | IS_IN*]->(sample_blocks: Block) '
			' OPTIONAL MATCH (item)-[: FROM | IS_IN*]->(sample_field: Field) '
			' OPTIONAL MATCH (item)-[: IS_IN]->(: BlockTrees) '
			'	-[: IS_IN]->(tree_block: Block) '
			' OPTIONAL MATCH (item)-[: IS_IN]->(: FieldTrees) '
			'	-[: IS_IN]->(tree_field: Field) '
			' OPTIONAL MATCH (item)-[: IS_IN]->(: FieldBlocks) '
			'	-[: IS_IN]->(block_field: Field) '
			' WITH '
			'	record_type.name_lower as record_type, '
			'	input.name as Input, '
			'	partner.name as Partner, '
			'	user.name as `Submitted by`, '
			'	item, '
			'	item.uid as UID, '
			'	item.id as ID, '
			'	item.name as Name,'
			'	COLLECT(DISTINCT sample_samples.id) as `Source samples`, '
			'	COLLECT(DISTINCT sample_trees.id) as `Source trees`, '
			'	COALESCE( '
			'		CASE WHEN item: Block THEN item.name ELSE Null END, '
			'		tree_block.name, '
			'		COLLECT(DISTINCT sample_blocks.name) '
			'	) as Block, '
			'	COALESCE( '
			'		CASE WHEN item: Block THEN item.id ELSE Null END, '
			'		tree_block.id, '
			'		COLLECT(DISTINCT sample_blocks.id) '
			'	) as `Block ID`, '
			'	COALESCE ( '
			'		CASE WHEN item: Field THEN item.name ELSE Null END, '
			'		sample_field.name, '
			'		tree_field.name, '
			'		block_field.name '
			'	) as Field, '
			'	COALESCE ( '
			'		CASE WHEN item: Field THEN item.uid ELSE Null END, '
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
				'	toInteger(split($block_uid, "_B")[1]) IN ([] + `Block ID`) '
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
				' RETURN '
				'	record_type, '
				'	Input as `Input variable`, '
				'	Partner, '
				'	`Submitted by`, '
				'	`Submitted at`, '
				'	Value, '
				'	Time, '
				'	[Start, End] as Period, '
				'	`Recorded by`, '
				'	UID, '
				'	Name,'
				'	Replicate, '
				'	`Source samples`, '
				'	`Source trees`, '
				'	Block, '
				'	`Block ID`, '
				'	Field, '
				'	`Field UID`, '
				'	Farm, '
				'	Region, '
				'	Country, '
				'	ID'
				' ORDER BY '
				'	"Input variable", '
				'	CASE '
				'		WHEN "Field UID" IS NOT NULL THEN "Field UID" '
				'		ELSE "UID" END, '
				'	"ID", '
				'	"Replicate" '
			)
		else:
			statement += (
				' RETURN '
				'	collect({'
				'		input_name: Input, '
				'		record_type: record_type, '
				'		values: Values '
				'	}) as Records, '
				'	UID, '
				'	Name,'
				'	`Source samples`, '
				'	`Source trees`, '
				'	Block, '
				'	`Block ID`, '
				'	Field, '
				'	`Field UID`, '
				'	Farm, '
				'	Region, '
				'	Country, '
				'	ID '
				' ORDER BY '
				'	CASE '
				'		WHEN "Field UID" IS NOT NULL THEN "Field UID" '
				'		ELSE "UID" END, '
				'	ID '
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
			' WITH partner '
			'	MATCH '
			'	(partner) '
			'	<-[: AFFILIATED {data_shared: True}]-(user: User) '
		)
		statement += self.user_record_query(parameters, data_format)
		with get_driver().session() as neo4j_session:
			return neo4j_session.read_transaction(
				list_records,
				statement,
				**parameters
			)

	@staticmethod
	def format_record(record, data_format='db'):
		if data_format == 'table':
			for f, input_type in enumerate(record['Records']):
				for v, value in enumerate(input_type['values']):
					# flatten each value to string if it is a list
					if isinstance(value, list):
						if len(value) > 1:
							for i, j in enumerate(value):
								if isinstance(j, (float, int)):
									record['Records'][f]['values'][v][i] = str(j)
								if isinstance(j, list):
									if len(j) > 1:
										for ji, jj in enumerate(j):
											if isinstance(jj, (float, int)):
												record['Records'][f]['values'][v][i][ji] = str(jj)
										record['Records'][f]['values'][v][i] = '[' + ', '.join(
											[jl for jl in record['Records'][f]['values'][v][i]]
										) + ']'
							record['Records'][f]['values'][v] = ', '.join(
								[l for l in record['Records'][f]['values'][v]]
							)
						else:
							record['Records'][f]['values'][v] = record['Records'][f]['values'][v][0]
				# then flatten the list of values to a string stored in the record dict
				if len(record['Records'][f]['values']) > 1:
					for v, value in enumerate(record['Records'][f]['values']):
						if isinstance(value, (float, int)):
							record['Records'][f]['values'][v] = str(value)
					record[input_type['input_name']] = ', '.join(
						[value for value in record['Records'][f]['values']]
					)
				elif record['Records'][f]['values']:
					record[input_type['input_name']] = record['Records'][f]['values'][0]
		else:  # data_format == 'db'
			for key in record:
				if key == "Period":
					if record['record_type'] != 'condition':
						record[key] = None
					if record[key]:
						if record[key][0]:
							record[key][0] = datetime.utcfromtimestamp(record[key][0] / 1000).strftime(
								"%Y-%m-%d %H:%M")
						else:
							record[key][0] = 'Undefined'
						if record[key][1]:
							record[key][1] = datetime.utcfromtimestamp(record[key][1] / 1000).strftime(
								"%Y-%m-%d %H:%M")
						else:
							record[key][1] = 'Undefined'
						record[key] = ' - '.join(record[key])
				elif key == 'Time' and record[key]:
					record[key] = datetime.utcfromtimestamp(record[key] / 1000).strftime("%Y-%m-%d %H:%M")
				elif key == 'Submitted at':
					record[key] = datetime.utcfromtimestamp(record[key] / 1000).strftime("%Y-%m-%d %H:%M:%S")
		for key in record:
			if isinstance(record[key], list):
				if not record[key]:
					if isinstance(record[key], list):
						record[key] = None
				else:
					if len(record[key]) > 1:
						for i, j in enumerate(record[key]):
							if isinstance(j, (float, int)):
								record[key][i] = str(j)
							if isinstance(j, list):
								for ii,jj in enumerate(j):
									if isinstance(jj, (float, int)):
										record[key][i][ii] = str(jj)
								record[key][i] = '[' + ', '.join([l for l in record[key][i]]) + ']'
						record[key] = ', '.join([str(i) for i in record[key]])
					else:
						record[key] = record[key][0]
		return record

	def records_to_file(self, result, data_format, file_type):
		# check if any data found, if not return none
		first_result = result[0]
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
			'Name',
			'UID',
			'Replicate'
		]
		fieldnames = [i for i in item_fieldnames if i in list(first_result.keys())]
		if data_format == 'table':
			inputs = [i['input_name'] for i in first_result['Records']]
			fieldnames += inputs
		else:
			fieldnames += [
				'Input variable',
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
			# collect a set of used fields so can remove columns that don't contain data
			used_fields = set()
			for record in result:
				# check if new fieldnames to add
				if data_format == 'table':
					for input_name in [input_type['input_name'] for input_type in record['Records']]:
						if input_name not in fieldnames:
							fieldnames.append(input_name)
				record = self.format_record(record, data_format)
				row_number += 1
				col_number = 0
				for field in fieldnames:
					if field in record:
						if record[field]:
							used_fields.add(field)
							worksheet.write(row_number, col_number, record[field])
					col_number += 1
			# hide columns not written to,
			# if we want to actually delete them we need to move to using openpyxl instead of xlsxwriter
			for i, j in enumerate(fieldnames):
				worksheet.write(0, i, j)
			for i, field in enumerate(fieldnames):
				if field not in used_fields:
					worksheet.set_column(i, i, None, None, {'hidden': True})
			wb.close()
		else:  # file_type == 'csv':
			file_path = self.get_file_path(
				'csv',
				base_filename
			)
			if data_format == 'table':
				file_path_temp = self.get_file_path(
					'csv',
					base_filename + '_temp'
				)
				with open(file_path_temp, 'w') as csv_temp_file:
					temp_writer = csv.DictWriter(
						csv_temp_file,
						fieldnames=fieldnames,
						quoting=csv.QUOTE_ALL,
						extrasaction='ignore')
					for record in result:
						for input_name in [input_type['input_name'] for input_type in record['Records']]:
							if input_name not in fieldnames:
								fieldnames.append(input_name)
						record = self.format_record(record, data_format)
						temp_writer.writerow(record)
				with open(file_path_temp, 'r') as csv_temp_file:
					with open(file_path, 'w') as csv_file:
						writer = csv.DictWriter(
							csv_file,
							fieldnames=fieldnames,
							quoting=csv.QUOTE_ALL,
							extrasaction='ignore'
						)
						writer.writeheader()
						csv_file.write(csv_temp_file.read())
				os.unlink(file_path_temp)
			else:
				with open(file_path, 'w') as csv_file:
					writer = csv.DictWriter(
						csv_file,
						fieldnames=fieldnames,
						quoting=csv.QUOTE_ALL,
						extrasaction='ignore'
					)
					writer.writeheader()
					for record in result:
						record = self.format_record(record, data_format)
						writer.writerow(record)
		download_url = url_for(
						"download_file",
						username=self.username,
						filename=os.path.basename(file_path),
						_external=True,
						_scheme="https"
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
