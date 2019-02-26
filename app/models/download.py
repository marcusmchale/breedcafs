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

from app.emails import send_email

import unicodecsv as csv

from datetime import datetime

from xlsxwriter import Workbook


# User class related (all uploads are tied to a user) yet more specifically regarding uploads
class Download:
	def __init__(self, username, email_requested=False):
		self.username = username
		self.email_requested = email_requested
		self.id_list = None
		self.replicates = None
		self.item_fieldnames = None
		self.item_level = None
		self.features = None
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

	def replicate_id_list(
			self,
			level,
			replicates
	):
		if all([
			replicates > 1,
			len(self.id_list) > 0
		]):
			id_list_reps = []
			for item in self.id_list:
				if level == 'field':
					item['Field UID'] = item['UID']
				elif level == 'block':
					item['Block UID'] = item['UID']
				elif level == 'tree':
					item['Tree UID'] = item['UID']
				elif level == 'sample':
					item['Sample UID'] = item['UID']
				for i in range(replicates):
					item['UID'] = str(item['UID']) + "." + str(int(i + 1))
					id_list_reps.append(item.copy())
			self.id_list = id_list_reps

	def set_item_fieldnames(self):
		fieldnames_order = [
			'Country',
			'Region',
			'Farm',
			'Field',
			'Field UID',
			'Block',
			'Block UID',
			'Tree UID',
			'Tree Custom ID',
			'Variety',
			'Parent Sample UID',
			'Harvest Time',
			'Harvest Condition',
			'Tissue',
			'Storage Condition',
			'Sample UID',
			'UID'
		]
		self.item_fieldnames = [i for i in fieldnames_order if i in self.id_list[0].keys()]

	def id_list_to_xlsx(
			self
	):
		if not self.id_list:
			return False
		base_filename = ' - '.join([self.id_list[0]['UID'], self.id_list[-1]['UID']])
		file_path = self.get_file_path(
			'xlsx',
			base_filename
		)
		wb = Workbook(file_path)
		worksheet = wb.add_worksheet("UID List")
		row_number = 0
		for i, j in enumerate(self.item_fieldnames):
			worksheet.write(row_number, i, j)
		for row in self.id_list:
			row_number += 1
			col_number = 0
			for field in self.item_fieldnames:
				if isinstance(row[field], list):
					row[field] = ", ".join(row[field])
				worksheet.write(row_number, col_number, row[field])
				col_number += 1
		wb.close()
		self.file_list.append({
			"filename": os.path.basename(file_path),
			"file_path": file_path,
			"file_size": os.path.getsize(file_path),
			"url": url_for(
				"download_file",
				username=self.username,
				filename= os.path.basename(file_path),
				_external=True
			)
		})

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
			with_timestamp=True,
			id_list=False
	):
		if base_filename and id_list:
			filename = (
					base_filename
					+ ' '
					+ ' to '.join([str(self.id_list[0]['UID']), str(self.id_list[-1]['UID'])])
			)
		elif base_filename and with_timestamp:
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

	def record_form_to_template(
		self,
		record_data
	):
		self.id_list = ItemList().generate_id_list(record_data)
		if not self.id_list:
			return False
		if record_data['record_type'] == 'trait' and record_data['replicates'] and record_data['replicates'] > 1:
			self.replicates = record_data['replicates']
		self.set_item_fieldnames()
		self.features = FeatureList(
			record_data['item_level'],
			record_data['record_type']).get_features(
				feature_group=record_data['feature_group'] if 'feature_group' in record_data else None,
				features=record_data['selected_features'] if 'selected_features' in record_data else None
		)
		if not self.features:
			return False
		self.id_list_to_template(
			record_data['record_type'],
			base_filename=record_data['item_level']
		)
		return True

	def id_list_to_template(
			self,
			record_type,
			base_filename=None
	):
		if not self.id_list and self.item_level:
			return False
		if not self.features:
			self.features = FeatureList(self.item_level, 'trait').get_features(feature_group="Registration")
		file_path = self.get_file_path(
			'xlsx',
			base_filename=base_filename,
			with_timestamp=False,
			id_list=True
		)
		wb = Workbook(file_path)
		template_worksheet = wb.add_worksheet("Template")
		item_details_worksheet = wb.add_worksheet("Item Details")
		feature_details_worksheet = wb.add_worksheet("Feature Details")
		hidden_worksheet = wb.add_worksheet("hidden")
		hidden_worksheet.hide()
		date_format = wb.add_format({'num_format': 'yyyy-mm-dd', 'left': 1})
		time_format = wb.add_format({'num_format': 'hh:mm', 'right': 1})
		right_border = wb.add_format({'right': 1})
		header_format = wb.add_format({'bottom': 1})
		row_number = 0
		# write header for context worksheet
		for i, j in enumerate(self.item_fieldnames):
			item_details_worksheet.write(row_number, i, j, header_format)
		if record_type == 'trait':
			core_template_fieldnames = ['UID', 'Date', 'Time', 'Person']
		else:
			core_template_fieldnames = ['UID', 'Start Date', 'Start Time', 'End Date', 'End Time', 'Person']
		feature_fieldnames = [feature['name'] for feature in self.features]
		template_fieldnames = core_template_fieldnames + feature_fieldnames
		feature_details_fieldnames = [
			'name',
			'format',
			'minimum',
			'maximum',
			'details',
			'category_list'
		]
		if record_type == 'trait':
		# column < row < cell formatting in priority
			template_worksheet.set_column(0, 0, None, cell_format=right_border)
			template_worksheet.set_column(1, 1, None, cell_format=date_format)
			template_worksheet.set_column(2, 2, None, cell_format=time_format)
			template_worksheet.set_column(3, 3, None, cell_format=right_border)
		else:
			template_worksheet.set_column(0, 0, None, cell_format=right_border)
			template_worksheet.set_column(1, 1, None, cell_format=date_format)
			template_worksheet.set_column(2, 2, None, cell_format=time_format)
			template_worksheet.set_column(3, 3, None, cell_format=date_format)
			template_worksheet.set_column(4, 4, None, cell_format=time_format)
			template_worksheet.set_column(5, 5, None, cell_format=right_border)
		template_worksheet.set_column(len(template_fieldnames)-1, len(template_fieldnames)-1, None, cell_format=right_border)
		row_number = 0
		# write header for template worksheet
		for i, j in enumerate(template_fieldnames):
			template_worksheet.write(row_number, i, j, header_format)
		# set the formatting for the feature columns
		numeric_format = wb.add_format({'num_format': '0.#'})
		date_format = wb.add_format({'num_format': 'yyyy-mm-dd'})
		text_format = wb.add_format({'num_format': '@'})
		percent_format = wb.add_format({'num_format': 9})
		location_format = wb.add_format({'num_format': '0.0000; 0.0000'})
		feature_formats = {
			"numeric": numeric_format,
			"date": date_format,
			"text": text_format,
			"percent": percent_format,
			"multicat": text_format,
			"categorical": text_format,
			"location": location_format,
			"boolean": text_format
		}
		if self.replicates:
			id_list_length = self.replicates * len(self.id_list)
		else:
			id_list_length = len(self.id_list)
		for i, feature in enumerate(self.features):
			column_number = len(core_template_fieldnames) + i
			template_worksheet.set_column(
				column_number,
				column_number,
				None,
				cell_format=feature_formats[feature['format']]
			)
			# store categories in hidden worksheet
			if 'category_list' in feature:
				column_letter = chr(65 + i)
				category_row_number = 0
				hidden_worksheet.write(row_number, i, feature['name_lower'])
				for j, category in enumerate(feature['category_list']):
					category_row_number += 1
					hidden_worksheet.write(category_row_number, i, category)
				template_worksheet.data_validation(1, column_number, id_list_length, column_number, {
					'validate': 'list',
					'source': '=hidden!$' + column_letter + '$2:$' + column_letter + '$' + str(j + 1)
				})
		# write the id_list
		for row in self.id_list:
			for key, value in row.iteritems():
				# if there is a list (or nested lists) stored in this item
				# make sure it is printed as a list of strings
				#if key == 'Treatments':
				#	for treatment in value:
				#		if treatment['name'] not in self.item_fieldnames:
				#			self.item_fieldnames.append(treatment['name'])
				#			column_number = self.item_fieldnames.index(treatment['name'])
				#			item_details_worksheet.write(0, column_number, treatment['name'], header_format)
				#		column_number = self.item_fieldnames.index(treatment['name'])
				#		item_details_worksheet.write(row_number, column_number, ", ".join(treatment['categories']))
				#else:
				if isinstance(value, list):
					value = ", ".join(value)
				column_number = self.item_fieldnames.index(key)
				if self.replicates and self.replicates > 1:
					item_number = ((row_number - 1) / self.replicates) + 1
					item_details_worksheet.write(item_number + 1, column_number, value)
				else:
					item_details_worksheet.write(row_number + 1, column_number, value)
			if self.replicates and self.replicates > 1:
				replicate_ids = [str(i) for i in range(1, self.replicates + 1)]
				for rep in replicate_ids:
					row_number += 1
					template_worksheet.write(row_number, 0, str(row['UID']) + '.' + str(rep))
			else:
				row_number += 1
				template_worksheet.write(row_number, 0, str(row['UID']))
		# create the details worksheet
		row_number = 0
		for header in feature_details_fieldnames:
			column_number = feature_details_fieldnames.index(header)
			feature_details_worksheet.write(row_number, column_number, header,  header_format)
		# add notes about Date/Time/Person
		date_time_person_details = [
			("date", "Required: Date these values were determined (YYYY-MM-DD, e.g. 2017-06-01)"),
			("time", "Optional: Time these values were determined (24hr, e.g. 13:00. Defaults to 12:00"),
			("person", "Optional: Person responsible for determining these values")
		]
		for i in date_time_person_details:
			row_number += 1
			feature_details_worksheet.write(row_number, 0, i[0])
			feature_details_worksheet.write(row_number, 4, i[1])
		# empty row to separate date/time/person from features
		row_number += 1
		for feature in self.features:
			row_number += 1
			for i, feature_header in enumerate(feature_details_fieldnames):
				if feature_header in feature:
					if isinstance(feature[feature_header], list):
						feature[feature_header] = ", ".join(value for value in feature[feature_header])
					feature_details_worksheet.write(row_number, i, feature[feature_header])
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
			for row in id_list:
				for item in row:
					if isinstance(row[item], list):
						row[item] = ", ".join([i for i in row[item]])
				# for key, value in row:
				writer.writerow(row)
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
			fieldnames,
			id_list,
			features,
			base_filename=None,
			with_timestamp=True
	):
		self.make_csv_file(
			fieldnames,
			id_list,
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
		for i, feature in enumerate(features):
			feature['realPosition'] = str(i + 1)
			feature['isVisible'] = 'True'
		self.make_csv_file(
			feature_fieldnames,
			features,
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

	def collect_records(
			self,
			parameters,
			data_format,
			file_format='csv'
	):
		# TODO this statement could be optimised per level (or even just 'include sample or not')
		statement = (
			' MATCH '
			'	(: User {username_lower: toLower($username)}) '
			'	-[: AFFILIATED {confirmed: True}]->(partner: Partner) '
			' MATCH '
			'	(partner)'
			'	<-[: AFFILIATED {data_shared: True}]-(user: User) '
			'	-[: SUBMITTED]->(: Submissions)'
			'	-[: SUBMITTED]->(: Records) '
			'	-[: SUBMITTED]->(: UserFieldFeature) '
			'	-[submitted: SUBMITTED]->(record: Record) '
			'	-[:RECORD_FOR]->(item_feature:ItemFeature) '
			'	-[:FOR_FEATURE*..2]->(feature:Feature) '
			'	, (item_feature) '
			'	-[:FOR_ITEM]->(item:Item) '
			'	-[:FROM | IS_IN *]->(farm: Farm) '
			'	-[:IS_IN]->(region: Region) '
			'	-[:IS_IN]->(country: Country) '
		)
		filters = []
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
				'	CASE WHEN record.time THEN record.time ELSE Null >= $record_start '
				'	AND '
				'	CASE WHEN record.time THEN record.time ELSE Null < $record_end '
				'	) OR ( '
				'	CASE WHEN record.start THEN record.start ELSE Null < $record_end '
				'	AND '
				'	CASE WHEN record.end THEN record.end ELSE Null > $record_start '
				'	) OR ( '
				'	record.start IS FALSE '
				'	AND '
				'	CASE WHEN record.end THEN record.end ELSE Null > $record_start '
				'	AND '
				'	CASE WHEN record.end THEN record.end ELSE Null <= $record_end'
				'	) OR ( '
				'	CASE WHEN record.start THEN record.start ELSE Null < $record_end '
				'	AND '
				'	CASE WHEN record.start THEN record.start ELSE Null >= $record_start '
				'	AND '
				'	record.end IS FALSE '
				'	) '
				') '
			)
		elif parameters['record_start']:
			filters.append(
				' ( '
				'	CASE WHEN record.time THEN record.time ELSE Null >= $record_start '
				'	OR '
				'	CASE WHEN record.end THEN record.end ELSE Null > $record_start '
				'	OR '
				'	CASE WHEN record.start THEN record.start ELSE Null >= $record_start '
				' ) '
			)
		elif parameters['record_end']:
			filters.append(
				' ( '
				'	CASE WHEN record.time THEN record.time ELSE Null < $record_end '
				'	OR '
				'	CASE WHEN record.end THEN record.end ELSE Null <= $record_end '
				'	OR '
				'	CASE WHEN record.start THEN record.start ELSE Null < $record_end '
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
		if parameters['field_uid']:
			filters.append(
				' ( '
				'	sample_field.uid = $field_uid '
				'	OR '
				'	tree_field.uid = $field_uid '
				'	OR '
				'	block_field.uid = $field_uid '
				'	OR '
				'	item.uid = $field_uid '
				' ) '
			)
		if parameters['block_uid']:
			filters.append(
				' ( '
				'	sample_block.uid = $block_uid '
				'	OR '
				'	tree_block.uid = $block_uid '
				'	OR '
				'	item.uid = $block_uid '
				' ) '
			)
		if parameters['tree_id_list']:
			filters.append (
				' ( '
				'	sample_tree.id IN $tree_id_list '
				'	OR '
				'	(	'
				'		item: Tree '
				'		AND '
				'		item.id IN $tree_id_list '
				'	) '
				' ) '
			)
		if parameters['sample_id_list']:
			filters.append (
				' (	'
				'	item: Sample '
				'	AND '
				'	item.id IN $tree_id_list '
				' ) '
			)
		if parameters['selected_features']:
			filters.append(
				' feature.name_lower in $selected_features '
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
			'	feature.name as Feature, '
			'	partner.name as Partner, '
			'	user.name as `Submitted by`, '
			'	item.uid as UID, '
			'	item.id as ID, '
			'	COLLECT(sample_sample.id) as `Source samples`, '
			'	COLLECT(sample_tree.id) as `Source trees`, '
			'	COALESCE( '
			'		tree_block.name, '
			'		COLLECT(sample_tree_block.name) '
			'	) as Block, '
			'	COALESCE( '
			'		tree_block.id, '
			'		COLLECT(sample_tree_block.id) '
			'	) as `Block ID`, '
			'	COALESCE ( '
			'		sample_tree_field.uid, '
			'		sample_field.uid, '
			'		tree_field.uid, '
			'		block_field.uid '
			'	) as `Field UID`, '
			'	COALESCE ( '
			'		sample_tree_field.name, '
			'		sample_field.name, '
			'		tree_field.name, '
			'		block_field.name '
			'	) as Field, '
			'	farm.name as Farm, '
			'	region.name as Region, '
			'	country.name as Country, '
		)
		if data_format == 'db':
			statement += (
				'	submitted.time as `Submitted at`, '
				'	record.value as Value, '
				'	record.time as Time, '
				'	record.start as Start, '
				'	record.end as End, '
				'	record.person as `Recorded by` '
				' WITH { '
				'	Feature: Feature, '
				'	Partner: Partner, '
				'	`Submitted by`: `Submitted by`, '
				'	`Submitted at`: `Submitted at`, '
				'	Value: Value, '
				'	`Time/Period`: coalesce('
				'		CASE WHEN Time <> False THEN Time Else Null END,'
				'		[Start, End]'
				'	), '
				'	`Recorded by`: `Recorded by`, '
				'	UID: UID, '
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
				'	result["ID"] '
				)
		else:  # data_format == 'table'
			statement += (
				' COLLECT(DISTINCT(record.value)) as Values '
				' WITH { '
				'	Records: collect({'
				'		feature_name: Feature, '
				'		values: Values '
				'	}), '
				'	UID: UID, '
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
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				bolt_result,
				statement,
				parameters
			)
		# check if any data found, if not return none
		first_result = result.peek()
		if not first_result:
			return {
				'status': 'SUCCESS',
				'result': 'No data found to match your filters'
			}
		fieldnames = [
			'Country',
			'Region',
			'Farm',
			'Field',
			'Field UID',
			'Block',
			'Block ID',
			'Source trees',
			'Source samples',
			'UID',
		]
		if data_format == 'table':
			features = [i['feature_name'] for i in first_result[0]['Records']]
			fieldnames += features
		else:  # data_format == 'db'
			fieldnames += [
				'Feature',
				'Value',
				'Time/Period',
				'Recorded by',
				'Submitted at',
				'Submitted by',
				'Partner'
			]
		# prepare the file
		time = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
		base_filename = time + '_records.csv'
		file_path = self.get_file_path(
			file_format,
			base_filename
		)
		if file_format == 'csv':
			with open(file_path, 'w') as csv_file:
				writer = csv.DictWriter(
					csv_file,
					fieldnames=fieldnames,
					quoting=csv.QUOTE_ALL,
					extrasaction='ignore')
				writer.writeheader()
				for record in result:
					if data_format == 'table':
						for feature in record[0]['Records']:
							for value in feature['values']:
								# flatten each value to string if it is a list
								if isinstance(value, list):
									record[0]['Records'][feature]['values'] = (
											'['
											+ ', '.join([i.encode() for i in value])
											+ ']'
									)
							# then flatten the list of values to a string stored in the record dict
							record[0][feature['feature_name']] = ', '.join(
								[str(value).encode() for value in feature['values']]
							)
					# any other values that need flattening, e.g. 'Source trees'
					for key in record[0]:
						if data_format == 'db':
							if key == "Time/Period":
								if isinstance(record[0][key], list):
									if record[0][key][0]:
										record[0][key][0] = datetime.utcfromtimestamp(record[0][key][0] / 1000).strftime("%Y-%m-%d %H:%M")
									else:
										record[0][key][0] = 'Undefined'
									if record[0][key][1]:
										record[0][key][1] = datetime.utcfromtimestamp(record[0][key][1] / 1000).strftime("%Y-%m-%d %H:%M")
									else:
										record[0][key][1] = 'Undefined'
									record[0][key] = ' - '.join(record[0][key])
								else:
									record[0][key] = datetime.utcfromtimestamp(record[0][key] / 1000).strftime(
										"%Y-%m-%d %H:%M")
							if key == 'Submitted at':
								record[0][key] = datetime.utcfromtimestamp(record[0][key] / 1000).strftime("%Y-%m-%d %H:%M")
						if isinstance(record[0][key], list):
							if not record[0][key]:
								record[0][key] = None
							else:
								record[0][key] = '[' + ', '.join([str(i).encode() for i in record[0][key]]) + ']'
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
