from app import app, os
import grp
from app.cypher import Cypher
from neo4j_driver import (
	get_driver,
	neo4j_query
)
from flask import (
	url_for
)
from app.models import(
	AddFieldItems,
	ItemList,
	FeatureList
)
import unicodecsv as csv

from datetime import datetime

from xlsxwriter import Workbook


# User class related (all uploads are tied to a user) yet more specifically regarding uploads
class Download:
	def __init__(self, username, email_requested=False):
		self.username = username
		self.email_requested = email_requested
		self.id_list = None
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
				template_worksheet.data_validation(1, column_number, len(self.id_list), column_number, {
					'validate': 'list',
					'source': '=hidden!$' + column_letter + '$2:$' + column_letter + '$' + str(j + 1)
				})
		# write the id_list
		for row in self.id_list:
			row_number += 1
			for key, value in row.iteritems():
				# if there is a list (or nested lists) stored in this item
				# make sure it is printed as a list of strings
				if key == 'Treatments':
					for treatment in value:
						if treatment['name'] not in self.item_fieldnames:
							self.item_fieldnames.append(treatment['name'])
							column_number = self.item_fieldnames.index(treatment['name'])
							item_details_worksheet.write(0, column_number, treatment['name'], header_format)
						column_number = self.item_fieldnames.index(treatment['name'])
						item_details_worksheet.write(row_number, column_number, ", ".join(treatment['categories']))
				else:
					if isinstance(value, list):
						value = ", ".join(value)
					column_number = self.item_fieldnames.index(key)
					item_details_worksheet.write(row_number, column_number, value)
			template_worksheet.write(row_number, 0, row['UID'])
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
			download_filters,
			data_format
	):
		parameters = download_filters
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
		)
		filters = []
		if download_filters['submission_start']:
			filters.append(' submitted.time >= $submission_start ')
		if download_filters['submission_end']:
			filters.append(' submitted.time <= $submission_end ')
		# collect all the records that have start or end inside the selected ranges
		# not including unbounded records that surround the window
		# if include these would need to compare between records to characterise closures.
		if download_filters['record_start'] and download_filters['record_end']:
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
		elif download_filters['record_start']:
			filters.append(
				' ( '
				'	CASE WHEN record.time THEN record.time ELSE Null >= $record_start '
				'	OR '
				'	CASE WHEN record.end THEN record.end ELSE Null > $record_start '
				'	OR '
				'	CASE WHEN record.start THEN record.start ELSE Null >= $record_start '
				' ) '
			)
		elif download_filters['record_end']:
			filters.append(
				' ( '
				'	CASE WHEN record.time THEN record.time ELSE Null < $record_end '
				'	OR '
				'	CASE WHEN record.end THEN record.end ELSE Null <= $record_end '
				'	OR '
				'	CASE WHEN record.start THEN record.start ELSE Null < $record_end '
				' ) '
			)
		if download_filters['item_level']:
			# match item_level label, has title case
			# as we are string building here rather than using parameters we need to be sure we aren't allowing injection
			# so i do a quick check on the item_level to make sure it conforms to our expectations
			# this is already done at the form level but in case this constructor gets exposed elsewhere I will do it here too
			if download_filters['item_level'].title() in ['Field', 'Block', 'Tree', 'Sample']:
				filters.append(
					' item: ' + download_filters['item_level'].title()
				)
		if download_filters['country']:
			filters.append(
				' country.name_lower = toLower(trim($country)) '
			)
		if download_filters['region']:
			filters.append(
				' region.name_lower = toLower(trim($region)) '
			)
		if download_filters['farm']:
			filters.append(
				' farm.name_lower = toLower(trim($farm)) '
			)
		if download_filters['field_uid']:
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
		if download_filters['block_uid']:
			filters.append(
				' ( '
				'	sample_block.uid = $block_uid '
				'	OR '
				'	tree_block.uid = $block_uid '
				'	OR '
				'	item.uid = $block_uid '
				' ) '
			)
		if download_filters['tree_id_list']:
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
		if download_filters['sample_id_list']:
			filters.append (
				' (	'
				'	item: Sample '
				'	AND '
				'	item.id IN $tree_id_list '
				' ) '
			)
		if download_filters['selected_features']:
			filters.append(
				' feature.name_lower in $selected_features '
			)
		if filters:
			statement += (
				' WHERE '
			)
			statement += ' AND '.join(filters)
		statement += (
			' WITH '
			'	feature.name as Feature, '
			'	partner.name as Partner, '
			'	user.name as `Submitted by`, '
			'	submitted.time as `Submitted at`, '
			'	record.value as Value, '
			'	record.time as Time, '
			'	record.start as Start, '
			'	record.end as End, '
			'	record.person as `Recorded by`, '
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
			'	country.name as Country '
		)
		if data_format == 'database':
			statement += (
				' RETURN { '
				'	Feature: Feature, '
				'	Partner: Partner, '
				'	`Submitted by`: `Submitted by`, '
				'	`Submitted at`: `Submitted at`, '
				'	Value: Value, '
				'	Time: Time, '
				'	Start: Start, '
				'	End: End, '
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
				'	Country: Country '
				' } '
				' ORDER BY '
				'	CASE '
				'		WHEN `Field UID` IS NOT NULL THEN `Field UID` '
				'		ELSE UID END, '
				'	ID '
				)
		else:  # data_format == 'table'
			statement += (
				' RETURN { '
				'	Records: collect({Feature: Feature, Values: collect(Value)}), '
				'	UID: UID, '
				'	`Source samples`: `Source samples`, '
				'	`Source trees`: `Source trees`, '
				'	Block: Block, '
				'	`Block ID`: `Block ID`, '
				'	Field: Field, '
				'	`Field UID`: `Field UID`, '
				'	Farm: Farm, '
				'	Region: Region, '
				'	Country: Country '
				' } '
				' ORDER BY '
				'	CASE '
				'		WHEN `Field UID` IS NOT NULL THEN `Field UID` '
				'		ELSE UID END, '
				'	ID '
			)

		import pdb; pdb.set_trace()










#this needs updating!!
	def get_csv(
			self,
			country,
			region,
			farm,
			field_uid,
			block_uid,
			level,
			features,
			data_format,
			start_time,
			end_time):
		# First limit the get the confirmed affiliated institutes and all data nodes submitted by these
		# TODO put this filter at the end of the query, might be less db-hits
		user_data_query = (
			' MATCH (:User {username_lower: toLower($username)}) '
			' -[:AFFILIATED {confirmed :true}] '
			' ->(partner:Partner) '
			' WITH partner '
			' MATCH (partner)<-[:AFFILIATED {data_shared :true}] '
			' -(user:User) '
			' -[:SUBMITTED*5]->(data:Data) '
			' WITH user.name as user_name, partner.name as partner_name, data '
		)
		# this section first as not dependent on level but part of all queries
		if farm != "":
			frc = (
				' -[:IS_IN]->(farm:Farm {name_lower: toLower($farm)}) '
				' -[:IS_IN]->(region:Region {name_lower: toLower($region)}) '
				' -[:IS_IN]->(country:Country {name_lower: toLower($country)}) '
			)
		elif region != "":
			frc = (
				' -[:IS_IN]->(farm:Farm) '
				' -[:IS_IN]->(region:Region {name_lower: toLower($region)}) '
				' -[:IS_IN]->(country:Country {name_lower: toLower($country)}) '
			)
		elif country != "":
			frc = (
				' -[:IS_IN]->(farm: Farm) '
				' -[:IS_IN]->(region: Region) '
				' -[:IS_IN]->(country: Country {name_lower: toLower($country)}) '
			)
		else:
			frc = (
				' -[:IS_IN]->(farm: Farm) '
				' -[:IS_IN]->(region: Region) '
				' -[:IS_IN]->(country: Country) '
			)
		# then build the level dependent query string
		if level == 'sample':
			index_fieldnames = [
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
				'Branch UID',
				'Leaf UID',
				'UID'
				#'Tissue',
				#'Storage',
				#'Date Sampled'
			]
			td = (
				' MATCH '
				'	(feature:feature) '
				'	<-[FOR_TRAIT*2]-(sample_trait:ItemTrait) '
				'	<-[DATA_FOR]-(data) '
				' WHERE (trait.name_lower) IN $traits' 
				' WITH user_name, partner_name, data, trait, sample_trait '
				' MATCH '
				'	(sample_trait) '
				'	-[:FOR_ITEM]->(sample:Sample) '
				'	-[:FROM_FIELD]->(:FieldSamples) '
				'	-[:FROM_FIELD]->(field: Field) '
			)
			# if block_uid is defined
			if block_uid != "":
				tdp = td + (
					' WHERE field.uid = $field_uid '
					' MATCH (field) '
				)
				optional_block = (
					' MATCH '
					'	(sample)-[:FROM_TREE]->(:TreeSamples) '
					'	-[:FROM_TREE]->(tree:Tree) '
					'	-[:IS_IN {current:True}]->(: BlockTrees) '
					'	-[:IS_IN]->(block: Block {uid: $block_uid}) '
					'	-[:IS_IN]->(: FieldBlocks) '
					'	-[:IS_IN]->(field) '
					' WITH user_name, partner_name, data, trait, sample_trait, '
					'	sample, field, farm, region, country, '
					'	collect(tree) as trees, '
					'	collect(block) as blocks '
				)
			# if field_uid is defined
			elif field_uid != "":
				tdp = td + (
					' WHERE field.uid = $field_uid '
					' MATCH (field) '
				)
				optional_block = (
					' OPTIONAL MATCH (tree) '
					' -[:IS_IN {current:True}]->(: BlockTrees) '
					' -[:IS_IN]->(block: Block) '
					' WITH user_name, partner_name, data, trait, sample_trait, '
					'	sample, field, farm, region, country, '
					'	collect(tree) as trees, '
					'	collect(block) as blocks '
				)
			# else no field selected
			else:
				tdp = td + ' MATCH (field) '
				optional_block = (
					' OPTIONAL MATCH '
					'	(sample)-[:FROM_TREE]->(:TreeSamples) '
					'	-[:FROM_TREE]->(tree:Tree) '
					' OPTIONAL MATCH '
					'	(tree) '
					'	-[:IS_IN {current:True}]->(: BlockTrees) '
					'	-[:IS_IN]->(block: Block) '
					' WITH user_name, partner_name, data, trait, sample_trait, '
					'	sample, field, farm, region, country, '
					'	collect(tree) as trees, '
					'	collect(block) as blocks '
				)
			optional_block += (
				' OPTIONAL MATCH '
				'	(sample)-[:FROM_BRANCH]->(branch:Branch) '
				' OPTIONAL MATCH '
				'	(sample)-[:FROM_LEAF]->(leaf:Leaf) '
			)
			# generate the return statement
			if data_format == 'table':
				response = (
					# need a with statement to allow order by with COLLECT
					' WITH '
					' 	country.name as Country, '
					' 	region.name as Region, '
					' 	farm.name as Farm, '
					' 	field.name as Field, '
					'	field.uid as `Field UID`, '
					' 	extract(block in blocks | block.name) as Block, '
					' 	extract(block in blocks | block.uid) as `Block UID`, '
					' 	extract(tree in trees | tree.uid) as `Tree UID`, '
					' 	extract(tree in trees | tree.custom_id) as `Tree Custom ID`, '
					' 	extract(tree in trees | tree.variety) as Variety, '
					' 	branch.uid as `Branch UID`, '
					' 	leaf.uid as `Leaf UID`, '
					' 	sample.uid as `Sample UID`, '
					#'	tissue.name as Tissue, '
					#'	storage.name as Storage, '
					'	sample.date as `Date Sampled`, '
					' 	sample.id as `Sample ID`, '
					'	COLLECT([trait.name, data.value]) as Traits '
					' RETURN '
					'	{ ' 
					'		Country: Country, '
					'		Region: Region, '
					'		Farm: Farm, '
					'		Field: Field, '
					'		`Field UID` : `Field UID`, '
					'		Block : Block, '
					'		`Block UID`: `Block UID`, '
					'		`Tree UID` : `Tree UID`, '
					'		`Tree Custom ID`: `Tree Custom ID`, '
					'		Variety: Variety, '
					'		`Branch UID`: `Branch UID`, '
					'		`Leaf UID`: `Leaf UID`, '
					'		UID : `Sample UID` ,'
					#'		Tissue: Tissue, '
					#'		Storage: Storage, '
					'		`Date Sampled`: `Date Sampled`, '
					'		`Sample ID` : `Sample ID`, '
					'		Traits : Traits } '
					' ORDER BY '
					'	`Field UID`, `Sample ID` '
				)
			else:  # if data_format == 'db':
				response = (
					' RETURN {'
					'	User: user_name, '
					'	Partner: partner_name,'
					'	Country: country.name, ' 
					'	Region: region.name, '
					'	Farm: farm.name, '
					'	Field: field.name, '
					'	`Field UID`: field.uid, '
					' 	Block: extract(block in blocks | block.name), '
					' 	`Block UID`: extract(block in blocks | block.uid), '
					' 	`Tree UID`: extract(tree in trees | tree.uid), '
					' 	`Tree Custom ID`: extract(tree in trees | tree.custom_id), '
					' 	Variety: extract(tree in trees | tree.variety), '
					'	`Sample UID`: sample.uid, '
					'	`Branch UID`: branch.uid, '
					'	`Leaf UID`: leaf.uid, '
					'	UID : sample.uid, '
					#'	Tissue: tissue.name, '
					#'	Storage: storage.name, '
					'	`Date Sampled`: sample.date, '
					'	`Sample ID`: sample.id, '
					'	Trait: trait.name, '
					'	Value: data.value, '
					'	Location: data.location, '
					'	`Recorded at`: apoc.date.format(data.time), '
					'	`Recorded by`: data.person '
					' } '
					' ORDER BY '
					'	field.uid, '
					'	sample.id, '
					'	trait.name, '
					'	apoc.date.format(data.time) '
					)
		elif level == 'leaf':
			index_fieldnames = [
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
				'Branch UID',
				'UID'
			]
			td = (
				' MATCH '
				'	(trait:LeafTrait) '
				'	<-[:FOR_TRAIT*2]-(leaf_trait) '
				'	<-[:DATA_FOR]-(data) '
				' WHERE (trait.name_lower) IN $traits '
				' WITH user_name, partner_name, data, trait, leaf_trait '
				' MATCH '
				'	(leaf_trait) '
				'	-[:FOR_ITEM]->(leaf:Leaf) '
				'	-[:FROM_TREE*2]->(tree:Tree) '
			)

			# if block_uid is defined
			if block_uid != "":
				tdp = td + (
					'-[:IS_IN {current: True}]->(: BlockTrees) '
					' -[:IS_IN]->(block: Block {uid: $block_uid}) '
					' -[:IS_IN]->(: FieldBlocks) '
					' -[:IS_IN]->(field: Field) '
				)
				optional_block = ''
			# if field_uid is defined
			elif field_uid != "":
				tdp = td + (
					' -[:IS_IN]->(: FieldTrees) '
					' -[:IS_IN]->(field: Field {uid: $field_uid}) '
				)
				optional_block = (
					' OPTIONAL MATCH (tree) '
					' -[:IS_IN {current: True}]->(: BlockTrees) '
					' -[:IS_IN]->(block: Block) '
				)
			# no field selected
			else:
				tdp = td + (
					' -[:IS_IN]->(: FieldTrees) '
					' -[:IS_IN]->(field:Field) '
				)
				optional_block = (
					' OPTIONAL MATCH (tree) '
					' -[:IS_IN {current:True}]->(: BlockTrees) '
					' -[:IS_IN]->(block:Block) '
				)
			optional_block += (
				' OPTIONAL MATCH '
				'	(leaf)-[:FROM_BRANCH]->(branch:Branch) '
			)
			# and generate the return statement
			if data_format == 'table':
				response = (
					# need a with statement to allow order by with COLLECT
					' WITH '
					'	country.name as Country, '
					'	region.name as Region, '
					'	farm.name as Farm, '
					'	field.name as Field, '
					'	field.uid as `Field UID`, '
					'	block.name as Block, '
					'	block.uid as `Block UID`, '
					'	tree.uid as `Tree UID`, '
					'	tree.custom_id as `Tree Custom ID`, '
					'	tree.variety as Variety, '
					'	branch.uid as `Branch UID`, '
					'	leaf.uid as `Leaf UID`, '
					'	leaf.id as `Leaf ID`, '
					'	COLLECT([trait.name, data.value]) as Traits '
					' RETURN '
					'	{ ' 
					'		Country: Country, '
					'		Region: Region, '
					'		Farm: Farm, '
					'		Field: Field, '
					'		`Field UID`: `Field UID`, '
					'		Block : Block, '
					'		`Block UID`: `Block UID`, '
					'		`Tree UID`: `Tree UID`, '
					'		`Tree Custom ID`: `Tree Custom ID`, '
					'		Variety: Variety, '
					'		`Branch UID`: `Branch UID`, '
					'		UID: `Leaf UID`, '
					'		`Leaf ID`: `Leaf ID`, '
					'		Traits: Traits } '
					' ORDER BY '
					'	`Field UID`, `Leaf ID` '
				)
			else:  # if data_format == 'db':
				response = (
					' RETURN {'
					'	User : user_name, '
					'	Partner : partner_name,'
					'	Country : country.name, ' 
					'	Region : region.name, '
					'	Farm : farm.name, '
					'	Field : field.name, '
					'	`Field UID`: field.uid, '
					'	Block : block.name, '
					'	`Block UID` : block.uid, '
					'	`Tree UID` : tree.uid, '
					'	`Tree Custom ID`: tree.custom_id,	'
					'	Variety: tree.variety, '
					'	`Branch UID`: branch.uid, '
					'	UID: leaf.uid, '
					'	`Leaf ID`: leaf.id, '
					'	Trait : trait.name, '
					'	Value : data.value, '
					'	Location : data.location, '
					'	`Recorded at` : apoc.date.format(data.time), '
					'	`Recorded by`: data.person '
					' } '
					' ORDER BY '
					'	field.uid, '
					'	leaf.id, '
					'	trait.name, '
					'	apoc.date.format(data.time) '
					)
		elif level == 'branch':
			index_fieldnames = [
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
				'UID',
			]
			td = (
				' MATCH '
				'	(trait: BranchTrait) '
				'	<-[:FOR_TRAIT*2]-(branch_trait) '
				'	<-[:DATA_FOR]-(data) '
				' WHERE (trait.name_lower) IN $traits'
				' WITH user_name, partner_name, data, trait, branch_trait '
				' MATCH '
				'	(branch_trait) '
				'	-[:FOR_ITEM]->(branch:Branch) '
				'	-[:FROM_TREE*2]->(tree:Tree) '
			)
			# if block_uid is defined
			if block_uid != "":
				tdp = td + (
					'-[:IS_IN {current:True}]->(:BlockTrees) '
					' -[:IS_IN]->(block:Block {uid:$blockUID}) '
					' -[:IS_IN]->(:FieldBlocks) '
					' -[:IS_IN]->(field:Field) '
				)
				optional_block = ''
			# if field_uid is defined (but no block_uid)
			elif field_uid != "":
				tdp = td + (
					' -[:IS_IN]->(:FieldTrees) '
					' -[:IS_IN]->(field:Field {uid: $field_uid}) '
				)
				optional_block = (
					' OPTIONAL MATCH (tree) '
					' -[:IS_IN {current:True}]->(:BlockTrees) '
					' -[:IS_IN]->(block:Block) '
				)
			# if no field selected
			else:
				tdp = td + (
					' -[:IS_IN]->(:FieldTrees) '
					' -[:IS_IN]->(field:Field) '
				)
				optional_block = (
					' OPTIONAL MATCH (tree) '
					' -[:IS_IN {current:True}]->(:BlockTrees) '
					' -[:IS_IN]->(block:Block) '
				)
			# generate the return statement
			if data_format == 'table':
				response = (
					# need a with statement to allow order by with COLLECT
					' WITH '
					' 	country.name as Country, '
					' 	region.name as Region, '
					' 	farm.name as Farm, '
					' 	field.name as Field, '
					' 	field.uid as `Field UID`, '
					' 	block.name as Block, '
					' 	block.uid as `Block UID`, '
					' 	tree.uid as `Tree UID`, '
					' 	tree.custom_id as `Tree Custom ID`, '
					' 	tree.variety as Variety, '
					' 	branch.uid as `Branch UID`, '
					'	branch.id as `Branch ID`,'
					'	COLLECT([trait.name, data.value]) as Traits '
					' RETURN '
					'	{ ' 
					'		Country: Country, '
					'		Region: Region, '
					'		Farm: Farm, '
					'		Field: Field, '
					'		`Field UID`: `Field UID`, '
					'		Block: Block, '
					'		`Block UID`: `Block UID`, '
					'		`Tree UID`: `Tree UID`, '
					'		`Tree Custom ID`: `Tree Custom ID`, '
					'		Variety: Variety, '
					'		UID: `Branch UID`, '
					'		`Branch ID`: `Branch ID`,'
					'		Traits: Traits } '
					' ORDER BY '
					'	`Field UID`, `Branch ID` '
				)
			else:  # if data_format == 'db':
				response = (
					' RETURN {'
					'	User: user_name, '
					'	Partner: partner_name,'
					'	Country: country.name, ' 
					'	Region: region.name, '
					'	Farm: farm.name, '
					'	Field: field.name, '
					'	`Field UID`: field.uid, '
					'	Block: block.name, '
					'	`Block UID`: block.uid, '
					'	`Tree UID`: tree.uid, '
					'	`Tree Custom ID`: tree.custom_id,	'
					'	Variety: tree.variety, '
					'	UID: branch.uid, '
					'	`Branch ID`: branch.id, '
					'	Trait: trait.name, '
					'	Value: data.value, '
					'	Location: data.location, '
					'	`Recorded at`: apoc.date.format(data.time), '
					'	`Recorded_by`: data.person '
					' } '
					' ORDER BY '
					'	field.uid, '
					'	branch.id, '
					'	trait.name, '
					'	data.time '
					)
		elif level == 'tree':
			index_fieldnames = [
				'Country',
				'Region',
				'Farm',
				'Field',
				'Field UID',
				'Block',
				'Block UID',
				'UID',
				'Tree Custom ID',
				'Variety'
			]
			td = (
				' MATCH '
				'	(trait:TreeTrait) '
				'	<-[:FOR_TRAIT*2]-(tree_trait) '
				'	<-[:DATA_FOR]-(data) '
				' WHERE (trait.name_lower) IN $traits '
				' WITH '
				'	user_name, '
				'	partner_name, '
				'	data, '
				'	trait, '
				'	tree_trait '
				' MATCH '
				'	(tree_trait) '
				'	-[:FOR_ITEM]->(tree:Tree) '
			)
			# if block_uid is defined
			if block_uid != "":
				tdp = td + (
					' -[:IS_IN {current: True}]->(: BlockTrees) '
					' -[:IS_IN]->(block: Block {uid: $block_uid}) '
					' -[:IS_IN]->(: FieldBlocks) '
					' -[:IS_IN]->(field: Field) '
				)
				optional_block = ''
			# if field_uid is defined
			elif field_uid != "":
				tdp = td + (
					' -[:IS_IN]->(FieldTrees) '
					' -[:IS_IN]->(field: Field {uid:$field_uid}) '
				)
				optional_block = (
					' OPTIONAL MATCH (tree) '
					' -[:IS_IN {current: True}]->(: BlockTrees) '
					' -[:IS_IN]->(block: Block) '
				)
			# if no field selected
			else:
				tdp = td + (
					' -[:IS_IN]->(:FieldTrees) '
					' -[:IS_IN]->(field:Field) '
				)
				optional_block = (
					' OPTIONAL MATCH (tree) '
					' -[:IS_IN {current:True}]->(:BlockTrees) '
					' -[:IS_IN]->(block:Block) '
				)
			# and generate the return statement
			if data_format == 'table':
				response = (
					# need WITH statement to allow order by with COLLECT
					' WITH '
					' 	country.name as Country, '
					' 	region.name as Region, '
					' 	farm.name as Farm, '
					' 	field.name as Field, '
					' 	field.uid as `Field UID`, '
					' 	block.name as Block, '
					' 	block.uid as `Block UID`, '
					' 	tree.uid as `Tree UID`, '
					' 	tree.custom_id as `Tree Custom ID`, '
					' 	tree.variety as Variety, '
					'	tree.id as `Tree ID`,'
					'	COLLECT([trait.name, data.value]) as Traits '
					' RETURN '
					'	{ ' 
					'		Country: Country, '
					'		Region: Region, '
					'		Farm: Farm, '
					'		Field: Field, '
					'		`Field UID`: `Field UID`, '
					'		Block: Block, '
					'		`Block UID`: `Block UID`, '
					'		UID: `Tree UID`, '
					'		`Tree Custom ID`: `Tree Custom ID`, '
					'		Variety: Variety, '
					'		`Tree ID`: `Tree ID`,'
					'		Traits: Traits '
					'	} '
					' ORDER BY '
					'	`Field UID`, `Tree ID` '
				)
			else:  # if data_format == 'db':
				response = (
					' RETURN {'
					'	User : user_name, '
					'	Partner : partner_name,'
					'	Country : country.name, ' 
					'	Region : region.name, '
					'	Farm : farm.name, '
					'	Field : field.name, '
					'	`Field UID`: field.uid, '
					'	Block : block.name, '
					'	`Block UID` : block.uid, '
					'	UID : tree.uid, '
					'	`Tree Custom ID`: tree.custom_id,	'
					'	Variety: tree.variety, '
					'	`Tree ID`: tree.id, '
					'	Trait : trait.name, '
					'	Value : data.value, '
					'	Location : data.location, '
					'	`Recorded at` : apoc.date.format(data.time), '
					'	`Recorded by`: data.person '
					' } '
					' ORDER BY field.uid, tree.id, trait.name, data.time '
					)
		elif level == 'block':
			index_fieldnames = [
				'Country',
				'Region',
				'Farm',
				'Field',
				'Field UID',
				'Block',
				'UID'
			]
			td = (
				' MATCH '
				'	(trait: BlockTrait) '
				'	<-[:FOR_TRAIT*2]-(block_trait) '
				'	<-[:DATA_FOR]-(data) '
				' WHERE (trait.name_lower) IN $traits '
				' WITH user_name, partner_name, data, trait, block_trait '
				' MATCH '
				'	(block_trait) '
				'	-[:FOR_ITEM]->(block:Block '
			)
			if block_uid != "":
				tdp = td + (
					' {uid:$blockUID}) '
					' -[:IS_IN]->(:FieldBlocks) '
					' -[:IS_IN]->(field:Field) '
				)
			elif field_uid != "":
				tdp = td + (
					') '
					' -[:IS_IN]->(:FieldBlocks)'
					' -[:IS_IN]->(field:Field {uid:$field_uid}) '
				)
			else:
				tdp = td + (
					' ) '
					' -[:IS_IN]->(:FieldBlocks)'
					' -[:IS_IN]->(field:Field) '
				)
			optional_block = ''
			# and generate the return statement
			if data_format == 'table':
				response = (
					# need a with statement to allow order by with COLLECT
					' WITH '
					' 	country.name as Country, '
					' 	region.name as Region, '
					' 	farm.name as Farm, '
					' 	field.name as Field, '
					' 	field.uid as `Field UID`, '
					' 	block.name as Block, '
					' 	block.uid as `Block UID`, '
					'	block.id as `Block ID`, '
					'	COLLECT([trait.name, data.value]) as Traits '
					' RETURN '
					'	{ ' 
					'		Country : Country, '
					'		Region : Region, '
					'		Farm : Farm, '
					'		Field : Field, '
					'		`Field UID` : `Field UID`, '
					'		Block : Block, '
					'		UID: `Block UID`, '
					'		`Block ID`: `Block ID`,'
					'		Traits : Traits } '
					' ORDER BY '
					'	`Field UID`, `Block ID` '
				)
			else:  # if data_format == 'db':
				response = (
					' RETURN { '
					'	User: user_name, '
					'	Partner: partner_name,'
					'	Country: country.name, ' 
					'	Region: region.name, '
					'	Farm: farm.name, '
					'	Field: field.name, '
					'	`Field UID`: field.uid, '
					'	Block: block.name, '
					'	UID: block.uid, '
					'	`Block ID`: block.id, '
					'	Trait: trait.name, '
					'	Value: data.value, '
					'	Location: data.location, '
					'	`Recorded at`: apoc.date.format(data.time), '
					'	`Recorded by`: data.person '
					' } '
					' ORDER BY '
					'	field.uid, block.id, trait.name, data.time '
					)
		else:  # level == 'field':
			index_fieldnames = [
				'Country',
				'Region',
				'Farm',
				'Field',
				'UID'
			]
			tdp = (
				' MATCH '
				'	(trait:FieldTrait) '
				'	<-[:FOR_TRAIT]-(field_trait) '
				'	<-[:DATA_FOR]-(data) '
				' WHERE (trait.name_lower) IN $traits'
				' WITH user_name, partner_name, data, trait, field_trait '
				' MATCH '
				'	(field_trait) '
				'	-[:FOR_ITEM]->(field: Field) '
			)
			optional_block = ''
			# and generate the return statement
			if data_format == 'table':
				response = (
					# need a WITH statement to allow order by with COLLECT
					' WITH '
					'	country.name as Country, '
					'	region.name as Region, '
					'	farm.name as Farm, '
					'	field.name as Field, '
					'	field.uid as `Field UID`, '
					'	COLLECT([trait.name_lower, data.value]) as Traits '
					' RETURN '
					'	{ ' 
					'		Country: Country, '
					'		Region: Region, '
					'		Farm: Farm, '
					'		Field: Field, '
					'		UID: `Field UID`, '
					'		Traits: Traits '
					'	} '
					' ORDER BY '
					'	`Field UID` '
				)
			else:  # if data_format == 'db':
				response = (
					' RETURN { '
					'	User: user_name, '
					'	Partner: partner_name, '
					'	Country: country.name, ' 
					'	Region: region.name, '
					'	Farm: farm.name, '
					'	Field: field.name, '
					'	UID: field.uid, '
					'	Trait: trait.name, '
					'	Value: data.value, '
					'	Location: data.location, '
					'	`Recorded at`: apoc.date.format(data.time), '
					'	`Recorded by`: data.person '
					' } '
					' ORDER BY field.uid, trait.name, data.time '
					)
		# add conditional for data between time range
		if start_time != "" and end_time != "":
			time_condition = ' WHERE data.time > $start_time AND data.time < $end_time '
		elif start_time == "" and end_time != "":
			time_condition = ' WHERE data.time < $end_time '
		elif start_time != "" and end_time == "":
			time_condition = ' WHERE data.time > $start_time '
		else:
			time_condition = ''
		# finalise query, get data
		query = user_data_query + tdp + frc + time_condition + optional_block + response
		with get_driver().session() as neo4j_session:
			result = neo4j_session.read_transaction(
				self._get_csv,
				query,
				country,
				region,
				farm,
				field_uid,
				block_uid,
				traits,
				start_time,
				end_time
			)
		# check if any data found, if not return none
		if len(result) == 0:
			return None
		# prepare data and variables to make file
		if data_format == 'table':
			# expand the traits and values as key:value pairs (each measure as an item in a list if more than one)
			for record in result:
				for i in record["Traits"]:
					# if the trait hasn't appeared yet
					if not i[0] in record:
						# if the new value isn't a list just store it as a string
						if not isinstance(i[1], list):
							record.update({i[0]: i[1]})
						# if the new value is a list (multicat)
						# make all the elements of the list into strings
						# and make a string out of this list of strings
						else:
							record.update({i[0]: ", ".join(i[1])})
					# if the trait is already there
					else:
						# if the stored value for this trait is not a list
						if not isinstance(record[i[0]], list):
							#  - we make the existing value an item in a list
							record.update({i[0]: [record[i[0]]]})
						# Need to handle multicats that have lists of values as a single data point
						# these always return a list (even if 1 element) so at least it is consistent
						# in this case we can determine if it is a multicat by checking if the new value is a list
						if not isinstance(i[1], list):
							record[i[0]].append(i[1])
						else:
							record[i[0]].append(", ".join(i[1]))
			trait_fieldnames = traits
			# now create the csv file from result and fieldnames
			fieldnames = index_fieldnames + trait_fieldnames
		else:  # if data_format == 'db':
			fieldnames = index_fieldnames + ['Trait', 'Value', 'Location', 'Recorded at', 'Recorded by']
		# create the file path
		time = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
		filename = time + '_data.csv'
		file_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], self.username, filename)
		# create user download path if not found
		download_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], self.username)
		if not os.path.isdir(download_path):
			os.mkdir(download_path)
			gid = grp.getgrnam(app.config['CELERYGRPNAME']).gr_gid
			os.chown(download_path, -1, gid)
			os.chmod(download_path, 0775)
		# make the file
		with open(file_path, 'w') as csv_file:
			writer = csv.DictWriter(
				csv_file,
				fieldnames = fieldnames,
				quoting = csv.QUOTE_ALL,
				extrasaction = 'ignore')
			writer.writeheader()
			for row in result:
				for item in row:
					if isinstance(row[item], list):
						row[item] = str(', '.join([str(i).encode() for i in row[item] if i]))
				writer.writerow(row)
			file_size = csv_file.tell()
		return {
			"filename": filename,
			"file_path": file_path,
			"file_size": file_size
		}

	def _get_csv(
			self,
			tx,
			query,
			country,
			region,
			farm,
			field_uid,
			block_uid,
			traits,
			start_time,
			end_time
	):
		# perform transaction and return the simplified result
		# remove all the transaction metadata and just keep the result map
		return [record[0] for record in tx.run(
			query,
			username = self.username,
			country = country,
			region =region,
			farm = farm,
			field_uid = field_uid,
			block_uid = block_uid,
			traits = traits,
			start_time = start_time,
			end_time = end_time
		)]
