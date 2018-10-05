from app import app, os
import grp
from app.cypher import Cypher
from samples import Samples
from neo4j_driver import (
	get_driver,
	neo4j_query
)
from flask import (
	session,
	url_for
)
from app.models import(
	ItemList
)
import unicodecsv as csv
from datetime import datetime
from app.models import (
	Fields,
	TraitList
)
from xlsxwriter import Workbook


# User class related (all uploads are tied to a user) yet more specifically regarding uploads
class Download:
	def __init__(self, username, email_requested=False):
		self.username = username
		self.email_requested = email_requested
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

	def get_file_list(
			self
	):
		return self.file_list

	def template_files(
			self,
			template_format,
			create_new_items,
			level,
			traits,
			country,
			region,
			farm,
			field_uid,
			block_uid,
			per_tree_replicates,
			trees_start,
			trees_end,
			branches_start,
			branches_end,
			leaves_start,
			leaves_end,
			samples_start,
			samples_end,
			tissue,
			storage,
			per_sample_replicates,
			samples_pooled,
			samples_count,
			start_time,
			end_time
	):
		if create_new_items:
			if level == 'branch':
				id_list = Fields.add_branches(
					field_uid,
					trees_start,
					trees_end,
					per_tree_replicates
				)
			elif level == 'leaf':
				id_list = Fields.add_leaves(
					field_uid,
					trees_start,
					trees_end,
					per_tree_replicates
				)
			else: # level == 'sample':
				if samples_pooled:
					id_list = Samples.add_samples_pooled(
						field_uid,
						samples_count
					)
				else:
					# TODO remove Collect page and merge here
					# check we have found matching ID's, if not return None
					id_list = Samples.add_samples_per_tree(
						field_uid,
						trees_start,
						trees_end,
						per_tree_replicates
					)
				# if requesting replicates for samples then create these replicates in the id_list
				if per_sample_replicates > 1:
					if len(id_list) == 0:
						return None
					id_list_reps = []
					for sample in id_list:
						sample['Sample UID'] = sample['UID']
						for i in range(per_sample_replicates):
							sample['UID'] = str(sample['Sample UID']) + "." + str(int(i + 1))
							id_list_reps.append(sample.copy())
					id_list = id_list_reps
		else:
			id_list = self.get_id_list(
				level,
				country,
				region,
				farm,
				field_uid,
				block_uid,
				trees_start,
				trees_end,
				branches_start,
				branches_end,
				leaves_start,
				leaves_end,
				samples_start,
				samples_end,
				tissue,
				storage,
				start_time,
				end_time
			)
		fieldnames = self.get_fieldnames(level, per_sample_replicates)
		# get trait details
		traits = TraitList.get_traits(level, traits)
		if template_format == 'xlsx':
			self.make_xlsx_data_template(
				fieldnames,
				id_list,
				traits,
				base_filename=level,
				with_timestamp=True
			)
		elif template_format == 'csv':
			self.make_csv_table_template(
				fieldnames,
				id_list,
				traits,
				base_filename=level,
				with_timestamp=True
			)
		else: # template_format == 'fb':
			self.make_fb_template(
				fieldnames,
				id_list,
				traits,
				base_filename=level,
				with_timestamp=True
			)

	@staticmethod
	def get_fieldnames(
			level,
			per_sample_replicates = None
	):
		fieldnames = [
			'Country',
			'Region',
			'Farm',
			'Field',
			'UID'
		]
		if level in ['block','tree','branch','leaf','sample']:
			fieldnames[-1:-1] = [
				'Field UID',
				'Block'
			]
		if level in ['tree', 'branch', 'leaf', 'sample']:
			fieldnames[-1:-1] = [
				'Block UID',
				'Variety',
				'Tree Custom ID'
			]
		if level in ['branch', 'leaf', 'sample']:
			fieldnames[-1:-1] = [
				'Tree UID'
			]
		if level in ['leaf','sample']:
			fieldnames[-1:-1] = [
				'Branch UID'
			]
		if level == 'sample':
			fieldnames[-1:-1] = [
				'Leaf UID',
				'Tissue',
				'Storage',
				'Date Sampled'
			]
			if per_sample_replicates:
				fieldnames.insert(-1, 'Sample UID')
		return fieldnames

	@staticmethod
	def get_id_list(
			level,
			country,
			region,
			farm,
			field_uid,
			block_uid,
			trees_start,
			trees_end,
			branches_start,
			branches_end,
			leaves_start,
			leaves_end,
			samples_start,
			samples_end,
			tissue,
			storage,
			start_time,
			end_time
	):
		if level == 'field':
			id_list = ItemList.get_fields(
				country=country,
				region=region,
				farm=farm)

		elif level == 'block':
			id_list = ItemList.get_blocks(
				country=country,
				region=region,
				farm=farm,
				field_uid=field_uid
			)
		elif level == 'tree':
			id_list = ItemList.get_trees(
				country=country,
				region=region,
				farm=farm,
				field_uid=field_uid,
				block_uid=block_uid,
				trees_start=trees_start,
				trees_end=trees_end
			)
		elif level == 'branch':
			id_list = ItemList.get_leaves(
				country=country,
				region=region,
				farm=farm,
				field_uid=field_uid,
				block_uid=block_uid,
				trees_start=trees_start,
				trees_end=trees_end,
				branches_start=branches_start,
				branches_end=branches_end
			)

		elif level == 'leaf':
			id_list = ItemList.get_leaves(
				country=country,
				region=region,
				farm=farm,
				field_uid=field_uid,
				block_uid=block_uid,
				trees_start=trees_start,
				trees_end=trees_end,
				branches_start=branches_start,
				branches_end=branches_end,
				leaves_start=leaves_start,
				leaves_end=leaves_end
			)
		elif level == 'sample':
			id_list = ItemList.get_samples(
				country=country,
				region=region,
				farm=farm,
				field_uid=field_uid,
				block_uid=block_uid,
				trees_start=trees_start,
				trees_end=trees_end,
				branches_start=branches_start,
				branches_end=branches_end,
				leaves_start=leaves_start,
				leaves_end=leaves_end,
				samples_start=samples_start,
				samples_end=samples_end,
				tissue=tissue,
				storage=storage,
				start_time=start_time,
				end_time=end_time
			)
		return id_list

	def get_file_path(
			self,
			file_extension,
			base_filename = None,
			with_timestamp = True
	):
		if base_filename and with_timestamp:
			filename = base_filename + '_' + self.time
		elif base_filename and not with_timestamp:
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
					filename = time_s + '_' + filename_appendix
				filename = filename + '.' + file_extension
				file_path = os.path.join(self.user_download_folder, filename)
		return file_path

	def make_xlsx_data_template(
			self,
			fieldnames,
			id_list,
			traits,
			base_filename = None,
			with_timestamp = True
	):
		file_path = self.get_file_path(
			'xlsx',
			base_filename,
			with_timestamp=with_timestamp
		)
		len_fieldnames = len(fieldnames)
		fieldnames += ['Date', 'Time', 'Person']
		fieldnames += [trait['name'] for trait in traits]
		trait_fieldnames = [
			'name',
			'format',
			'minimum',
			'maximum',
			'details',
			'category_list'
		]
		wb = Workbook(file_path)
		date_format = wb.add_format({'num_format': 'yyyy-mm-dd', 'left': 1})
		time_format = wb.add_format({'num_format': 'hh:mm'})
		right_border = wb.add_format({'right': 1})
		uid_format = wb.add_format({'left':1})
		header_format = wb.add_format({'border': 1})
		template_worksheet = wb.add_worksheet("Template")
		# column < row < cell formatting in priority
		template_worksheet.set_column(len_fieldnames -1, len_fieldnames -1, None, cell_format=uid_format)
		template_worksheet.set_column(len_fieldnames, len_fieldnames, None, cell_format=date_format)
		template_worksheet.set_column(len_fieldnames +1, len_fieldnames+1, None, cell_format=time_format)
		template_worksheet.set_column(len_fieldnames + 2, len_fieldnames + 2, None, cell_format=right_border)
		template_worksheet.set_column(len(fieldnames)-1, len(fieldnames)-1, None, cell_format=right_border)
		row_number = 0
		for header in fieldnames:
			column_number = fieldnames.index(header)
			template_worksheet.write(row_number, column_number, header, header_format)
		for row in id_list:
			row_number += 1
			for key, value in row.iteritems():
				# if there is a list (or nested lists) stored in this item
				# make sure it is printed as a list of strings
				if isinstance(value, list):
					value = ", ".join([i for i in value])
				column_number = fieldnames.index(key)
				template_worksheet.write(row_number, column_number, value)
		trait_details_worksheet = wb.add_worksheet("Trait details")
		row_number = 0
		for header in trait_fieldnames:
			column_number = trait_fieldnames.index(header)
			trait_details_worksheet.write(row_number, column_number, header)
		for trait in traits:
			row_number += 1
			for header in trait_fieldnames:
				if header in trait:
					if isinstance(trait[header], list):
						trait[header] = ", ".join([i for i in trait[header]])
					column_number = trait_fieldnames.index(header)
					trait_details_worksheet.write(row_number, column_number, trait[header])
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
			traits,
			base_filename=None,
			with_timestamp=True
	):
		self.make_csv_file(
			fieldnames,
			id_list,
			base_filename=base_filename,
			with_timestamp=with_timestamp
		)
		trait_fieldnames = [
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
		for i, trait in enumerate(traits):
			trait['realPosition'] = str(i + 1)
			trait['isVisible'] = 'True'
		self.make_csv_file(
			trait_fieldnames,
			traits,
			base_filename=base_filename,
			with_timestamp=with_timestamp,
			file_extension='trt'
		)
		# TODO may need to replace 'name' with 'trait' in header but doesn't seem to affect Field-Book

	def make_csv_table_template(
			self,
			fieldnames,
			id_list,
			traits,
			base_filename=None,
			with_timestamp=True
	):
		fieldnames += ['Date', 'Time', 'Person']
		fieldnames += [trait['name'] for trait in traits]
		self.make_csv_file(
			fieldnames,
			id_list,
			base_filename=base_filename + '_data',
			with_timestamp=with_timestamp
		)
		# and a file that describes the trait details
		trait_fieldnames = [
			'name',
			'format',
			'minimum',
			'maximum',
			'details',
			'category_list'
		]
		self.make_csv_file(
			trait_fieldnames,
			traits,
			base_filename=base_filename + '_details',
			with_timestamp=with_timestamp
		)

	def get_csv(
			self,
			country,
			region,
			farm,
			field_uid,
			block_uid,
			level,
			traits,
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
				' <-[:IS_IN]->(farm:Farm) '
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
				'UID',
				'Tissue',
				'Storage',
				'Date Sampled'
			]
			td = (
				' MATCH '
				'	(trait:SampleTrait) '
				'	<-[FOR_TRAIT*2]-(sample_trait) '
				'	<-[DATA_FOR]-(data)'
				' WHERE (trait.name) IN $traits' 
				' WITH user_name, partner_name, data, trait, sample_trait '
				' MATCH '
				'	(sample_trait) '
				'	-[:FOR_ITEM]->(sample:Sample) '
				'	-[:FROM_TREE*2]->(tree:Tree), '
				'	(sample)'
				'	-[:COLLECTED_AS*2]->(tist) '
				'	-[:OF_TISSUE]->(tissue), '
				'	(tist)-[:STORED_IN]->(storage), '
				'	(tree) '
			)
			# if block_uid is defined
			if block_uid != "":
				tdp = td + (
					'-[:IS_IN]->(: BlockTrees) '
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
					' -[:IS_IN]->(: BlockTrees) '
					' -[:IS_IN]->(block: Block) '
				)
			# else no field selected
			else:
				tdp = td + (
					' -[:IS_IN]->(: FieldTrees) '
					' -[:IS_IN]->(field:Field) '
				)
				optional_block = (
					' OPTIONAL MATCH (tree) '
					' -[:IS_IN]->(: BlockTrees) '
					' -[:IS_IN]->(block: Block) '
				)
			optional_block += (
				' OPTIONAL MATCH '
				'	(sample)-[:FROM_BRANCH {current: True}]->(branch:Branch) '
				' OPTIONAL MATCH '
				'	(sample)-[:FROM_LEAF {current:True}]->(leaf:Leaf) '
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
					' 	block.name as Block, '
					' 	block.uid as `Block UID`, '
					' 	tree.uid as `Tree UID`, '
					' 	tree.custom_id as `Tree Custom ID`, '
					' 	tree.variety as Variety, '
					' 	branch.uid as `Branch UID`, '
					' 	leaf.uid as `Leaf UID`, '
					' 	sample.uid as `Sample UID`, '
					'	tissue.name as Tissue, '
					'	storage.name as Storage, '
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
					'		Tissue: Tissue, '
					'		Storage: Storage, '
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
					'	Block: block.name, '
					'	`Block UID`: block.uid, '
					'	`Tree UID`: tree.uid, '
					'	`Tree Custom ID`: tree.custom_id,	'
					'	Variety: tree.variety, '
					'	`Sample UID`: sample.uid, '
					'	`Branch UID`: branch.uid, '
					'	`Leaf UID`: leaf.uid, '
					'	UID : sample.uid, '
					'	Tissue: tissue.name, '
					'	Storage: storage.name, '
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
				' WHERE (trait.name) IN $traits '
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
				'	(leaf)-[:FROM_BRANCH {current: True}]->(branch:Branch) '
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
				' WHERE (trait.name) IN $traits'
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
				' WHERE (trait.name) IN $traits '
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
				' WHERE (trait.name) IN $traits '
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
				' WHERE (trait.name) IN $traits'
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
					'	COLLECT([trait.name, data.value]) as Traits '
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
							record.update({i[0]: str(i[1])})
						# if the new value is a list (multicat)
						# make all the elements of the list into strings
						# and make a string out of this list of strings
						else:
							record.update({i[0]: str([str(y) for y in i[1]])})
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
							record[i[0]].append(str(i[1]))
						else:
							record[i[0]].append(str([str(y) for y in i[1]]))
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
						row[item] = ", ".join([i for i in row[item]])
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
