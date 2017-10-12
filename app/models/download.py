import os
from app import app
from app.cypher import Cypher
from user import User
from lists import Lists
from config import uri, driver, ALLOWED_EXTENSIONS
import cStringIO
import unicodecsv as csv
from datetime import datetime

#User class related (all uploads are tied to a user) yet more specifically regarding uploads
class Download(User):
	def __init__(self, username):
		self.username=username
	def get_csv(self, country, region, farm, plotID, blockUID, level, traits, data_format, start_date, end_date):
		self.country = country
		self.region = region
		self.farm = farm
		self.plotID = plotID
		self.blockUID = blockUID
		traits = [i.encode('utf-8') for i in traits]
		self.traits=traits
		self.start_date = start_date
		self.end_date = end_date
		#build query strings - always be careful not to allow injection when doing this!!
		#all of the input needs to be validated (in this case it is already done by WTForms checking against selectfield options)
		#but as I am not entirely sure about the security of this layer I am adding another check of the values that are used to concatenate the string
		TREETRAITS = Lists('TreeTrait').create_list('name')
		BLOCKTRAITS = Lists('BlockTrait').create_list('name')
		#this section first as not dependent on level but part of query build
		if country == "" :
			frc = (' -[:IS_IN]->(farm:Farm) '
				' -[:IS_IN]->(region:Region) '
				' -[:IS_IN]->(country:Country) ')
		elif country != "" and region == "":
			frc = (' -[:IS_IN]->(farm:Farm) '
				' -[:IS_IN]->(region:Region) '
				' -[:IS_IN]->(country:Country {name:$country}) ')
		elif region != "" and farm == "" :
			frc = (' <-[:IS_IN]->(farm:Farm) '
				' -[:IS_IN]->(region:Region {name:$region}) '
				' -[:IS_IN]->(country:Country {name:$country}) ')
		elif farm != "" :
			frc = (' -[:IS_IN]->(farm:Farm {name:$farm}) '
				' -[:IS_IN]->(region:Region {name:$region}) '
				' -[:IS_IN]->(country:Country {name:$country}) ')
		#then level dependent stuff
		if level == 'tree' and set(traits).issubset(set(TREETRAITS)):
			index_fieldnames = [
				'Country',
				'Region',
				'Farm', 
				'Plot', 
				'Block',
				'PlotID',
				'TreeID',
				'UID']
			td = ( ' MATCH (trait:TreeTrait) '
				' WHERE (trait.name) IN ' + str(traits) +
				' WITH trait '
				' MATCH (trait)-[:PLOT_DATA]->(PlotTreeTraitData) '
				' -[:SUBMISSIONS]->(data:Data)-[:DATA_FOR]->(tree:Tree) ' )
			#if no plot selected
			if plotID == "" :
				tdp = td + ( ' <-[:REGISTERED_TREE]-(PlotTrees) '
					' <-[:CONTAINS_TREES]-(plot:Plot) ' )
				query = tdp + frc +	' OPTIONAL MATCH (tree)-[:CONTAINS_TREE]-(block:Block)'
			#if plotID is defined (but no blockUID)
			elif blockUID == "" and plotID != "" :
				tdp = td + ( ' <-[:REGISTERED_TREE]-(PlotTrees) '
					' <-[:CONTAINS_TREES]-(plot:Plot {uid:$plotID}) ' )
				query = tdp + frc +	' OPTIONAL MATCH (tree)-[:CONTAINS_TREE]-(block:Block)'
			#if blockID is defined
			elif blockUID != "":
				tdp = td + ( '<-[:CONTAINS_TREE]-(block:Block {uid:$blockUID}) '
					' <-[:REGISTERED_BLOCK]-(:PlotBlocks) '
					' <-[:CONTAINS_BLOCKS]-(plot:Plot) ')
				query = tdp + frc
			#and generate the return statement
			if data_format == 'table':
				response = ( 
					#need a with statement to allow order by with COLLECT
					' WITH '
						' tree.uid as UID, '
						' plot.uid as PlotID, '
						' tree.id as TreeID, '
						' block.name as Block, '
						' plot.name as Plot, '
						' farm.name as Farm, '
						' region.name as Region, '
						' country.name as Country, '
						' COLLECT([trait.name, data.value]) as Traits '
					' RETURN '
						' { UID : UID ,'
						' PlotID : PlotID, '
						' TreeID : TreeID, '
						' Block : Block, '
						' Plot : Plot, '
						' Farm : Farm, '
						' Region : Region, '
						' Country : Country, '
						' Traits : Traits } '
					' ORDER BY '
						' PlotID, TreeID ' )
			elif data_format == 'db':
				response = (
					' RETURN {'
						' Country : country.name, ' 
						' Region : region.name, '
						' Farm : farm.name, '
						' Plot : plot.name, '
						' Block : block.name, '
						' PlotID : plot.uid, '
						' TreeID : tree.id, '
						' UID : tree.uid, '
						' Trait : trait.name, '
						' Value : data.value, '
						' Location : data.location, '
						' Recorded_at : data.timeFB, '
						' Recorded_by: data.person '
					'}'
					)
			query = query + response
			#defining these as headers for tree data
		elif level == 'block' and set(traits).issubset(set(BLOCKTRAITS)):
			index_fieldnames = [
				'Country',
				'Region',
				'Farm', 
				'Plot', 
				'PlotID',
				'Block',
				'BlockID',
				'UID']
			td = ( ' MATCH (trait:BlockTrait) '
				' WHERE (trait.name) IN ' + str(traits) +
				' WITH trait '
				' MATCH (trait)-[:PLOT_DATA]->(PlotBlockTraitData) '
				' -[:SUBMISSIONS]->(data:Data) ' )
			if blockUID != "" :
				tdp = td + ( ' -[:DATA_FOR]->(block:Block {uid:$blockUID}) '
					' <-[:REGISTERED_BLOCK]-(:PlotBlocks) '
					' <-[:CONTAINS_BLOCKS]-(plot:Plot) ' )
			if plotID != "" :
				tdp = td + ( ' -[:DATA_FOR]->(block:Block) '
					' <-[:REGISTERED_BLOCK]-(:PlotBlocks)'
					' <-[:CONTAINS_BLOCKS]-(plot:Plot {uid:$plotID}) ' )
			elif plotID == "" :
				tdp = td + ( ' -[:DATA_FOR]->(block:Block) '
					' <-[:REGISTERED_BLOCK]-(:PlotBlocks)'
					' <-[:CONTAINS_BLOCKS]-(plot:Plot) ' )
			query = tdp + frc
			if data_format == 'table':
				response = ( 
					#need a with statement to allow order by with COLLECT
					' WITH '
						' block.uid as UID, '
						' plot.uid as PlotID, '
						' block.id as BlockID, '
						' block.name as Block, '
						' plot.name as Plot, '
						' farm.name as Farm, '
						' region.name as Region, '
						' country.name as Country, '
						' COLLECT([trait.name, data.value]) as Traits '
					' RETURN {'
						' UID : UID ,'
						' PlotID : PlotID, '
						' BlockID : BlockID, '
						' Block : Block, '
						' Plot : Plot, '
						' Farm : Farm, '
						' Region : Region, '
						' Country : Country, '
						' Traits : Traits '
					'}'
					' ORDER BY '
						' PlotID, BlockID ' )
			elif data_format == 'db':
				response = (
					' RETURN { '
						' Country : country.name, ' 
						' Region : region.name, '
						' Farm : farm.name, '
						' Plot : plot.name, '
						' PlotID : plot.uid, '
						' Block : block.name, '
						' BlockID : block.id, '
						' UID : block.uid, '
						' Trait : trait.name, '
						' Value : data.value, '
						' Location : data.location, '
						' Recorded_at : data.timeFB, '
						' Recorded_by : data.person'
					'}'
					)
			query = query + response
		self.query = query
		with driver.session() as session:
			result = session.read_transaction(self._get_csv)
		if data_format == 'table':
			#expand the traits and values as key:value pairs
			[[record.update({i[0]:i[1] for i in record['Traits']})] for record in result]
				#now pop out the Traits key and add the returned trait names to a set of fieldnames
				#trait_fieldnames = set()
				#[[trait_fieldnames.add(i[0]) for i in record.pop('Traits')] for record in result]
			#replaced the above with using the fieldnames of traits from the form, this will provide a predictable output from a user perspective )
			#will also allow me to do all this as a stream once the files get larger - thanks Tony for this advice
			if set(traits).issubset(set(BLOCKTRAITS)) or set(traits).issubset(set(TREETRAITS)):
				trait_fieldnames = traits
			#now create the csv file from result and fieldnames
			fieldnames =  index_fieldnames + list(trait_fieldnames)
		elif data_format == 'db':
			fieldnames = index_fieldnames + ['Trait', 'Value', 'Location', 'Recorded_at', 'Recorded_by']
		time = datetime.now().strftime('%Y%m%d-%H%M%S_')
		filename = time + self.username + '.csv'
		file_path = os.path.join(app.instance_path, app.config['DOWNLOAD_FOLDER'], filename)
		with open(file_path,'w') as data_file:
			writer = csv.DictWriter(data_file, 
				fieldnames=fieldnames, 
				quoting=csv.QUOTE_ALL,
				extrasaction='ignore')
			writer.writeheader()
			for i, item in enumerate(result):
				writer.writerow(item)
		return filename
	def _get_csv(self, tx):
		#perform transaction and return the simplified result (remove all the transaction metadata and just keep the result map)
		return [record[0] for record in tx.run(self.query,
			country = self.country,
			region = self.region,
			farm = self.farm,
			plotID = self.plotID,
			blockUID = self.blockUID,
			traits = self.traits)]