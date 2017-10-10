from app import app
from app.cypher import Cypher
from user import User
from lists import Lists
from config import uri, driver, ALLOWED_EXTENSIONS
import cStringIO
import unicodecsv as csv 

#User class related (all uploads are tied to a user) yet more specifically regarding uploads
class Download(User):
	def __init__(self, username):
		self.username=username
	def get_csv(self, country, region, farm, plotID, blockUID, level, traits):
		self.country = country
		self.region = region
		self.farm = farm
		self.plotID = plotID
		self.blockUID = blockUID
		self.level = level
		self.traits = traits
		with driver.session() as session:
			return session.read_transaction(self._get_csv)
	def _get_csv(self, tx):
		country = self.country
		region = self.region
		farm = self.farm
		plotID = self.plotID
		blockUID = self.blockUID
		level = self.level
		traits = [i.encode('utf-8') for i in self.traits]
		#build query strings - always be careful not to allow injection when doing this!!
		#all of the input needs to be validated (in this case it is already done by WTForms checking against selectfield options)
		#but as I am not entirely sure about the security of this layer I am adding another check of the values before building strings into the query
		TREETRAITS = Lists('TreeTrait').create_list('name')
		BLOCKTRAITS = Lists('BlockTrait').create_list('name')
		#the below commented out because these are not entered as strings, rather they are used as parameters so are safe from injection
		#COUNTRIES = set(Lists('Country').create_list('name'))
		#REGIONS = set(Lists('Country').get_connected('name', form.country.data, 'IS_IN'))
		#FARMS = set(Fields(form.country.data).get_farms(form.region.data))
		#PLOTS = set(Fields(form.country.data).get_plotIDs(form.region.data, form.farm.data))
		#BLOCKS = set(Fields.get_blockUIDs(form.plot.data))
		#
		#this section first as not dependent on level
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
			response = ( ' RETURN {Trait : trait.name, '
					' Details : trait.details, '
					' Value : data.value, '
					' UID : tree.uid ,'
					' PlotID : plot.uid, '
					' TreeID : tree.id, '
					' Plot : plot.name, '
					' Farm : farm.name, '
					' Region : region.name, '
					' Country : country.name } '
				' ORDER BY block.uid ' )
			query = query + response
			#create the list of fieldnames to use in the CSV later
			fieldnames = ['Trait',
				'Details',
				'Value',
				'UID',
				'PlotID',
				'TreeID',
				'Block',
				'Plot', 
				'Farm', 
				'Region', 
				'Country']
		if level == 'block' and set(traits).issubset(set(BLOCKTRAITS)):
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
			response = (' RETURN {Trait : trait.name, '
					' Details : trait.details, '
					' Value : data.value, '
					' UID : block.uid ,'
					' PlotID : plot.uid, '
					' BlockID : block.id, '
					' Block : block.name, '
					' Plot : plot.name, '
					' Farm : farm.name, '
					' Region : region.name, '
					' Country : country.name } '
				' ORDER BY block.uid ')
			query = query + response
			#create the list of fieldnames to use in the CSV later
			fieldnames = ['Trait',
				'Details',
				'Value',
				'UID',
				'PlotID',
				'BlockID',
				'Block',
				'Plot', 
				'Farm', 
				'Region', 
				'Country']
		result = tx.run(query,
			country = country,
			region = region,
			farm = farm,
			plotID = plotID,
			blockUID = blockUID,
			traits = traits)
		result_dict = [record[0] for record in result]
		#now create the csv file from this result
		data_file = cStringIO.StringIO()
		writer = csv.DictWriter(data_file, 
		fieldnames=fieldnames, 
		quoting=csv.QUOTE_ALL,
		extrasaction='ignore')
		writer.writeheader()
		for i, item in enumerate(result_dict):
			writer.writerow(item)
		data_file.seek(0)
		return data_file
