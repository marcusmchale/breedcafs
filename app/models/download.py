from app import app
from app.cypher import Cypher
from user import User
from config import uri, driver, ALLOWED_EXTENSIONS


#User class related (all uploads are tied to a user) yet more specifically regarding uploads
class Download(User):
	def __init__(self, username):
		self.username=username
	def get_csv(self, country, region, farm, plotID, blockUID, level, traits):
		self.country = country
		self.region = region
		self.farm = farm
		self.plotID = plotID
		self.blockUID = blockID
		self.level = level
		self.traits = traits
		with driver.session() as session:
			return session.read_transaction(self._get_csv)
	def _get_csv(self, tx):

		for record in tx.run(Cypher.get_csv,
			country = self.country,
			region = self.region,
			farm = self.farm,
			plotID = self,plotID,
			blockUID = self.blockUID,
			level = self.level,
			traits = self.traits):

		
		if country != "":
			q = ' MATCH (c:Country {name:' + country + '})'
		if region != "":
			q = q + ' -[:IS_IN]-(r:Region {name:' + region + '})'
		if farm != "":
			q = q + ' -[:IS_IN]-(f:Farm {name:' + farm + '})'
		if plotID != "":
			q = q + ' -[:IS_IN]-(p:Plot {uid:' + plotID + '})'
		if blockUID != "":
			q = q + ' -[:CONTAINS_BLOCKS]->(pb:PlotBlocks) ' \
				' -[:REGISTERED_BLOCK]-(b:Block {uid:' + blockUID + '}) '

			return record

	def create_trt(self, selection, keyby):
		#key by is the value from the form to search for a match with a key in the dict
		#this is originally in the tuple generated for the form
		self.keyby=keyby
		self.selection=selection
		selected_traits = self.get_selected()
		fieldnames = ['name',
			'format',
			'defaultValue',
			'minimum',
			'maximum',
			'details',
			'categories',
			'isVisible',
			'realPosition']
		trt = cStringIO.StringIO()
		writer = csv.DictWriter(trt, 
			fieldnames=fieldnames, 
			quoting=csv.QUOTE_ALL,
			extrasaction='ignore')
		writer.writeheader()
		for i, trait in enumerate(selected_traits):
			trait['realPosition'] = str(i+1)
			trait['isVisible'] ='True'
			writer.writerow(trait)
		trt.seek(0)
		return trt
