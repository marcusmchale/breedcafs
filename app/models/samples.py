import unicodecsv as csv 
import cStringIO
from app import app
from app.cypher import Cypher
from config import uri, driver
from flask import session

class Samples:
	def __init__ (self):
		pass
	def add_tissue(self, tissue):
		self.tissue = tissue
		with driver.session() as session:
			session.write_transaction(self._add_tissue)
	def _add_tissue(self, tx):
		tx.run(Cypher.tissue_add,
			tissue = self.tissue,
			username = session['username'])
# should really reduce these calls down to a common function, lots of repitition here!!
	def add_storage(self, storage):
		self.storage = storage
		with driver.session() as session:
			session.write_transaction(self._add_storage)
	def _add_storage(self, tx):
		tx.run(Cypher.storage_add,
			storage = self.storage,
			username = session['username'])
	def add_samples(self, plotID, start, end, replicates, tissue, storage, date):
		self.plotID = plotID
		self.start = start
		self.end = end
		self.replicates = replicates
		self.tissue = tissue
		self.storage = storage
		self.date = date
		with driver.session() as session:
			session.write_transaction(self._add_samples)
		fieldnames= ['UID', 'PlotID', 'TreeID', 'TreeName', 'SampleID', 'Date', 'Tissue', 'Storage', 'Plot', 'Farm', 'Region', 'Country']
		samples_csv = cStringIO.StringIO()
		writer = csv.DictWriter(samples_csv,
			fieldnames=fieldnames,
			quoting=csv.QUOTE_ALL,
			extrasaction='ignore')
		writer.writeheader()
		for sample in self.id_list:
			writer.writerow(sample)
		samples_csv.seek(0)
		return samples_csv
	def _add_samples(self, tx):
		tx.run(Cypher.sample_id_lock,
			plotID = self.plotID)
		result=tx.run(Cypher.samples_add,
			plotID = self.plotID,
			start = self.start,
			end = self.end,
			replicates = self.replicates,
			tissue = self.tissue,
			storage=self.storage,
			date=self.date,
			username = session['username'])
		self.id_list = [record[0] for record in result]
