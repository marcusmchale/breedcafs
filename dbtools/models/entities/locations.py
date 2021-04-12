class Location:
	def __init__(self, name: str):
		self.name = name


class Country(Location):
	def __init__(self, name: str):
		super().__init__(name)


class Region(Location):
	def __init__(self, name: str, country: Country):
		super().__init__(name)
		self.country = country


class Farm(Location):
	def __init__(self, name: str, country: Country, region: Region):
		super().__init__(name)
		self.country = country
		self.region = region

