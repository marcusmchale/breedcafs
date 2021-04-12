

class Item:
	def __init__(self, uid):
		self.uid = uid


class Field(Item):
	def __init__(self, uid: int, name: str, name_lower: str):
		super().__init__(uid)
		self.name = name
		self.name_lower = name_lower


class Block(Item):
	def __init__(self, uid: str, fid: int, name: str, name_lower: str):
		super().__init__(uid)
		self.fid = fid
		self.name = name
		self.name_lower = name_lower


class Tree(Item):
	def __init__(self, uid: str, fid: int, name: str = None, row: int = None, column: int = None):
		super().__init__(uid)
		self.fid = fid
		self.name = name
		self.row = row
		self.column = column


class Sample(Item):
	def __init__(self, uid: str, fid: int, name: str = None, unit: str = None):
		super().__init__(uid)
		self.fid = fid
		self.name = name
		self.unit = unit
