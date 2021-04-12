from typing import List

from dbtools.models.entities.enums import RecordType, RecordFormat, ItemLevels


class InputVariable:
	def __init__(
			self,
			name: str,
			record_type: RecordType,
			record_format: RecordFormat,
			item_levels: ItemLevels,
			details: str,
			minimum: float = None,
			maximum: float = None,
			categories: List[str] = None
	):
		self.name = name
		self.record_type = record_type
		self.record_format = record_format
		self.item_levels = item_levels
		self.details = details
		self.minimum = minimum
		self.maximum = maximum
		self.categories = categories

