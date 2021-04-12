from typing import List, Optional, Union


from dbtools.models.entities.input_variables import InputVariable
from datetime import datetime


class Record:
	def __init__(
			self,
			input_variable: InputVariable
	):
		self.input_variable = input_variable


class PropertyRecord(Record):
	def __init__(
			self,
			input_variable: InputVariable,
			value: Union[str, float, bool, List[str, float, bool]]
	):
		super().__init__(input_variable)
		self.value = value


class TraitRecord(Record):
	def __init__(
			self,
			input_variable: InputVariable,
			value: Union[str, float, bool, List[str, float, bool]],
			time: datetime.time,
			replicate: int = 0
	):
		super().__init__(input_variable)
		self.value = value
		self.time = time
		self.replicate = replicate


class ConditionRecord(Record):
	def __init__(
			self,
			input_variable: InputVariable,
			value: Union[str, float, bool, List[str, float, bool]],
			start: Optional[datetime.time] = None,
			end: Optional[datetime.time] = None,
			replicate: int = 0
	):
		super().__init__(input_variable)
		self.value = value
		self.start = start
		self.end = end
		self.replicate = replicate


class CurveRecord(Record):
	def __init__(
			self,
			input_variable: InputVariable,
			x_values: List[float],
			y_values: List[float],
			time: datetime.time,
			replicate: int = 0
	):
		super().__init__(input_variable)
		self.x_values = x_values
		self.y_values = y_values
		self.time = time
		self.replicate = replicate
