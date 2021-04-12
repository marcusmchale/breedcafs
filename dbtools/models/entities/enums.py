from enum import Enum


class ItemLevels(Enum):
	FIELD = 'field'
	BLOCK = 'block'
	TREE = 'tree'
	SAMPLE = 'sample'


class RecordType(Enum):
	PROPERTY = 'property'
	TRAIT = 'trait'
	CONDITION = 'condition'
	CURVE = 'curve'


class RecordFormat(Enum):
	LOCATION = 'location'
	DATE = 'date'
	MULTICAT = 'multicat'
	NUMERIC = 'numeric'
	TEXT = 'text'
	BOOLEAN = 'boolean'
	CATEGORICAL = 'categorical'
	PERCENT = 'percent'


class FileExtension(Enum):
	CSV = '.csv'
	XLSX = '.xlsx'
	FASTQ = '.fastq'
	FASTA = '.fasta'


class SubmissionType(Enum):
	DB = 'db'
	TABLE = 'table'
	SEQ = 'seq'
