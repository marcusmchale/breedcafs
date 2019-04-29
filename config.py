DEBUG = False

# NB: only upper case config variables are stored in the app.Config object
RECORD_TYPES = [
	'property',
	'trait',
	'condition'
]

# record type to worksheet name dictionary
WORKSHEET_NAMES = {
	'mixed': 'Records',
	'property': 'Properties (Input)',
	'trait': 'Traits (Input)',
	'condition': 'Conditions (Input)'
}