from dbtools import logging
from typing import List

from dbtools.cypher.form_field_lists import queries


# the below is useful in considering combinations of parameters for filters to create the cypher queries
# from itertools import product
# [['field'] + [x for x in i if x] for i in product(('blocks', None), ('trees', None), ('samples', None))]


# the below belongs in the controller passing lists into the filtered queries
# from dbtools.utils.parsers import parse_range_list
# if field_uid_list:
# 	try:
# 		field_uid_list = parse_range_list(field_uid_list)
# 	except ValueError:
# 		return 'Invalid range of Field UIDs'
# if block_id_list:
# 	try:
# 		block_id_list = parse_range_list(block_id_list)
# 	except ValueError:
# 		return 'Invalid range of Block IDs'
# if tree_id_list:
# 	try:
# 		tree_id_list = parse_range_list(tree_id_list)
# 	except ValueError:
# 		return 'Invalid range of Tree IDs'
# if sample_id_list:
# 	try:
# 		sample_id_list = parse_range_list(sample_id_list)
# 	except ValueError:
# 		return 'Invalid range of Sample IDs'


def run_query(tx, item_label, filter_string, result_type, parameters):
	query_name = '_'.join(['get', item_label.lower(), result_type, filter_string]) + '.cypher'
	try:
		query = queries['query_name']
	except KeyError as e:
		raise KeyError('The requested query has not been loaded, it may not exist: %s' % query_name) from e
	result = tx.run(query, parameters)
	if result_type == 'tuples':
		return [tuple(record[0]) for record in result]
	elif result_type == 'count':
		return result.single()[0]


def parse_location_parameters(filter_dict):
	# hierarchy of source definitions are:
	#  - Greater specificity first, i.e. UID references then farm, region then country
	#  - List parameters take priority over single references
	# All list parameters must be supported by definite values for higher levels
	hierarchy = [('fields', 'uid'), ('farms', 'farm'), ('regions', 'region'), ('countries', 'country')]
	absolute_references = ['fields', 'uid']
	# first just check that all the supplied keys are recognised
	recognised_keys = [item for sublist in hierarchy for item in sublist]
	unrecognised_keys = [key for key in filter_dict.keys() if key not in recognised_keys]
	if unrecognised_keys:
		raise ValueError('Unrecognised keys in the supplied filter dict: %s', ', '.join(unrecognised_keys))
	parameters = dict()
	source = None
	for level in hierarchy:
		if not source:
			for key in level:
				if key in filter_dict and filter_dict[key]:
					if not source:
						source = key
						parameters[key] = filter_dict[key]
					break
		elif source in absolute_references:
			break
		else:
			key = level[1]
			if key in filter_dict and filter_dict[key]:
				parameters[key] = filter_dict[key]
			else:
				raise ValueError('%s is defined but %s is not' % (source, key))
	return source, parameters


def get_items_locations(
		tx,
		item_label: str,
		result_type: str,
		uid: int = None,
		fields: list = None,
		farms: list = None,
		regions: list = None,
		countries: list = None,
		farm: str = None,
		region: str = None,
		country: str = None
):
	filter_dict = {
		'uid': uid,
		'fields': fields,
		'farms': farms,
		'regions': regions,
		'countries': countries,
		'country': country,
		'region': region,
		'farm': farm
	}
	source, parameters = parse_location_parameters(filter_dict)
	return run_query(tx, item_label, source, result_type, parameters)


def get_items_id_list_filters(
		tx,
		item_label: str,
		result_type: str,
		field: int,
		blocks: List[int] = None,
		trees: List[int] = None,
		samples: List[int] = None
):
	if item_label.lower() in ['block', 'tree'] and any([trees, samples]):
		logging.warning('Cannot filter %s items by sample IDs. Ignoring these filters' % item_label)
		samples = None
		trees = None
	parameters = {
		'field': field,
	}
	if blocks:
		parameters['blocks'] = blocks
	if trees:
		parameters['trees'] = trees
	if samples:
		parameters['samples'] = samples
	filters = list(parameters.keys())
	filter_order = ['field', 'blocks', 'trees', 'samples']
	filters.sort(key=lambda key: filter_order.index(key))
	filter_string = '_'.join(filters)
	return run_query(tx, item_label, filter_string, result_type, parameters)
