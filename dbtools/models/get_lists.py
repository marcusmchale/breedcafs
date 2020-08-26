from app import logging

from .queries import Query



def get_properties(tx, username):
	return tx.run(cypher_queries['get_user_properties'], username=username)


class SelectionList:
	def __init__(
			self
	):
		pass

	@staticmethod
	def get_input_group_levels(
			input_group,
			partner=None,
			username=None
	):
		parameters = {
			"input_group": input_group,
			"username": username,
			"partner": partner
		}
		query = (
			' MATCH (ig:InputGroup {id: $input_group}) '
		)
		if partner or username:
			if partner:
				query += (
					' OPTIONAL MATCH (: Partner {name_lower: toLower(trim($partner))}) '
				)
			elif username:
				query += (
					' MATCH (: User {username_lower: toLower(trim($username))}) '
					'	-[: AFFILIATED {data_shared: True}]->(partner: Partner) '
					' OPTIONAL MATCH (partner) '
				)
			query += (
				'	<-[: AFFILIATED {data_shared: True}]-(: User) '
				'	-[: SUBMITTED]->(: Submissions) '
				'	-[: SUBMITTED]->(: InputGroups) '
				'	-[sub: SUBMITTED]->(ig) '
				' WITH ig ORDER BY sub LIMIT 1 '
			)
		else:
			query += (
				' OPTIONAL MATCH (ig)<-[sub:SUBMITTED]-() '
				' WITH ig WHERE sub IS NULL '
			)
		query += (
			' MATCH (ig)-[: AT_LEVEL]->(level: ItemLevel) '
			' RETURN DISTINCT [level.name_lower, level.name] '
		)
		result = Query().get_bolt(query, parameters)
		result = [tuple(record[0]) for record in result]
		return sorted(result, key=lambda x: ["field", "block", "tree", "sample"].index(x[0]))

	@staticmethod
	def get_inputs(
			input_group=None,
			record_type=None,
			item_level=None,
			inverse=False,
			inverse_filter=None,
			details=False
	):
		parameters = {
			"input_group": input_group,
			"record_type": record_type,
			"item_level": item_level,
			"inverse_filter": inverse_filter
		}
		if input_group:
			# these are used in relation to filtering groups
			# but want to still provide default groups "un-submitted" for username/partner filters
			query = (
				' MATCH'
				'	(input:Input)-[position:IN_GROUP]->(:InputGroup {id: $input_group}) '
				' WITH input ORDER BY position.position '
			)
		else:
			query = (
				' MATCH '
				'	(input:Input) '
				' WITH input ORDER BY input.name_lower '
			)
		if inverse:
			if input_group:
				query += (
					' WITH collect(input) as selected_inputs '
					' MATCH (input:Input) WHERE NOT input IN selected_inputs '
				)
			if inverse_filter:
				query += (
					' MATCH '
					'	(input)-[position:IN_GROUP]->(:InputGroup {id: $inverse_filter}) '
					' WITH input ORDER BY position.position '
				)
			else:
				query += (
					' WITH input '
					' ORDER BY input.name_lower '
				)
		if record_type:
			query += (
				' MATCH '
				'	(input) '
				'	-[:OF_TYPE]->(: RecordType { '
				'		name_lower: toLower($record_type) '
				'	}) '
			)
		if item_level:
			query += (
				' MATCH '
				'	(input) '
				'	-[:AT_LEVEL]->(: ItemLevel { '
				'		name_lower: toLower($item_level) '
				'	}) '
			)
		if details:
			query += (
				' OPTIONAL MATCH '
				'	(input)-[: AT_LEVEL]->(level: ItemLevel) '
				' OPTIONAL MATCH '
				'	(input)-[: OF_TYPE]->(record_type: RecordType) '
				' WITH input, collect(DISTINCT level.name_lower) as levels, record_type '
			)
			query += (
				' RETURN { '
				'	name: input.name, '
				'	name_lower: input.name_lower, '
				'	format: input.format, '
				'	minimum: input.minimum, '
				'	maximum: input.maximum, '
				'	details: input.details, '
				'	category_list: input.category_list, '
				'	record_type: record_type.name_lower, '
				'	levels: levels '
				' } '
			)
		else:
			query += (
				' RETURN [ '
				'	input.name_lower, '
				'	input.name '
				' ] '
			)
		result = Query().get_bolt(query, parameters)
		return [tuple(record[0]) for record in result]

	@staticmethod
	def get_partners():
		parameters = {}
		query = (
			' MATCH (partner:Partner) '
			' RETURN [ '
			'	partner.name, '
			'	partner.fullname'
			' ] '
			' ORDER BY partner.fullname'
		)
		result = Query().get_bolt(query, parameters)
		return [tuple(record[0]) for record in result]



	@staticmethod
	def get_input_groups(
			item_level=None,
			record_type=None,
			partner=None,
			username=None,
			include_defaults=False
	):
		parameters = {
			"item_level": item_level,
			"record_type": record_type,
			"partner": partner,
			"username": username
		}
		query = (
			' MATCH '
			'	(ig: InputGroup) '
		)
		if item_level:
			query += (
				' MATCH (ig)-[:AT_LEVEL]->(:ItemLevel {name_lower:toLower($item_level)}) '
			)
		if record_type:
			query += (
				' MATCH '
				'	(ig) '
				'	<-[:IN_GROUP]-(: Input) '
				'	-[:OF_TYPE]->(: RecordType {name_lower: toLower($record_type)}) '
			)
		if partner or username:
			match = ' MATCH '
			if include_defaults:
				match = ' OPTIONAL MATCH '
			query += match + (
				'	(ig)'
				'	<-[: SUBMITTED]-(: InputGroups) '
				'	<-[: SUBMITTED]-(: Submissions) '
				'	<-[: SUBMITTED]-(: User) '
				'	-[:AFFILIATED {data_shared: True}]->(partner: Partner) '
			)
			if partner:
				query += (
					' WHERE partner.name_lower = toLower($partner) '
				)
			if username:
				query += match + (
					'	(partner) '
					'	<-[:AFFILIATED {data_shared: True}]-(user: User { '
					'		username_lower: toLower($username) '
					'	}) '
				)
			if include_defaults:
				query += (
					' WITH ig, partner '
					'	WHERE partner IS NULL '
					'	OR user IS NOT NULL '
				)
			else:
				query += (
					' WITH ig, partner '
				)
		else:
			query += (
				' OPTIONAL MATCH (ig)<-[sub: SUBMITTED]-() '
				' WITH ig WHERE sub IS NULL '
			)
		if partner or username and include_defaults:
			query += (
				' RETURN [ '
				'	toString(ig.id), '	
				'	CASE '
				'		WHEN partner IS NULL THEN ig.name + " (default)" '
				'		ELSE ig.name '
				'	END '
				' ] '
				' ORDER BY ig.name_lower '
			)
		else:
			query += (
				' RETURN [ '
				'	toString(ig.id), '
				'	ig.name '
				' ] '
				' ORDER BY ig.name_lower '
			)
		result = Query().get_bolt(query, parameters)
		return [tuple(record[0]) for record in result]

	@staticmethod
	def get_record_types():
		query = (
			' MATCH '
			'	(record_type: RecordType) '
			' RETURN [ '
			'	record_type.name_lower, '
			'	record_type.name'
			' ] '
			' ORDER BY record_type.name_lower '
		)
		result = Query().get_bolt(query)
		return [tuple(record[0]) for record in result]

	@staticmethod
	def get_item_levels():
		query = (
			' MATCH '
			'	(item_level: ItemLevel) '
			' RETURN [item_level.name_lower, item_level.name] '
		)
		result = Query().get_bolt(query)
		result = [tuple(record[0]) for record in result]
		return sorted(result, key=lambda x: ["field", "block", "tree", "sample"].index(x[0]))




class ItemList:
	def __init__(self):
		pass

	@staticmethod
	def build_match_item_query(record_data):
		clause_tuples = [
			(record_data['field_uid_list'] and not record_data['field_uid'], ' field.uid IN $field_uid_list '),
			(record_data['block_id_list'] and not record_data['block_uid'], ' block.id IN $block_id_list '),
			(record_data['tree_id_list'], ' tree.id IN $tree_id_list '),
			(record_data['sample_id_list'], ' sample.id IN $sample_id_list')
		]
		parameters = {
			'item_level': record_data['item_level'],
		}
		if clause_tuples[0][0]:
			parameters['field_uid_list'] = record_data['field_uid_list']
		if clause_tuples[1][0]:
			parameters['block_id_list'] = record_data['block_id_list']
		if clause_tuples[2][0]:
			parameters['tree_id_list'] = record_data['tree_id_list']
		if clause_tuples[3][0]:
			parameters['sample_id_list'] = record_data['sample_id_list']
		query = (
			' MATCH (country: Country '
		)
		if record_data['country']:
			parameters['country'] = record_data['country']
			query += (
				' {name_lower: toLower($country)} '
			)
		query += (
			' )<-[: IS_IN]-(region: Region '
		)
		if record_data['region']:
			parameters['region'] = record_data['region']
			query += (
				' {name_lower: toLower($region)} '
			)
		query += (
			' )<-[: IS_IN]-(farm: Farm '
		)
		if record_data['farm']:
			parameters['farm'] = record_data['farm']
			query += (
				' {name_lower: toLower($farm)} '
			)
		query += (
			' )<-[: IS_IN]-(field: Field '
		)
		if record_data['field_uid']:
			parameters['field_uid'] = record_data['field_uid']
			query += (
				' {uid: $field_uid} '
			)
		query += (
			' ) '
		)
		if record_data['item_level'] == 'field':
			if clause_tuples[0][0]:
				query += ' WHERE ' + clause_tuples[0][1]
			query += (
				' WITH '
				' country, region, farm, field as item '
			)
		if any([
			record_data['item_level'] == 'block',
			record_data['block_uid'],
			record_data['block_id_list']
		]):
			query += (
				' <-[: IS_IN]-(:FieldBlocks) '
				' <-[: IS_IN]-(block: Block '
			)
			if record_data['block_uid']:
				parameters['block_uid'] = record_data['block_uid']
				query += (
					' {uid: $block_uid} '
				)
			query += (
				' ) '
			)
			if record_data['item_level'] == 'block':
				if any([i[0] for i in clause_tuples[0:2]]):
					query += (
						' WHERE '
					)
					query += (
						' AND '.join(
							[i[1] for i in clause_tuples[0:2] if i[0]]
						)
					)
				query += (
					' WITH '
					' country, region, farm, field, block as item '
				)
		if any([
			record_data['item_level'] == 'tree',
			record_data['tree_id_list']
		]):
			if record_data['block_uid'] or record_data['block_id_list']:
				parameters['block_uid'] = record_data['block_uid']
				query += (
					' <-[: IS_IN]-(: BlockTrees)<-[: IS_IN]- '
				)
			else:
				query += (
					' <-[ :IS_IN]-(: FieldTrees)<-[: IS_IN]- '
				)
			query += (
				' (tree: Tree) '
			)
			if record_data['item_level'] == 'tree':
				if any([i[0] for i in clause_tuples[0:3]]):
					query += (
						' WHERE '
					)
					query += (
						' AND '.join(
							[i[1] for i in clause_tuples[0:3] if i[0]]
						)
					)
				if any([record_data['block_uid'], record_data['block_id_list']]):
					query += (
						' WITH '
						' country, region, farm, field, block, tree as item '
					)
				else:
					query += (
						' WITH '
						' country, region, farm, field, tree as item '
					)
		if record_data['item_level'] == 'sample':
			query += (
				' <-[: FROM | IS_IN* ]-(: ItemSamples) '
				' <-[: FROM* ]-(sample: Sample) '
			)
			if record_data['item_level'] == 'sample':
				if any([i[0] for i in clause_tuples[0:4]]):
					query += (
						' WHERE '
					)
					query += (
						' AND '.join(
							[i[1] for i in clause_tuples[0:4] if i[0]]
						)
					)
				query += (
					' WITH '
					'	country, region, farm, field, sample as item '
				)
				if any([record_data['block_uid'], record_data['block_id_list']]):
					query += (
						', block '
					)
				if record_data['tree_id_list']:
					query += (
						', tree'
					)
		# Optional matches
		if record_data['item_level'] in ['tree', 'sample']:
			if not any([record_data['block_uid'], record_data['block_id_list']]):
				query += (
					' OPTIONAL MATCH '
					'	(item)-[: FROM | IS_IN*]->(block: Block) '
				)
			if record_data['item_level'] == 'sample':
				query += (
					' WITH '
					'	item, '
					'	country, region, farm, '
					'	field,'
					'	block '
					' ORDER BY field.uid, block.id '
					' WITH '
					'	item, '
					'	country, region, farm, '
					'	field, '
					'	collect(distinct block) as blocks '
				)
				if not record_data['tree_id_list']:
					query += (
						' OPTIONAL MATCH '
						'	(item)-[:FROM*]->(tree:Tree) '
						' WITH '
						' 	item, '
						'	country, region, farm, '
						'	field, blocks, '
						'	tree '
						' ORDER BY field.uid, tree.id '
						' WITH '
						' 	item, '
						'	country, region, farm, '
						'	field, blocks, '
						'	collect(distinct tree) as trees '
					)
				query += (
					' OPTIONAL MATCH '
					'	p = (item)-[:FROM*]->(sample: Sample) '
					' WITH '
					'	item, '
					'	country, region, farm,'
					'	field, blocks, trees,'
					'	sample '
					' ORDER BY field.uid, item.id, length(p), sample.id '
					' WITH '
					'	item, '
					'	country, region, farm, '
					'	field, blocks, trees, '
					'	collect(distinct sample) as samples '
					' OPTIONAL MATCH '
					'	(item)-[:FROM]->(source_sample: Sample) '
					' WITH '
					'	item, '
					'	country, region, farm, '
					'	field, blocks, trees, samples, '
					'	source_sample '
					' ORDER BY field.uid, item.id, source_sample.id '
					' WITH '
					'	item, '
					'	country, region, farm, '
					'	field, blocks, trees, samples, '
					'	collect(distinct source_sample) as source_samples '
				)
		return query, parameters

	def generate_id_list(self, record_data):
		query, parameters = self.build_match_item_query(record_data)
		query += (
			' RETURN { '
			'	UID: item.uid, '
			'	Name: item.name, '
			'	Varieties: item.varieties, '
			'	Country: country.name, '
			'	Region: region.name, '
			'	Farm: farm.name, '
		)
		if parameters['item_level'] == 'field':
			query += (
				' Field: item.name, '
				' Elevation: item.elevation, '
				' Time: apoc.date.format(item.time, "ms", "yyyy-MM-dd HH:mm") '
			)
		else:
			query += (
				' Field: field.name, '
				' `Field UID`: field.uid, '
			)
			if parameters['item_level'] == 'block':
				query += (
					' Block: item.name, '
					' Time: apoc.date.format(coalesce(item.time, field.time), "ms", "yyyy-MM-dd HH:mm") '
				)
			elif parameters['item_level'] == 'tree':
				query += (
					' Block: block.name, '
					' `Block ID` : block.id, '
					' `Tree ID`: item.id, '
					' Row: item.row, '
					' Column: item.column, '
					' Time: apoc.date.format(coalesce(item.time, block.time, field.time), "ms", "yyyy-MM-dd HH:mm") '
				)
			elif parameters['item_level'] == 'sample':
				query += (
					' Blocks: [x in blocks | x.name], '
					' `Block IDs` : [x in blocks | x.id], '
					' `Tree IDs`: [x in trees | x.id], '
					' `Tree Names`: [x in trees | x.name], '
					' `Source Sample IDs`: [x in samples | x.id], '
					' `Source Sample Names`: [x in samples | x.name], '
					' Unit: item.unit, '
					' source_level: CASE '
					'	WHEN size(source_samples) <> 0 THEN "Sample" '
					'	WHEN size(trees) <> 0 THEN "Tree" '
					'	WHEN size(blocks) <> 0 THEN "Block" '
					'	ELSE "Field" '
					'	END, '
					' source_ids: CASE '
					'	WHEN size(source_samples) <> 0 THEN [x in source_samples | x.id ] '
					'	WHEN size(trees) <> 0 THEN [x in trees | x.id ] '
					'	WHEN size(blocks) <> 0 THEN [x in blocks | x.id ] '
					'	ELSE [field.uid]  '
					'	END, '
					' Time: apoc.date.format(coalesce(item.time, samples[0].time), "ms", "yyyy-MM-dd HH:mm") '
				)
		query += (
			' } '
			' ORDER BY '
		)
		if parameters['item_level'] == 'field':
			query += (
				' item.uid '
			)
		else:
			query += (
				' field.uid, item.id '
			)
		result = Query().get_bolt(query, parameters)
		return result
