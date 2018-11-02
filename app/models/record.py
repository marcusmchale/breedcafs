from app import app, ServiceUnavailable, AuthError

from app.cypher import Cypher
from app.emails import send_email
from flask import render_template, url_for, jsonify

from neo4j_driver import get_driver, neo4j_query

import datetime


class Record:
	def __init__(self, username):
		self.username = username
		# create a transaction session

	@staticmethod
	def find_condition_conflicts(
			start,
			end,
			field_uid,
			conditions_dict
	):
		# make a list of the conditions that are actually set
		conditions = [i for i, j in conditions_dict.iteritems() if j]
		found_values = {}
		with get_driver().session() as neo4j_session:
			for condition in conditions:
				parameters = {
					'start': start,
					'end': end,
					'field_uid': field_uid,
					'condition': condition,
					'value': conditions_dict[condition]
				}
				result = neo4j_session.read_transaction(
					neo4j_query,
					Cypher.existing_field_condition,
					parameters
				)
				result_list = [record for record in result]
				if result_list:
					found_values[condition] = result_list
		return found_values

	@staticmethod
	def find_treatment_conflicts(parameters):
		with get_driver().session() as neo4j_session:
			if parameters['block_uid']:
				result = neo4j_session.read_transaction(
					neo4j_query,
					Cypher.existing_block_tree_treatment,
					parameters
				)
			else:
				result = neo4j_session.read_transaction(
					neo4j_query,
					Cypher.existing_field_tree_treatment,
					parameters
				)
			result_list = [record for record in result]
		return result_list

	@staticmethod
	def condition_result_table(conflicts):
		header_string = (
				'<tr><th><p>Condition</p></th><th><p>Start</p></th><th><p>End</p></th>'
				+ '<th><p>Value</p></th><th><p>Submitted</p></th><th><p>User</p></th></tr>'
		)
		for key, value in conflicts.iteritems():
			for item in value:
				row_string = '<tr><td>'
				row_string += '</td><td>'.join(
					[
						key,
						datetime.datetime.utcfromtimestamp(int(item['start']) / 1000).strftime("%Y-%m-%d"),
						datetime.datetime.utcfromtimestamp(int(item['end']) / 1000).strftime("%Y-%m-%d"),
						str(item['value']),
						datetime.datetime.utcfromtimestamp(int(item['submitted_at']) / 1000).strftime("%Y-%m-%d %H:%M:%S"),
						item['user']
					]
				)
				row_string += '</td></tr>'
				header_string += row_string
		return '<table>' + header_string + '<table>'

	@staticmethod
	def treatment_result_table(results):
		header_string = (
				'<tr><th><p>Treatment</p></th><th><p>Category</p></th><th><p>Tree</p></th>'
				+ '<th><p>Submitted</p></th><th><p>User</p></th></tr>'
		)
		for tree in results:
				row_string = '<tr><td>'
				if tree['tree_custom_id']:
					tree_name = tree['tree_uid'] + '<br>(' + tree['tree_custom_id'] + ')'
				else:
					tree_name = tree['tree_uid']
				row_string += '</td><td>'.join(
					[
						tree['treatment_name'],
						tree['treatment_category'],
						tree_name,
						datetime.datetime.utcfromtimestamp(int(tree['submitted_at']) / 1000).strftime("%Y-%m-%d %H:%M:%S"),
						tree['user']
					]
				)
				row_string += '</td></tr>'
				header_string += row_string
		return '<table>' + header_string + '<table>'

	def weather(
			self,
			field_uid,
			start,
			end,
			wind_speed_max,
			wind_direction,
			temperature_min,
			temperature_max,
			solar_radiation,
			rainfall,
			humidity
	):
		username = self.username
		with get_driver().session() as neo4j_session:
			# Make sure we have the condition submission node for this user
			if not [i for i in neo4j_session.read_transaction(
				neo4j_query,
				Cypher.match_condition_submission_node,
				{'username': username}
			)]:
				print('submissions node not found, merging')
				neo4j_session.write_transaction(
					neo4j_query,
					Cypher.merge_condition_submission_node,
					{'username': username}
				)
			# Store weather as FieldCondition - similar to trait but Start/End for data point rather than time-point.
			# Has value, start_time, end_time, person (user from form submission)
			# Conflicts:
				# If period overlapping for condition do not allow update
			conditions_dict = {
				'wind speed maximum': wind_speed_max,
				'wind direction': wind_direction,
				'temperature minimum': temperature_min,
				'temperature maximum': temperature_max,
				'solar radiation': solar_radiation,
				'rainfall': rainfall,
				'humidity': humidity
			}
			# make sure to check that start is less than end!!
			# first check to see if a value is already set in the selected period
			found_conflicts = self.find_condition_conflicts(start, end, field_uid, conditions_dict)
			if found_conflicts:
				html_table = self.condition_result_table(found_conflicts)
				return jsonify({
					'submitted': (
						' Record not submitted. <br><br> '
						' Conflicting values for some of the input data are found in this period: '
						+ html_table
					)
				})
			else:
				all_submitted = {}
				for condition in [i for i, j in conditions_dict.iteritems() if j]:
					parameters = {
						'username': username,
						'start': start,
						'end': end,
						'field_uid': field_uid,
						'condition': condition,
						'value': conditions_dict[condition]
					}
					result = neo4j_session.write_transaction(
						neo4j_query,
						Cypher.merge_field_condition,
						parameters
					)
					all_submitted[condition] = [record for record in result]
				html_table = self.condition_result_table(all_submitted)
				return jsonify({'submitted': (
					' Weather record submitted or updated: '
					+ html_table
					)
				})

	def treatment(
			self,
			field_uid,
			block_uid,
			trees_start,
			trees_end,
			treatment_name,
			treatment_category
	):
		username = self.username
		with get_driver().session() as neo4j_session:
			# Make sure we have the treatment submission node for this user
			if not [i for i in neo4j_session.read_transaction(
				neo4j_query,
				Cypher.match_treatment_submission_node,
				{'username': username}
			)]:
				print('treatment node not found, merging')
				neo4j_session.write_transaction(
					neo4j_query,
					Cypher.merge_treatment_submission_node,
					{'username': username}
				)
			parameters = {
				'username': self.username,
				'treatment_name': treatment_name,
				'treatment_category': treatment_category,
				'field_uid': field_uid,
				'block_uid': block_uid,
				'trees_start': trees_start,
				'trees_end': trees_end
				}
			treatment_conflicts = self.find_treatment_conflicts(parameters)
			if treatment_conflicts:
				html_table = self.treatment_result_table(treatment_conflicts)
				return jsonify({
					'submitted': (
						' Record not submitted. <br><br> '
						' Conflicting values for some of the input data are found in this period: '
						+ html_table
					)
				})
			else:
				if block_uid:
					result = neo4j_session.write_transaction(
						neo4j_query,
						Cypher.merge_block_tree_treatment,
						parameters
					)
				else:
					result = neo4j_session.write_transaction(
						neo4j_query,
						Cypher.merge_field_tree_treatment,
						parameters
					)
				result_list = [record for record in result]
				html_table = self.treatment_result_table(result_list)
				return jsonify({'submitted': (
					' Treatment record submitted or updated: '
					+ html_table
					)
				})


