from itertools import chain
import datetime


class Parsers:
	def __init__(self):
		pass

	@staticmethod
	def parse_range(
			range_string
	):
		parts = range_string.split('-')
		if 1 > len(parts) > 2:
			raise ValueError("Bad range: '%s'" % (range_string,))
		parts = [int(i) for i in parts]
		start = parts[0]
		end = start if len(parts) == 1 else parts[1]
		if start > end:
			end, start = start, end
		return range(start, end + 1)

	@staticmethod
	def parse_range_list(
			range_list
	):
		return sorted(set(chain(*[Parsers.parse_range(range_string) for range_string in range_list.split(',')])))

	@staticmethod
	def timestamp_fb_format(timestamp_string):
		timestamp = str(timestamp_string).strip()
		try:
			datetime.datetime.strptime(timestamp[0:19], '%Y-%m-%d %H:%M:%S')
			if not all([
				timestamp[-5] in ['+', '-'],
				int(timestamp[-4:-2]) < 24,
				int(timestamp[-4:-2]) >= 0,
				int(timestamp[-2:]) < 60,
				int(timestamp[-2:]) >= 0
			]):
				return False
			else:
				return timestamp
		except (ValueError, IndexError):
			return False

	@staticmethod
	def date_format(date_string):
		date_string = str(date_string).strip()
		if not date_string:
			return True
		else:
			try:
				time = datetime.datetime.strptime(date_string, '%Y-%m-%d')
				# the below is just to make sure can render it again, strftime fails on dates pre-1990
				datetime.datetime.strftime(time, '%Y')
				return date_string
			except ValueError:
				return False

	@staticmethod
	def time_format(time_string):
		time_string = str(time_string).strip()
		if not time_string:
			return True
		else:
			try:
				datetime.datetime.strptime(time_string, '%H:%M')
				return time_string
			except (ValueError, IndexError):
				return False

	@staticmethod
	def uid_format(uid):
		uid = str(uid).strip().upper()
		if uid.isdigit():
			return True
		else:
			if len(uid.split("_")) == 2:
				if all([
					uid.split("_")[0].isdigit(),
					uid.split("_")[1][0] in ["B", "T", "R", "L", "S"],
					uid.split("_")[1][1:].isdigit()
				]):
					return uid
				else:
					if all([
						uid.split("_")[0].isdigit(),
						uid.split("_")[1][0] == "S",
						uid.split("_")[1].split(".")[0][1:].isdigit(),
						uid.split("_")[1].split(".")[1].isdigit()
					]):
						return uid
					else:
						return False
			else:
				return False