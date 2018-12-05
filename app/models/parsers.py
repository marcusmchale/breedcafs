from itertools import chain


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
