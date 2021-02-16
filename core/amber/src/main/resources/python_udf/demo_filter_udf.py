def filter_function(row, *args):
	"""
	This is a demo filter UDF. Filters tuples whose value of input contains the keyword "virus".
	This function is automatically recognized as the input of a `TexeraFilterOperator`.
	:param row: Input tuple.
	:param args: Argument(s), including the name that specify the input field.
	:return: Whether the input tuple satisfies the filter condition.
	"""
	return row[args[0]].find("virus") != -1
