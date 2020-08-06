import pandas


class TexeraUDFOperator(object):
	"""
	Base class for row-oriented one-table input, one-table output user-defined operators. This must be implemented
	before using.
	"""
	def __init__(self):
		self._args = None

	def open(self, args: list):
		"""
		Specify here what the UDF should do before executing on tuples. For example, you may want to open a model file
		before using the model for prediction.

			:param args: a list of possible arguments that might be used. This is specified in Texera's UDFOperator's
				configuration panel. The order of these arguments is input attributes, output attributes, outer file
				 paths. Whoever uses these arguments are supposed to know the order.
		"""
		self._args = args

	def accept(self, row: pandas.Series, nth_child: int = 0):
		"""
		This is what the UDF operator should do for every row. Do not return anything here, just accept it. The result
		should be retrieved with next().

			:param row: The input row to accept and do custom execution.
			:param nth_child: In the future might be useful.
		"""
		raise NotImplementedError

	def has_next(self):
		"""
		Return a boolean value that indicates whether there will be a next result.
		"""
		raise NotImplementedError

	def next(self):
		"""
		Get the next result row. This will be called after accept(), so result should be prepared.
		"""
		raise NotImplementedError

	def close(self):
		"""
		Close this operator, releasing any resources. For example, you might want to close a model file.
		"""
		raise NotImplementedError


class TexeraMapOperator(TexeraUDFOperator):
	"""
	Base class for one-input-tuple to one-output-tuple mapping operator. Either inherit this class (in case you want to
	override open() and close(), e.g., open and close a model file.) or init this class object with a map function.
	The map function should return the result tuple. If use inherit, then script should have an attribute named
	`operator_instance` that is an instance of the inherited class; If only use filter function, simply define a
	`map_function` in the script.
	"""

	def __init__(self, map_function=None):
		super().__init__()
		self._map_function = map_function
		self._result_tuples = []

	def accept(self, row: pandas.Series, nth_child=0):
		if self._map_function is not None:
			self._result_tuples.append(self._map_function(row, self._args))  # must take args
		else:
			raise NotImplementedError

	def has_next(self):
		return len(self._result_tuples) != 0

	def next(self):
		return self._result_tuples.pop()

	def close(self):
		pass


class TexeraFilterOperator(TexeraUDFOperator):
	"""
		Base class for filter operators. Either inherit this class (in case you want to
		override open() and close(), e.g., open and close a model file.) or init this class object with a filter function.
		The filter function should return a boolean value indicating whether the input tuple meets the filter criteria.
		If use inherit, then script should have an attribute named `operator_instance` that is an instance of the
		inherited class; If only use filter function, simply define a `filter_function` in the script.
		"""

	def __init__(self, filter_function=None):
		super().__init__()
		self._filter_function = filter_function
		self._result_tuples = []

	def accept(self, row: pandas.Series, nth_child=0):
		if self._filter_function is not None:
			if self._filter_function(row, self._args):
				self._result_tuples.append(row)
		else:
			raise NotImplementedError

	def has_next(self):
		return len(self._result_tuples) != 0

	def next(self):
		return self._result_tuples.pop()

	def close(self):
		pass
