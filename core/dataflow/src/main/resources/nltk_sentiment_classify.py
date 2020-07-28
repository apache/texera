import sys
import pickle
import pyarrow
import pandas
import ast
import threading
import pyarrow.flight

portNumber = sys.argv[1]
pickleFullPathFileName = sys.argv[2]
inputAttributeName = sys.argv[3]
resultAttributeName = sys.argv[4]


class FlightServer(pyarrow.flight.FlightServerBase):
	def __init__(self, host="localhost", location=None, tls_certificates=None, auth_handler=None):
		super(FlightServer, self).__init__(
			location, auth_handler, tls_certificates)
		self.flights = {}
		self.host = host
		self.tls_certificates = tls_certificates

	@classmethod
	def descriptor_to_key(self, descriptor):
		return (descriptor.descriptor_type.value, descriptor.command,
				tuple(descriptor.path or tuple()))

	def _make_flight_info(self, key, descriptor, table):
		if self.tls_certificates:
			location = pyarrow.flight.Location.for_grpc_tls(
				self.host, self.port)
		else:
			location = pyarrow.flight.Location.for_grpc_tcp(
				self.host, self.port)
		endpoints = [pyarrow.flight.FlightEndpoint(repr(key), [location]), ]

		mock_sink = pyarrow.MockOutputStream()
		stream_writer = pyarrow.RecordBatchStreamWriter(
			mock_sink, table.schema)
		stream_writer.write_table(table)
		stream_writer.close()
		data_size = mock_sink.size()

		return pyarrow.flight.FlightInfo(table.schema,
										 descriptor, endpoints,
										 table.num_rows, data_size)

	def list_flights(self, context, criteria):
		"""
		Getting a list of available datasets on the server. This method is not used here,
		but might be useful in the future.
		"""
		for key, table in self.flights.items():
			if key[1] is not None:
				descriptor = \
					pyarrow.flight.FlightDescriptor.for_command(key[1])
			else:
				descriptor = pyarrow.flight.FlightDescriptor.for_path(*key[2])

			yield self._make_flight_info(key, descriptor, table)

	def get_flight_info(self, context, descriptor):
		"""
		Returning an “access plan” for a dataset of interest, possibly requiring consuming multiple data streams.
		This request can accept custom serialized commands containing, for example, your specific
		application parameters.
		"""
		key = FlightServer.descriptor_to_key(descriptor)
		if key in self.flights:
			table = self.flights[key]
			return self._make_flight_info(key, descriptor, table)
		raise KeyError('Flight not found.')

	def do_put(self, context, descriptor, reader, writer):
		"""
		Pass Arrow stream from the client to the server. The data must be associated with a `FlightDescriptor`,
		which can be either a path or a command. Here the path is not actually a path on the disk,
		but rather an identifier.
		"""
		key = FlightServer.descriptor_to_key(descriptor)
		self.flights[key] = reader.read_all()

	def do_get(self, context, ticket):
		"""
		Before getting the stream, the client must first ask the server for available tickets
		(to the specified dataset) of the specified `FlightDescriptor`.
		"""
		key = ast.literal_eval(ticket.ticket.decode())
		if key not in self.flights:
			print("Flight Server:\tNOT IN")
			return None
		return pyarrow.flight.RecordBatchStream(self.flights[key])

	def do_action(self, context, action):
		"""
		Each (implementation-specific) action is a string (defined in the script). The client is expected to know
		available actions. When a specific action is called, the server executes the corresponding action and
		maybe will return any results, i.e. a generalized function call.
		"""
		if action.type == "compute":
			# to execute the computation of sentiments.
			input_descriptor = pyarrow.flight.FlightDescriptor.for_path(b'ToPython')
			# print("Flight Server:\tComputing sentiment...")
			key = FlightServer.descriptor_to_key(input_descriptor)
			pickle_file = open(pickleFullPathFileName,'rb')
			# print("Flight Server:\t\tReading model file...", end =" ")
			sentiment_model = pickle.load(pickle_file)
			# print("Done.")
			# print("Flight Server:\t\tConverting Arrow data to pandas.Dataframe...", end =" ")
			input_dataframe = pandas.DataFrame(self.flights[key].column(inputAttributeName).to_pandas())
			# print("Done.")
			# print("Flight Server:\t\tExecuting computation...", end=" ")
			predictions = []
			for index, row in input_dataframe.iterrows():
				p = 1 if sentiment_model.classify(row[inputAttributeName]) == "pos" else -1
				predictions.append(p)
			pickle_file.close()
			# print("Done.")
			# print("Flight Server:\t\tConverting back to Arrow data...", end =" ")
			output_descriptor = pyarrow.flight.FlightDescriptor.for_path(b'FromPython')
			output_data = self.flights[key]
			predictions = pyarrow.array(predictions)
			output_data = output_data.append_column(resultAttributeName, predictions)
			self.flights[FlightServer.descriptor_to_key(output_descriptor)] = output_data
			# print("Done.")
			# print("Flight Server:\tDone.")
			self.flights.pop(key)
			yield pyarrow.flight.Result(pyarrow.py_buffer(b'Success!'))
		elif action.type == "healthcheck":
			# to check the status of the server to see if it is running.
			yield pyarrow.flight.Result(pyarrow.py_buffer(b'Flight Server is up and running!'))
		elif action.type == "shutdown":
			# to shutdown the server.
			yield pyarrow.flight.Result(pyarrow.py_buffer(b'Flight Server is shut down!'))
			# Shut down on background thread to avoid blocking current
			# request
			threading.Thread(target=self._shutdown).start()
		else:
			raise KeyError("Unknown action {!r}".format(action.type))

	def _shutdown(self):
		"""Shut down after a delay."""
		# print("Flight Server:\tServer is shutting down...")

		self.shutdown()
		self.wait()


def main():
	location = "grpc+tcp://localhost:"+portNumber
	server = FlightServer("localhost", location)
	# print("Flight Server:\tServing on", location)
	server.serve()


if __name__ == '__main__':
	main()
