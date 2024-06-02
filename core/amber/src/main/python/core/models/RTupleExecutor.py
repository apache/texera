import pickle

import pyarrow as pa
import rpy2.robjects as robjects
from rpy2_arrow.arrow import (rarrow_to_py_array, pyarrow_to_r_array,
                              rarrow_to_py_table, converter as arrow_converter)
from rpy2.robjects import default_converter
from rpy2.robjects.conversion import localconverter as local_converter
from rpy2.rinterface import ByteSexpVector
from typing import Iterator, List, Mapping, Optional, Union, MutableMapping
from core.models import ArrowTableTupleProvider, Tuple, TupleLike, Table, TableLike
from core.models.operator import SourceOperator, TupleOperatorV2
import traceback

class RTupleExecutor(TupleOperatorV2):
    """
    An operator that serializes/deserializes objects using R code.
    """

    is_source = False

    def _convert_r_to_py(value):
        if isinstance(value, robjects.vectors.BoolVector):
            return bool(value[0])
        if isinstance(value, robjects.vectors.IntVector):
            return int(value[0])
        if isinstance(value, robjects.vectors.FloatVector):
            return float(value[0])
        if isinstance(value, robjects.vectors.ComplexVector):
            return complex(value[0])
        if isinstance(value, robjects.vectors.StrVector):
            return str(value[0])
        if isinstance(value, robjects.vectors.ByteSexpVector):
            return bytes(value)
        if isinstance(value, robjects.vectors.ListVector):
            try:
                if isinstance(value[0], robjects.vectors.ByteSexpVector):
                    # print("VALUE", value)
                    # print("VALUE TYPE", type(value))
                    # print("VALUE[0]", value[0])
                    # print("VALUE[0] TYPE", type(value[0]))
                    # return pickle.loads(value[0])
                    return bytes(value[0])
            except (IndexError, ValueError) as e:
                print(traceback.format_exc())
                raise e
            return {key: _convert_r_to_py(value.rx2(key)) for key in value.names}
        return value

    _pyarrow_array_to_r_list = robjects.r("""
        function(pyarrow_StructArray) {
            StructArray_as_df <- pyarrow_StructArray$as_vector()
            StructArray_as_list <- as.list(StructArray_as_df)
            return (StructArray_as_list)
        }
        """)

    def __init__(self, r_code: str):
        """
        Initialize the RSerializeExecutor with R code.

        Args:
            r_code (str): R code to be executed.
        """
        super().__init__()
        # Use the local converter from rpy2 to load in the R function given by the user
        with local_converter(default_converter):
            self._func = robjects.r(r_code)

    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        """
        Process an input Tuple from the given link.

        :param tuple_: Tuple, a Tuple from an input port to be processed.
        :param port: int, input port index of the current Tuple.
        :return: Iterator[Optional[TupleLike]], producing one TupleLike object at a
            time, or None.
        """
        input_dict = tuple_.as_dict()
        input_schema = tuple_._schema.as_arrow_schema()
        input_pyarrow_array = pa.array([input_dict], type=pa.struct(input_schema))
        # TODO: Write the code to output the tuple from whatever is returned from R
            # Don't have to worry about serialization
        # TODO: deal with converting raw bytes and complex R type to StructArray in R?
        # TODO: Very close 6:47 PM June 1, 2024
            # TODO 1) Get the input python tuple DONE
            # TODO 2) Convert the input python dict into an R list
            # TODO 3) When converting python dict -> R list, loop over the list and unserialize if any columns are binary
                # TODO 3a) POTENTIAL ISSUE, Python's pickling/serialization may not matchup correct with R's serialization
                # TODO 3b) POTENTIAL ISSUE, How do you store an object in an R List?
        with local_converter(arrow_converter):
            # Converted the input_pyarrow_array to an R array
            input_r_list = RTupleExecutor._pyarrow_array_to_r_list(input_pyarrow_array)
            output_r_list = self._func(input_r_list, port)
            # Convert R List to Python Dictionary (mapped to rpy2 types)
            output_python_dict = {key: output_r_list.rx2(key) for key in output_r_list.names}
            # Convert Python Dictionary's values (change values to base Python types)
            output_python_dict = {key: RTupleExecutor._convert_r_to_py(value)
                                  for key, value in output_python_dict.items()}

        yield Tuple(output_python_dict)

class RSourceTupleExecutor(SourceOperator):
    """
    A source operator that serializes objects using R code.
    """

    is_source = True

    def _convert_r_to_py(value):
        if isinstance(value, robjects.vectors.BoolVector):
            return bool(value[0])
        if isinstance(value, robjects.vectors.IntVector):
            return int(value[0])
        if isinstance(value, robjects.vectors.FloatVector):
            return float(value[0])
        if isinstance(value, robjects.vectors.ComplexVector):
            return complex(value[0])
        if isinstance(value, robjects.vectors.StrVector):
            return str(value[0])
        if isinstance(value, robjects.vectors.ByteSexpVector):
            return bytes(value)
        if isinstance(value, robjects.vectors.ListVector):
            return {key: _convert_r_to_py(value.rx2(key)) for key in value.names}
        return value

    def __init__(self, r_code: str):
        """
        Initialize the RSerializeSourceExecutor with R code.

        Args:
            r_code (str): R code to be executed.
        """
        super().__init__()
        # Use the local converter from rpy2 to load in the R function given by the user
        with local_converter(default_converter):
            self._func = robjects.r(r_code)

    def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:
        """
        Produce Tuples or Tables using the provided R function. Used by the source operator only.

        :return: Iterator[Union[TupleLike, TableLike, None]], producing
            one TupleLike object, one TableLike object, or None, at a time.
        """
        with local_converter(arrow_converter):
            # R List
            output_r_list = self._func()
            # Convert R List to Python Dictionary (mapped to rpy2 types)
            output_python_dict = {key: output_r_list.rx2(key) for key in output_r_list.names}
            # Convert Python Dictionary's values (change values to base Python types)
            output_python_dict = {key: RSourceTupleExecutor._convert_r_to_py(value)
                                  for key, value in output_python_dict.items()} # HERE I HAVE THE PYTHON BYTES ALREADY
            print(output_python_dict)

        yield Tuple(output_python_dict)