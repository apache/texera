import pickle
import datetime
import pyarrow as pa
import rpy2.robjects as robjects
from rpy2_arrow.arrow import converter as arrow_converter
from rpy2.robjects import default_converter
from rpy2.robjects.conversion import localconverter as local_converter
from typing import Iterator, Optional, Union
from core.models import Tuple, TupleLike, TableLike
from core.models.operator import SourceOperator, TupleOperatorV2


class RTupleExecutor(TupleOperatorV2):
    """
    An operator that serializes/deserializes objects using R code.
    """

    is_source = False

    # _pyarrow_array_to_r_list = robjects.r(
    #     """
    #     function(pyarrow_StructArray) {
    #         StructArray_as_df <- pyarrow_StructArray$as_vector()
    #         StructArray_as_list <- as.list(StructArray_as_df)
    #         return (StructArray_as_list)
    #     }
    #     """
    # )

    def _convert_r_to_py(value):
        if isinstance(value, robjects.vectors.BoolVector):
            return bool(value[0])
        if isinstance(value, robjects.vectors.IntVector):
            return int(value[0])
        if isinstance(value, robjects.vectors.FloatVector):
            if isinstance(value, robjects.vectors.POSIXct):
                return next(value.iter_localized_datetime())
            else:
                return float(value[0])
        if isinstance(value, robjects.vectors.ComplexVector):
            return complex(value[0])
        if isinstance(value, robjects.vectors.StrVector):
            return str(value[0])
        return value

    # def _tuple_to_r_input_METHOD1(input_tuple):
    #     input_schema = input_tuple._schema.as_arrow_schema()
    #     has_binary = False
    #     for field in input_schema:
    #         if field.type == pa.binary():
    #             has_binary = True
    #             break
    #
    #     input_dict = input_tuple.as_dict()
    #     if has_binary:
    #         input_r_list = {}
    #         for k, v in input_dict.items():
    #             if isinstance(v, bytes):
    #                 input_r_list[k] = pickle.loads(
    #                     v[10:]
    #                 )
    #             elif isinstance(v, datetime.datetime):
    #                 input_r_list[k] = robjects.vectors.POSIXct.sexp_from_datetime(
    #                     [v]
    #                 )
    #             else:
    #                 input_r_list[k] = v
    #         input_r_list = robjects.vectors.ListVector(input_r_list)
    #     else:
    #         input_pyarrow_array = pa.array(
    #             [input_dict], type=pa.struct(input_schema)
    #         )
    #         input_r_list = RTupleExecutor._pyarrow_array_to_r_list(
    #             input_pyarrow_array
    #         )
    #     return input_r_list

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
        # One way:
        # can detect if the tuple has binary in its schema or not
        # if no binary, use rpy2-arrow to do conversion
        # if binary, use rpy2 ListVector to convert dictionary

        # Second way:
        # detect if tuple has binary
        # for every column that is NOT binary, can create
        # subtuple/partial tuple
        # ex: a,b,c and c is binary, can create a subtuple of (a,b)
        # and convert (a,b) with rpy2-arrow
        # c will be converted via regular rpy2, then add c to (a,b) in the R side
        # see tuple.py get_partial_tuple()

        with local_converter(arrow_converter):
            # TODO: use for method 2

            # TODO: preserve column order after doing conversion
            input_schema = tuple_._schema.as_arrow_schema()
            nonbinary_fields = [
                field.name for field in input_schema if field.type != pa.binary()
            ]
            binary_fields = [
                field.name for field in input_schema if field.type == pa.binary()
            ]
            # for field in input_schema:
            #     if field.type != pa.binary():
            #         nonbinary_fields.append(field.name)
            #     else:
            #         binary_fields.append(field.name)
            partial_tuple = tuple_.get_partial_tuple(nonbinary_fields)
            partial_tuple_schema = partial_tuple._schema.as_arrow_schema()
            nonbinary_pyarrow_array = pa.array(
                [partial_tuple.as_dict()], type=pa.struct(partial_tuple_schema)
            )

            binary_tuple = tuple_.get_partial_tuple(binary_fields)
            binary_r_list = {}
            for k, v in binary_tuple.as_dict().items():
                if isinstance(v, bytes):
                    binary_r_list[k] = pickle.loads(v[10:])
                elif isinstance(v, datetime.datetime):
                    binary_r_list[k] = robjects.vectors.POSIXct.sexp_from_datetime([v])
                else:
                    binary_r_list[k] = v
            binary_r_list = robjects.vectors.ListVector(binary_r_list)
            combine_lists_rcode = robjects.r(
                """
                function(nonbinary_list, binary_list) {
                    nonbinary_list <- as.list(nonbinary_list$as_vector())
                    return (c(nonbinary_list, binary_list)) # returns R list
                    # make sure to preserve order
                }
            """
            )
            input_r_list = combine_lists_rcode(nonbinary_pyarrow_array, binary_r_list)
            # TODO: preserve column order after doing conversion
            # TODO **********************
            # TODO: use for method 1 ->
            #  input_r_list = RTupleExecutor._tuple_to_r_input_METHOD1(tuple_)
            output_r_list = self._func(input_r_list, port)
            # output = next(output_r_list)
            # Convert R List to Python Dictionary (mapped to rpy2 types)
            output_python_dict = {
                key: output_r_list.rx2(key) for key in output_r_list.names
            }
            # Convert Python Dictionary's values (change values to base Python types)
            output_python_dict = {
                key: RTupleExecutor._convert_r_to_py(value)
                for key, value in output_python_dict.items()
            }
            # TODO: implement generator (multiple tuples) and test performance
            # TODO: try using generator yield, if doesn't work use List of lists
            # TODO: test performance of both methods with multiple tuples
            # user can return 0, 1, or more than 1 tuple
            # should support return of NULL/None (no tuples)
            # Ideas:
            # Use lists of lists
            # 0 tuple = empty list, 1 tuple = 1 element, more than 1 tuples =
            # more than 1 element
            # assume output is a list of lists, like a generator
            # then for loop over it and do conversion and yield each Tuple
            yield Tuple(output_python_dict)


class RTupleSourceExecutor(SourceOperator):
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
            if isinstance(value, robjects.vectors.POSIXct):
                return next(value.iter_localized_datetime())
            else:
                return float(value[0])
        if isinstance(value, robjects.vectors.ComplexVector):
            return complex(value[0])
        if isinstance(value, robjects.vectors.StrVector):
            return str(value[0])
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
        Produce Tuples or Tables using the provided R function.
        Used by the source operator only.

        :return: Iterator[Union[TupleLike, TableLike, None]], producing
            one TupleLike object, one TableLike object, or None, at a time.
        """
        with local_converter(arrow_converter):
            # R List
            output_r_list = self._func()
            # Convert R List to Python Dictionary (mapped to rpy2 types)
            output_python_dict = {
                key: output_r_list.rx2(key) for key in output_r_list.names
            }
            # Convert Python Dictionary's values (change values to base Python types)
            output_python_dict = {
                key: RTupleSourceExecutor._convert_r_to_py(value)
                for key, value in output_python_dict.items()
            }

        yield Tuple(output_python_dict)
