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


class RSerializeExecutor(TupleOperatorV2):
    """
    An operator that serializes/deserializes objects using R code.
    """

    is_source = False

    _object_to_arrow = robjects.r(
        """
        library(arrow)
        object_to_arrow <- function(object) { 
            serialized <- serialize(object, connection = NULL)
            df <- data.frame(object = I(list(serialized)))
            return (arrow::arrow_table(df))   
        }
        """
    )
    _arrow_to_object = robjects.r(
        """
        # library(arrow)
        # arrow_to_object <- function(arrowTable) { 
        #     unserialized <- unserialize(unlist((as.data.frame(arrowTable))$object))
        #     return (unserialized)
        # }
        function(bytes) {
            return (unserialize(bytes))
        }
        """
    )

    _output_to_arrow_array = robjects.r(
        """
        library(arrow)
        function(udfOutput) {
            return (arrow::arrow_array(udfOutput))
        }
        """
    )

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

    # def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
    #     """
    #     Process an input Table using the provided R function. The Table is represented as a
    #     pandas.DataFrame.
    #
    #     :param table: Table, a table to be processed.
    #     :param port: int, input port index of the current Tuple. Currently unused in R-UDF
    #     :return: Iterator[Optional[TableLike]], producing one TableLike object at a
    #     time, or None.
    #     """
    #     input_pyarrow_table = pa.Table.from_pandas(table)
    #     with local_converter(arrow_converter):
    #         input_object = RSerializeExecutor._arrow_to_object(input_pyarrow_table)
    #         output_object = self._func(input_object, port)
    #         output_rarrow_table = RSerializeExecutor._object_to_arrow(output_object)
    #         output_pyarrow_table = rarrow_to_py_table(output_rarrow_table)
    #
    #     for field_accessor in ArrowTableTupleProvider(output_pyarrow_table):
    #         yield Tuple(
    #             {name: field_accessor for name in output_pyarrow_table.column_names}
    #         )

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

        # TODO: CONVERT input_pyarrow_array to an R list/R Arrow Array?
            # find a data struct that is easy to use set, get, and append methods
            # use that for tuple representation from Python to R side
        # TODO: yield out the output from R as a Python tuple
        # TODO: Write the code to output the tuple from whatever is returned from R
            # Don't have to worry about serialization
        with local_converter(arrow_converter):
            # print(tuple_["object"])
            # print(type(tuple_["object"]))
            input_object = RSerializeExecutor._arrow_to_object(ByteSexpVector(bytearray(tuple_["object"])))
            output_object = self._func(input_object, port)


            # tuple_["line"] = robjects.r("""
            # lm(wt~mpg, mtcars)
            # """)
            # print(input_pyarrow_array)
            # print(type(input_pyarrow_array))
            # r_code = """
            # function(input) {
            #     print(input)
            #     print(class(input))
            # }
            # """
            # func = robjects.r(r_code)
            # func(input_pyarrow_array)
        yield tuple_

class RSerializeSourceExecutor(SourceOperator):
    """
    A source operator that serializes objects using R code.
    """

    is_source = True

    _object_to_arrow = robjects.r(
        """
        library(arrow)
        object_to_arrow <- function(object) { 
            serialized <- serialize(object, connection = NULL)
            df <- data.frame(object = I(list(serialized)))
            return (arrow::arrow_table(df))   
        }
        """
    )

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
            output_obj = self._func()
            output_rarrow_table = RSerializeSourceExecutor._object_to_arrow(output_obj)
            output_pyarrow_table = rarrow_to_py_table(output_rarrow_table)

        for field_accessor in ArrowTableTupleProvider(output_pyarrow_table):
            yield Tuple(
                {name: field_accessor for name in output_pyarrow_table.column_names}
            )