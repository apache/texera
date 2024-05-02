import pyarrow as pa
import rpy2.robjects as robjects
from rpy2_arrow.arrow import rarrow_to_py_table, converter as arrow_converter
from rpy2.robjects import default_converter
from rpy2.robjects.conversion import localconverter as local_converter
from core.models import ArrowTableTupleProvider, Tuple
from core.models.operator import SourceOperator, TableOperator

class RSerializeExecutor(TableOperator):
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
        library(arrow)
        arrow_to_object <- function(arrowTable) { 
            unserialized <- unserialize(unlist((as.data.frame(arrowTable))$object))
            return (unserialized)
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
        with local_converter(default_converter):
            self._func = robjects.r(r_code)

    def process_table(self, table, port):
        """
        Process the input table using the provided R function.

        Args:
            table (pandas.DataFrame): Input table.
            port: Port identifier.

        Yields:
            Tuple: Processed tuples.
        """
        input_pyarrow_table = pa.Table.from_pandas(table)
        with local_converter(arrow_converter):
            input_object = RSerializeExecutor._arrow_to_object(input_pyarrow_table)
            output_object = self._func(input_object)
            output_rarrow_table = RSerializeExecutor._object_to_arrow(output_object)
            output_pyarrow_table = rarrow_to_py_table(output_rarrow_table)

        for field_accessor in ArrowTableTupleProvider(output_pyarrow_table):
            yield Tuple(
                {name: field_accessor for name in output_pyarrow_table.column_names}
            )


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
        with local_converter(default_converter):
            self._func = robjects.r(r_code)

    def produce(self):
        """
        Produce data using the provided R function.

        Yields:
            Tuple: Produced tuples.
        """
        with local_converter(arrow_converter):
            output_obj = self._func()
            output_rarrow_table = RSerializeSourceExecutor._object_to_arrow(output_obj)
            output_pyarrow_table = rarrow_to_py_table(output_rarrow_table)

        for field_accessor in ArrowTableTupleProvider(output_pyarrow_table):
            yield Tuple(
                {name: field_accessor for name in output_pyarrow_table.column_names}
            )