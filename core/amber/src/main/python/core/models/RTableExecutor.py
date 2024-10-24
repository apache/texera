import pyarrow as pa
import rpy2.robjects as robjects
from rpy2_arrow.arrow import rarrow_to_py_table, converter as arrow_converter
from rpy2.robjects import default_converter
from rpy2.robjects.conversion import localconverter as local_converter
import typing
from typing import Iterator, Optional, Union
from core.models import ArrowTableTupleProvider, Tuple, TupleLike, Table, TableLike
from core.models.operator import SourceOperator, TableOperator


class RTableExecutor(TableOperator):
    """
    An executor that can execute R code on Arrow tables.
    """

    is_source = False

    _arrow_to_r_dataframe = robjects.r(
        "function(table) { return (as.data.frame(table)) }"
    )

    _r_dataframe_to_arrow = robjects.r(
        """
        library(arrow)
        function(df) { return (arrow::as_arrow_table(df)) }
        """
    )

    def __init__(self, r_code: str):
        """
        Initialize the RTableExecutor with R code.

        Args:
            r_code (str): R code to be executed.
        """
        super().__init__()
        with local_converter(default_converter):
            self._func: typing.Callable[[pa.Table], pa.Table] = robjects.r(r_code)

    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        """
        Process an input Table using the provided R function.
        The Table is represented as a pandas.DataFrame.

        :param table: Table, a table to be processed.
        :param port: int, input port index of the current Tuple.
            Currently unused in R-UDF
        :return: Iterator[Optional[TableLike]], producing one TableLike object at a
        time, or None.
        """
        input_pyarrow_table = pa.Table.from_pandas(table)
        with local_converter(arrow_converter):
            input_r_dataframe = RTableExecutor._arrow_to_r_dataframe(
                input_pyarrow_table
            )
            output_r_dataframe = self._func(input_r_dataframe, port)
            output_rarrow_table = RTableExecutor._r_dataframe_to_arrow(
                output_r_dataframe
            )
            output_pyarrow_table = rarrow_to_py_table(output_rarrow_table)

        for field_accessor in ArrowTableTupleProvider(output_pyarrow_table):
            yield Tuple(
                {name: field_accessor for name in output_pyarrow_table.column_names}
            )


class RTableSourceExecutor(SourceOperator):
    """
    A source operator that produces an R Table or Table-like object using R code.
    """

    is_source = True
    _source_output_to_arrow = robjects.r(
        """
    library(arrow)
    function(source_output) {
        return (arrow::as_arrow_table(as.data.frame(source_output)))
    }
    """
    )

    def __init__(self, r_code: str):
        """
        Initialize the RTableSourceExecutor with R code.

        Args:
            r_code (str): R code to be executed.
        """
        super().__init__()
        # Use the local converter from rpy2 to load in the R function given by the user
        with local_converter(default_converter):
            self._func = robjects.r(r_code)

    def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:
        """
        Produce Table using the provided R function.
        Used by the source operator only.

        :return: Iterator[Union[TupleLike, TableLike, None]], producing
            one TupleLike object, one TableLike object, or None, at a time.
        """
        with local_converter(arrow_converter):
            output_table = self._func()
            output_rarrow_table = RTableSourceExecutor._source_output_to_arrow(
                output_table
            )
            output_pyarrow_table = rarrow_to_py_table(output_rarrow_table)

        for field_accessor in ArrowTableTupleProvider(output_pyarrow_table):
            yield Tuple(
                {name: field_accessor for name in output_pyarrow_table.column_names}
            )
