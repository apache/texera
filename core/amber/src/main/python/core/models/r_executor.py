import rpy2.robjects as robjects
import typing
from rpy2_arrow.arrow import (rarrow_to_py_table, converter as arrowconverter)
from rpy2.robjects.conversion import localconverter
import pyarrow as pa

from core.models import ArrowTableTupleProvider, Tuple
from core.models.operator import TableOperator


class RDplyrExecutor(TableOperator):
    is_source = False
    _arrow_to_dplyr = robjects.r("""
                    arrow_to_dplyr <- function(table) {
                        return (table %>% collect()) 
                    }  
                    """)
    _dplyr_to_arrow = robjects.r("""
                    library(arrow)
                    dplyr_to_arrow <- function(table) {
                        return (arrow::arrow_table(table)) 
                    }  
                    """)

    def __init__(self, r_code: str):
        super().__init__()
        self._func: typing.Callable[[pa.Table], pa.Table] = robjects.r(r_code)

    def process_table(self, table, port):
        py_arrow_table = pa.Table.from_pandas(table)
        with localconverter(arrowconverter):
            result_table = rarrow_to_py_table(RDplyrExecutor._dplyr_to_arrow(
                self._func(RDplyrExecutor._arrow_to_dplyr(py_arrow_table)
                           )))

        for field_accessor in ArrowTableTupleProvider(result_table):
            yield Tuple(
                {name: field_accessor for name in result_table.column_names}
            )
