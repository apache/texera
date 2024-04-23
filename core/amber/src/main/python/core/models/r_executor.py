import rpy2.robjects as robjects
import typing
from rpy2_arrow.arrow import rarrow_to_py_table, converter as arrowconverter
from rpy2.robjects import default_converter
from rpy2.robjects.conversion import localconverter
import pyarrow as pa

from core.models import ArrowTableTupleProvider, Tuple
from core.models.operator import SourceOperator, TableOperator


class RDplyrExecutor(TableOperator):
    is_source = False
    _arrow_to_dplyr = robjects.r(
        "arrow_to_dplyr <- function(table) { return (table %>% collect()) }"
    )
    _dplyr_to_arrow = robjects.r(
        """
        library(arrow)
        dplyr_to_arrow <- function(table) { return (arrow::arrow_table(table)) }
        """
    )

    def __init__(self, r_code: str):
        super().__init__()
        with localconverter(default_converter):
            self._func: typing.Callable[[pa.Table], pa.Table] = robjects.r(r_code)

    def process_table(self, table, port):
        input_pyarrow_table = pa.Table.from_pandas(table)
        with localconverter(arrowconverter):
            input_dplyr_table = RDplyrExecutor._arrow_to_dplyr(input_pyarrow_table)
            output_dplyr_table = self._func(input_dplyr_table)
            output_rarrow_table = RDplyrExecutor._dplyr_to_arrow(output_dplyr_table)
            output_pyarrow_table = rarrow_to_py_table(output_rarrow_table)

        for field_accessor in ArrowTableTupleProvider(output_pyarrow_table):
            yield Tuple(
                {name: field_accessor for name in output_pyarrow_table.column_names}
            )


class RSeuratOperator(TableOperator):
    is_source = False
    _seurat_to_arrow = robjects.r(
        """
        library(Seurat)
        library(arrow)
        
        seurat_to_arrow <- function(seuratObj) { 
            expression_data <- GetAssayData(seuratObj, slots = "counts")
            expression_df <- as.data.frame(as.matrix(expression_data))
            expression_df <- expression_df %>%
                tibble::rownames_to_column(var = "Feature")
            expression_df <- t(expression_df)
            return (arrow::arrow_table(expression_df))   
        }
        """
    )
    _arrow_to_seurat = robjects.r(
        """
        library(arrow)
        library(Seurat)
        arrow_to_seurat <- function(arrowTable) { 
            df <- as.data.frame(arrowTable)
            df <- t(df)
            expression_matrix <- as.matrix(df %>% select(-Feature))
            rownames(expression_matrix) <- df$Feature
            seurat <- CreateSeuratObject(counts = expression_matrix)
            return (seurat)
        }
        """
    )

    def __init__(self, r_code: str):
        super().__init__()
        with localconverter(default_converter):
            self._func = robjects.r(r_code)

    def process_table(self, table, port):
        input_pyarrow_table = pa.Table.from_pandas(table)
        with localconverter(arrowconverter):
            input_seurat_object = RSeuratOperator._arrow_to_seurat(input_pyarrow_table)
            output_seurat_object = self._func(input_seurat_object)
            output_rarrow_table = RSeuratOperator._seurat_to_arrow(output_seurat_object)
            output_pyarrow_table = rarrow_to_py_table(output_rarrow_table)

        for field_accessor in ArrowTableTupleProvider(output_pyarrow_table):
            yield Tuple(
                {name: field_accessor for name in output_pyarrow_table.column_names}
            )


            # Could have an operator that takes in a table and outputs a Seurat object
                # Source operator outputting a Seurat object
                    # Source operator will read a file and output a Seurat object

class RSeuratSourceExecutor(SourceOperator):
    is_source = True
    _seurat_to_arrow = robjects.r(
        """
        library(Seurat)
        library(arrow)
        library(tidyseurat)
        
        seurat_to_arrow <- function(seuratObj) { 
            expression_data <- GetAssayData(seuratObj, slots = "counts")
            print("1")
            expression_df <- as.data.frame(as.matrix(expression_data))
            print("2")
            expression_df <- expression_df %>%
                tibble::rownames_to_column(var = "Feature")
            print("3")
            expression_df <- t(expression_df)
            print("4")
            return (arrow::arrow_table(expression_df))   
        }
        """
    )

    def __init__(self, r_code: str):
        super().__init__()
        with localconverter(default_converter):
            self._func = robjects.r(r_code)

    def produce(self):

        with localconverter(arrowconverter):

            output_seurat_obj = self._func()
            output_rarrow_table = RSeuratSourceExecutor._seurat_to_arrow(output_seurat_obj)
            output_pyarrow_table = rarrow_to_py_table(output_rarrow_table)

        for field_accessor in ArrowTableTupleProvider(output_pyarrow_table):
            yield Tuple(
                {name: field_accessor for name in output_pyarrow_table.column_names}
            )
