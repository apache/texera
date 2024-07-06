import pickle
import datetime
import pyarrow as pa
import rpy2
import rpy2.robjects as robjects
from rpy2_arrow.arrow import converter as arrow_converter
from rpy2.robjects import default_converter
from rpy2.robjects.conversion import localconverter as local_converter
from typing import Iterator, Optional, Union
from core.models import Tuple, TupleLike, TableLike, r_utils
from core.models.operator import SourceOperator, TupleOperatorV2
import warnings

warnings.filterwarnings(action="ignore", category=UserWarning, module=r"rpy2*")


class RTupleExecutor(TupleOperatorV2):
    """
    An operator that can execute R code on R Lists (R's representation of a Tuple)
    """

    is_source = False

    _combine_binary_and_non_binary_lists = robjects.r(
        """
        function(non_binary_list, binary_list) {
            non_binary_list <- as.list(non_binary_list$as_vector())
            return (c(non_binary_list, binary_list))
        }
        """
    )

    def __init__(self, r_code: str):
        """
        Initialize the RTupleExecutor with R code.

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
        with local_converter(arrow_converter):
            input_schema: pa.Schema = tuple_._schema.as_arrow_schema()
            input_fields: list[str] = [field.name for field in input_schema]
            non_binary_fields: list[str] = [
                field.name for field in input_schema if field.type != pa.binary()
            ]
            binary_fields: list[str] = [
                field.name for field in input_schema if field.type == pa.binary()
            ]

            non_binary_pyarrow_array: pa.StructArray = pa.array([], type=pa.struct([]))
            if non_binary_fields:
                non_binary_tuple: Tuple = tuple_.get_partial_tuple(non_binary_fields)
                non_binary_tuple_schema: pa.Schema = (
                    non_binary_tuple._schema.as_arrow_schema()
                )
                non_binary_pyarrow_array: pa.StructArray = pa.array(
                    [non_binary_tuple.as_dict()],
                    type=pa.struct(non_binary_tuple_schema),
                )

            binary_r_list: dict[str, object] = {}
            if binary_fields:
                binary_tuple: Tuple = tuple_.get_partial_tuple(binary_fields)
                for k, v in binary_tuple.as_dict().items():
                    if isinstance(v, bytes):
                        binary_r_list[k] = pickle.loads(v[10:])
                    elif isinstance(v, datetime.datetime):
                        binary_r_list[k] = robjects.vectors.POSIXct.sexp_from_datetime(
                            [v]
                        )
                    else:
                        binary_r_list[k] = v

            binary_r_list: rpy2.robjects.ListVector = robjects.vectors.ListVector(
                binary_r_list
            )

            input_r_list: rpy2.robjects.ListVector = (
                RTupleExecutor._combine_binary_and_non_binary_lists(
                    non_binary_pyarrow_array, binary_r_list
                )
            )

            output_r_generator: rpy2.robjects.SignatureTranslatedFunction = self._func(
                input_r_list, port
            )

            while True:
                output_py_tuple: Tuple = r_utils.extract_tuple_from_r(
                    output_r_generator, False, input_fields
                )
                yield output_py_tuple if output_py_tuple is not None else None
                if output_py_tuple is None:
                    break


class RTupleSourceExecutor(SourceOperator):
    """
    A source operator that produces a generator that yields R Lists using R code.
    R Lists are R's representation of a Tuple
    """

    is_source = True

    def __init__(self, r_code: str):
        """
        Initialize the RTupleSourceExecutor with R code.

        Args:
            r_code (str): R code to be executed.
        """
        super().__init__()
        # Use the local converter from rpy2 to load in the R function given by the user
        with local_converter(default_converter):
            self._func = robjects.r(r_code)

    def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:
        """
        Produce Tuples using the provided R generator returned by the UDF.
        The returned R generator is an iterator
        that yields R Lists (R's representation of Tuple)
        Used by the source operator only.

        :return: Iterator[Union[TupleLike, TableLike, None]], producing
            one TupleLike object, one TableLike object, or None, at a time.
        """
        with local_converter(arrow_converter):
            output_r_generator: rpy2.robjects.SignatureTranslatedFunction = self._func()
            while True:
                output_py_tuple: Tuple = r_utils.extract_tuple_from_r(
                    output_r_generator, True
                )
                yield output_py_tuple if output_py_tuple is not None else None
                if output_py_tuple is None:
                    break
