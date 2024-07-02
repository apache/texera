import pickle
import datetime
import pyarrow as pa
import rpy2
import rpy2.rinterface as rinterface
import rpy2.robjects as robjects
from rpy2_arrow.arrow import converter as arrow_converter
from rpy2.robjects import default_converter
from rpy2.robjects.conversion import localconverter as local_converter
from typing import Iterator, Optional, Union
from core.models import Tuple, TupleLike, TableLike
from core.models.operator import SourceOperator, TupleOperatorV2
import warnings

warnings.filterwarnings(action="ignore", category=UserWarning, module=r"rpy2*")


def _convert_r_to_py(value: rpy2.robjects):
    """
    :param value: A value that is from one of rpy2's many types (from rpy2.robjects)
    :return: A Python representation of the value, if convertable.
        If not, it returns the value itself
    """
    if isinstance(value, robjects.vectors.BoolVector):
        return bool(value[0])
    if isinstance(value, robjects.vectors.IntVector):
        return int(value[0])
    if isinstance(value, robjects.vectors.FloatVector):
        if isinstance(value, robjects.vectors.POSIXct):
            return next(value.iter_localized_datetime())
        else:
            return float(value[0])
    if isinstance(value, robjects.vectors.StrVector):
        return str(value[0])
    return value


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

            non_binary_tuple: Tuple = tuple_.get_partial_tuple(non_binary_fields)
            non_binary_tuple_schema: pa.Schema = (
                non_binary_tuple._schema.as_arrow_schema()
            )
            non_binary_pyarrow_array: pa.StructArray = pa.array(
                [non_binary_tuple.as_dict()], type=pa.struct(non_binary_tuple_schema)
            )

            binary_tuple: Tuple = tuple_.get_partial_tuple(binary_fields)
            binary_r_list: dict[str, object] = {}
            for k, v in binary_tuple.as_dict().items():
                if isinstance(v, bytes):
                    binary_r_list[k] = pickle.loads(v[10:])
                elif isinstance(v, datetime.datetime):
                    binary_r_list[k] = robjects.vectors.POSIXct.sexp_from_datetime([v])
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
                output_r_tuple: rpy2.robjects.ListVector = output_r_generator()
                if (
                        isinstance(output_r_tuple, rinterface.SexpSymbol)
                        and str(output_r_tuple) == ".__exhausted__."
                ):
                    break
                if isinstance(output_r_tuple.names, rpy2.rinterface_lib.sexp.NULLType):
                    yield None
                    break

                diff_fields: list[str] = [
                    field_name
                    for field_name in output_r_tuple.names
                    if field_name not in input_fields
                ]

                output_python_dict: dict[str, object] = {
                    key: output_r_tuple.rx2(key) for key in (input_fields + diff_fields)
                }
                output_python_dict: dict[str, object] = {
                    key: _convert_r_to_py(value)
                    for key, value in output_python_dict.items()
                }
                yield Tuple(output_python_dict)


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
        Produce Tuples using the provided R function.
        Used by the source operator only.

        :return: Iterator[Union[TupleLike, TableLike, None]], producing
            one TupleLike object, one TableLike object, or None, at a time.
        """
        with local_converter(arrow_converter):
            output_r_generator: rpy2.robjects.SignatureTranslatedFunction = self._func()
            while True:
                output_r_tuple: rpy2.robjetcs.ListVector = output_r_generator()
                if (
                        isinstance(output_r_tuple, rinterface.SexpSymbol)
                        and str(output_r_tuple) == ".__exhausted__."
                ):
                    break
                if isinstance(output_r_tuple.names, rpy2.rinterface_lib.sexp.NULLType):
                    yield None
                    break

                output_python_dict = {
                    key: output_r_tuple.rx2(key) for key in output_r_tuple.names
                }

                output_python_dict: dict[str, object] = {
                    key: _convert_r_to_py(value)
                    for key, value in output_python_dict.items()
                }
                yield Tuple(output_python_dict)
