# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import rpy2
import rpy2.rinterface as rinterface
import rpy2.robjects as robjects
from core.models import Tuple
import warnings

warnings.filterwarnings(action="ignore", category=UserWarning, module=r"rpy2*")


def convert_r_to_py(value: rpy2.robjects):
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


def extract_tuple_from_r(
    output_r_generator: rpy2.robjects.SignatureTranslatedFunction,
    source_operator: bool,
    input_fields: [None, list[str]] = None,
) -> [Tuple, None]:
    output_r_tuple: rpy2.robjects.ListVector = output_r_generator()
    if (
        isinstance(output_r_tuple, rinterface.SexpSymbol)
        and str(output_r_tuple) == ".__exhausted__."
    ) or isinstance(output_r_tuple.names, rpy2.rinterface_lib.sexp.NULLType):
        return None

    output_python_dict: dict[str, object] = {}
    if source_operator:
        output_python_dict = {
            key: output_r_tuple.rx2(key) for key in output_r_tuple.names
        }
    else:
        diff_fields: list[str] = [
            field_name
            for field_name in output_r_tuple.names
            if field_name not in input_fields
        ]
        output_python_dict: dict[str, object] = {
            key: output_r_tuple.rx2(key) for key in (input_fields + diff_fields)
        }

    output_python_dict: dict[str, object] = {
        key: convert_r_to_py(value) for key, value in output_python_dict.items()
    }

    return Tuple(output_python_dict)
