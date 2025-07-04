#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from core.models import Tuple
from proto.edu.uci.ics.amber.engine.architecture.worker import (
    TableProfile, GlobalProfile, NumericMatrix, ColumnProfile, ColumnIndexList
)
from typing import List, Dict, Any
import pandas as pd
from dataprofiler import Profiler, ProfilerOptions

# --------------------------------------------------------------------------- #
# Helpers to convert DataProfiler dict ---------------> protobuf TableProfile #
# --------------------------------------------------------------------------- #

def _to_numeric_matrix(flat: List[float]) -> NumericMatrix:
    return NumericMatrix(values=flat, rows=0, cols=0)   # TODO: update dimensions

def _dp_global_to_proto(gjs: Dict[str, Any]) -> GlobalProfile:
    gp = GlobalProfile(
        samples_used=int(gjs["samples_used"]),
        column_count=int(gjs["column_count"]),
        row_count=int(gjs["row_count"]),
        row_has_null_ratio=gjs["row_has_null_ratio"],
        row_is_null_ratio=gjs["row_is_null_ratio"],
        unique_row_ratio=gjs["unique_row_ratio"],
        duplicate_row_count=int(gjs["duplicate_row_count"]),
        file_type=gjs.get("file_type", ""),
        encoding=gjs.get("encoding", "")
    )

    correlation = gjs.get("correlation_matrix")
    if correlation and isinstance(correlation, list):
        flat = [float(x) for row in correlation for x in row]
        gp.correlation_matrix = _to_numeric_matrix(flat)

    chi2 = gjs.get("chi2_matrix")
    if chi2 and isinstance(chi2, list):
        flat = [float(x) for row in chi2 for x in row]
        gp.chi2_matrix = _to_numeric_matrix(flat)

    for col, idx_list in gjs.get("profile_schema", {}).items():
        gp.profile_schema[col] = ColumnIndexList(indices=idx_list)

    if "times" in gjs:
        gp.times.row_stats_ms = gjs["times"].get("row_stats", 0) * 1_000

    return gp
def _dp_column_to_proto(cjs: Dict[str, Any]) -> ColumnProfile:
    cp = ColumnProfile(
        column_name=cjs["column_name"],
        data_type=cjs.get("data_type", ""),
        data_label=cjs.get("data_label", ""),
        categorical=bool(cjs.get("categorical")),
        order=cjs.get("order", "")
    )
    samples = cjs.get("samples", [])
    if isinstance(samples, list):
        cp.samples.extend([str(s) for s in samples[:10]])
    elif isinstance(samples, str):
        # compact-format DP report returns a JSON list-as-string
        sample_str = samples.strip("[]")
        cp.samples.extend([s.strip(" '") for s in sample_str.split(",")[:10]])

    stats = cjs.get("statistics", {})
    cs = cp.statistics

    numeric_fields = ("min", "max", "median", "mean", "variance", "stddev", "skewness", "kurtosis", "sum")
    for field in numeric_fields:
        value = stats.get(field)
        if value not in (None, "nan"):
            try:
                # only convert if it looks like a number
                cs_field = float(value)
                setattr(cs, field, cs_field)
            except (ValueError, TypeError):
                # skip non-floatable values like datetime strings
                pass

    if "quantiles" in stats:
        try:
            cs.quantiles.extend([float(v) for _, v in sorted(stats["quantiles"].items())])
        except Exception:
            pass

    cs.num_zeros = int(stats.get("num_zeros", 0))
    cs.num_negatives = int(stats.get("num_negatives", 0))
    cs.unique_count = int(stats.get("unique_count", 0))
    cs.unique_ratio = float(stats.get("unique_ratio", 0))
    cs.null_count = int(stats.get("null_count", 0))
    cs.null_types.extend(stats.get("null_types", []))

    if "categorical_count" in stats:
        for cat, cnt in stats["categorical_count"].items():
            cp.statistics.categorical_count[cat] = int(cnt)

    dtr = stats.get("data_type_representation", {})
    for k, v in dtr.items():
        try:
            cp.statistics.data_type_representation[k] = float(v)
        except Exception:
            pass

    return cp

def dp_report_to_tableprofile(report: Dict[str, Any]) -> TableProfile:
    tp = TableProfile()
    tp.global_profile = _dp_global_to_proto(report["global_stats"])
    for col_js in report["data_stats"]:
        tp.column_profiles.append(_dp_column_to_proto(col_js))
    return tp

# --------------------------------------------------------------------------- #
#                     TableProfileManager (Python version)                    #
# --------------------------------------------------------------------------- #

class TableProfileManager:
    def __init__(self):
        self._rows: List[Dict[str, Any]] = []
        self._profile_proto: TableProfile | None = None
        self._dirty: bool = False                      # ← tracks if buffer changed

        profiler_options = ProfilerOptions()
        profiler_options.set({
            "structured_options.data_labeler.is_enabled": True
        })
        self.profiler_options = profiler_options

    def update_table_profile(self, tup: Tuple):
        row_dict = tup.as_dict()
        self._rows.append(row_dict)
        self._dirty = True

    def get_table_profile(self) -> TableProfile:
        """
        Return the protobuf `TableProfile`.

        It is rebuilt iff (a) we do not have one yet, or (b) new rows were
        appended since the last build (`_dirty` is True).
        """
        if self._profile_proto is None or self._dirty:
            self._profile_proto = self._build_profile()
        return self._profile_proto

    def _build_profile(self) -> TableProfile:
        """
        Build (or rebuild) the profile from the current buffer.
        Called only by `get_table_profile`.
        """
        try:
            if not self._rows:
                # no data yet – return an empty proto
                return TableProfile()

            df = pd.DataFrame(self._rows)
            profile = Profiler(
                df,
                options=self.profiler_options,
                profiler_type="structured",
            )
            report = profile.report(report_options={"output_format": "compact"})
            return dp_report_to_tableprofile(report)
        finally:
            # whether succeeded or failed, mark buffer as clean so that we
            # don’t rebuild until new tuples arrive
            self._dirty = False

    def to_bytes(self) -> bytes:
        return self.get_table_profile().SerializeToString()

    def reset(self):
        self._rows.clear()
        self._profile_proto = None
