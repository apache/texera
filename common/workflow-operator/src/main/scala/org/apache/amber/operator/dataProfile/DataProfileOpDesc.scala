/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.amber.operator.dataProfile

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.amber.core.tuple.{AttributeType, Schema}
import org.apache.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.amber.operator.PythonOperatorDescriptor
import org.apache.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}

class DataProfileOpDesc extends PythonOperatorDescriptor {

  @JsonProperty()
  @JsonSchemaTitle("Sample Size")
  @JsonPropertyDescription("Number of rows to sample for profiling (leave empty to use all rows)")
  @JsonDeserialize(contentAs = classOf[Int])
  var sampleSize: Option[Int] = None

  @JsonProperty(defaultValue = "[]")
  @JsonSchemaTitle("Columns to Profile")
  @JsonPropertyDescription(
    "Select specific columns to profile (leave empty to profile all columns)"
  )
  var columns: List[ColumnUnit] = List()

  override def generatePythonCode(): String = {
    val sampleCode = sampleSize match {
      case Some(size) => s"df = df.sample(n=min($size, len(df)), random_state=42)"
      case None       => ""
    }

    val columnSelectionCode = if (columns != null && columns.nonEmpty) {
      val columnNames = columns.map(_.getColumnName).mkString("', '")
      s"df = df[['$columnNames']]"
    } else {
      ""
    }

    s"""
       |import pandas as pd
       |import json
       |from ydata_profiling import ProfileReport
       |from pytexera import *
       |
       |class ProcessTableOperator(UDFTableOperator):
       |
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        # Table is already a DataFrame in pytexera
       |        df = table
       |
       |        $columnSelectionCode
       |
       |        $sampleCode
       |
       |        # Configure minimal profile settings to reduce token usage
       |        profile = ProfileReport(
       |            df,
       |            minimal=True,  # Use minimal mode
       |            # Disable expensive computations
       |            correlations=None,
       |            missing_diagrams=None,
       |            duplicates=None,
       |            interactions=None,
       |            samples=None
       |        )
       |
       |        # Get the full JSON report
       |        report_json_str = profile.to_json()
       |        report_data = json.loads(report_json_str)
       |
       |        # Extract only essential metrics
       |        essential_metrics = {
       |            'table_stats': {
       |                'n_rows': report_data.get('table', {}).get('n', 0),
       |                'n_columns': report_data.get('table', {}).get('n_var', 0),
       |                'missing_cells': report_data.get('table', {}).get('n_cells_missing', 0),
       |                'missing_percentage': report_data.get('table', {}).get('p_cells_missing', 0),
       |                'duplicates': report_data.get('table', {}).get('n_duplicates', 0),
       |                'duplicate_percentage': report_data.get('table', {}).get('p_duplicates', 0),
       |                'memory_size': report_data.get('table', {}).get('memory_size', 0),
       |                'column_types': report_data.get('table', {}).get('types', {})
       |            },
       |            'variables': {}
       |        }
       |
       |        # Extract essential variable-level metrics
       |        variables = report_data.get('variables', {})
       |        for col_name, col_stats in variables.items():
       |            essential_metrics['variables'][col_name] = {
       |                'type': col_stats.get('type'),
       |                'n_missing': col_stats.get('n_missing', 0),
       |                'p_missing': col_stats.get('p_missing', 0),
       |                'n_distinct': col_stats.get('n_distinct', 0),
       |                'p_distinct': col_stats.get('p_distinct', 0),
       |                'is_unique': col_stats.get('is_unique', False)
       |            }
       |
       |            # Add numeric-specific stats if available
       |            if 'mean' in col_stats:
       |                essential_metrics['variables'][col_name].update({
       |                    'mean': col_stats.get('mean'),
       |                    'std': col_stats.get('std'),
       |                    'min': col_stats.get('min'),
       |                    'max': col_stats.get('max'),
       |                    'median': col_stats.get('50%')
       |                })
       |
       |            # Add text-specific stats if available
       |            if 'max_length' in col_stats:
       |                essential_metrics['variables'][col_name].update({
       |                    'max_length': col_stats.get('max_length'),
       |                    'mean_length': col_stats.get('mean_length'),
       |                    'min_length': col_stats.get('min_length')
       |                })
       |
       |        # Convert to compact JSON
       |        report_json = json.dumps(essential_metrics, separators=(',', ':'))
       |
       |        # Create output DataFrame with single row containing the JSON report
       |        result_df = pd.DataFrame({
       |            'report': [report_json]
       |        })
       |
       |        yield result_df
       |""".stripMargin
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Data Profile",
      "Generate essential data profiling metrics using ydata-profiling",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    Map(
      operatorInfo.outputPorts.head.id -> Schema()
        .add("report", AttributeType.STRING)
    )
  }
}
