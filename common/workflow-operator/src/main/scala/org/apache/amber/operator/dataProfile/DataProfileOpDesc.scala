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

  @JsonProperty(
    value = "Minimal Mode",
    required = false,
    defaultValue = "false"
  )
  @JsonPropertyDescription("Enable minimal mode for faster profiling with fewer statistics")
  var minimalMode: Boolean = false

  @JsonProperty(
    value = "Explorative Mode",
    required = false,
    defaultValue = "false"
  )
  @JsonPropertyDescription("Enable explorative mode for more detailed analysis")
  var explorativeMode: Boolean = false

  @JsonProperty()
  @JsonSchemaTitle("Sample Size")
  @JsonPropertyDescription("Number of rows to sample for profiling (leave empty to use all rows)")
  @JsonDeserialize(contentAs = classOf[Int])
  var sampleSize: Option[Int] = None

  @JsonProperty(
    value = "Enable Correlations",
    required = false,
    defaultValue = "true"
  )
  @JsonPropertyDescription("Calculate correlation matrices")
  var enableCorrelations: Boolean = true

  @JsonProperty(
    value = "Enable Missing Values Analysis",
    required = false,
    defaultValue = "true"
  )
  @JsonPropertyDescription("Analyze missing values patterns")
  var enableMissingValues: Boolean = true

  @JsonProperty(
    value = "Enable Duplicates Check",
    required = false,
    defaultValue = "true"
  )
  @JsonPropertyDescription("Check for duplicate rows")
  var enableDuplicates: Boolean = true

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

    // Build ProfileReport parameters
    val minimalModePy = if (minimalMode) "True" else "False"
    val explorativeModePy = if (explorativeMode) "True" else "False"

    // For correlations, missing_diagrams, duplicates: None disables, omitting enables
    val correlationsParam = if (enableCorrelations) "" else "correlations=None,"
    val missingDiagramsParam = if (enableMissingValues) "" else "missing_diagrams=None,"
    val duplicatesParam = if (enableDuplicates) "" else "duplicates=None,"

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
       |        # Generate profile report
       |        profile = ProfileReport(
       |            df,
       |            minimal=$minimalModePy,
       |            explorative=$explorativeModePy,
       |            $correlationsParam
       |            $missingDiagramsParam
       |            $duplicatesParam
       |        )
       |
       |        # Convert report to JSON
       |        report_json = profile.to_json()
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
      "Generate comprehensive data profiling report using ydata-profiling",
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
