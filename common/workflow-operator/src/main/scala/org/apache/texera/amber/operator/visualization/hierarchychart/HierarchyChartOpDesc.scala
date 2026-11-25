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

package org.apache.texera.amber.operator.visualization.hierarchychart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.visualization.{
  PlotlyStandaloneCode,
  PythonVisualizationOpDesc
}
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder

import javax.validation.constraints.{NotEmpty, NotNull}

// type constraint: value can only be numeric
@JsonSchemaInject(json = PythonVisualizationOpDesc.NumericValueRuleJson)
class HierarchyChartOpDesc extends PythonVisualizationOpDesc with PlotlyStandaloneCode {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Chart Type")
  @JsonPropertyDescription("Treemap or Sunburst")
  @NotNull(message = "Chart Type cannot be empty")
  var hierarchyChartType: HierarchyChartType = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Hierarchy Path")
  @JsonPropertyDescription(
    "Hierarchy of attributes from a higher-level category to lower-level category"
  )
  @NotEmpty(message = "Hierarchy Path cannot be empty")
  var hierarchy: List[HierarchySection] = List()

  @JsonProperty(value = "value", required = true)
  @JsonSchemaTitle("Value Column")
  @JsonPropertyDescription("The value associated with the size of each sector in the chart")
  @AutofillAttributeName
  @NotNull(message = "Value Column cannot be empty")
  var value: EncodableString = ""

  override def operatorInfo: OperatorInfo =
    OperatorInfo.forVisualization(
      "Hierarchy Chart",
      "Visualize data in hierarchy",
      OperatorGroupConstants.VISUALIZATION_BASIC_GROUP
    )

  def manipulateTable(): PythonTemplateBuilder =
    HierarchyChartSupport.filterPositiveAndDropNa(value, hierarchy)

  def createPlotlyFigure(): PythonTemplateBuilder =
    HierarchyChartSupport.createPlotlyFigure(
      hierarchyChartType.getPlotlyExpressApiName,
      value,
      hierarchy
    )

  override def generatePythonCode(): String =
    HierarchyChartSupport.runtimePythonCode(
      "Hierarchy chart is not available.",
      manipulateTable(),
      createPlotlyFigure()
    )

  override def generateStandaloneCode(): String =
    HierarchyChartSupport.standalonePythonCode(
      "Hierarchy chart",
      hierarchyChartType.getPlotlyExpressApiName,
      value,
      hierarchy
    )
}
