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

package org.apache.texera.amber.operator.visualization.parallelCoordinatesPlot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext

class ParallelCoordinatesPlotOpDesc extends PythonOperatorDescriptor {

  @JsonProperty(value = "dimensions", required = true)
  @JsonSchemaTitle("Dimensions")
  @JsonPropertyDescription("List of numeric columns to visualize as parallel axes")
  @AutofillAttributeName
  var dimensions: List[String] = List()

  @JsonProperty(value = "color", required = false)
  @JsonSchemaTitle("Color Column")
  @JsonPropertyDescription("Column used to color or group the lines")
  @AutofillAttributeName
  var color: String = _

  override def operatorInfo: OperatorInfo =
    OperatorInfo.forVisualization(
      "Parallel Coordinates Plot",
      "Visualize multivariate data using parallel coordinate axes",
      OperatorGroupConstants.VISUALIZATION_SCIENTIFIC_GROUP
    )

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  def manipulateTable(): PythonTemplateBuilder = {
    val dimCols = dimensions.map(c => pyb"$c").mkString(",")
    val colorFilter =
      if (color != null && color.nonEmpty) pyb"&(table[$color].notnull())"
      else ""
    pyb"""
         |        table = table[table[$dimCols].notnull().all(axis=1)$colorFilter].copy()
         |"""
  }
}
