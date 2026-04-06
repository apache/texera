/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.texera.amber.operator.visualization.nestedTable

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.OutputPort.OutputMode
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder

import java.util
import scala.jdk.CollectionConverters.ListHasAsScala

class NestedTableOpDesc extends PythonOperatorDescriptor {

  @JsonPropertyDescription(
    "List of columns to include in the nested table chart and their subgroup"
  )
  @JsonProperty(value = "add attribute", required = true)
  var includedColumns: util.List[NestedTableConfig] = _

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Nested Table",
      "Visualize Data in a Depth Two Nested Table",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

  private def createNestedTable(): PythonTemplateBuilder = {
    val sortedColumns = includedColumns.asScala.sortBy(_.attributeGroup)

    pyb"""
       |        columns = pd.MultiIndex.from_tuples([
       |            ${sortedColumns
      .map { config =>
        val name =
          if (config.newName != null && config.newName.nonEmpty) config.newName
          else config.originalName
        pyb"(${config.attributeGroup}, $name)"
      }
      .mkString(",\n             ")}
       |        ])
       |
       |        data = []
       |        for _, row in table.iterrows():
       |            data.append([
       |                ${sortedColumns
      .map(config => pyb"row[${config.originalName}]")
      .mkString(", ")}
       |            ])
       |
       |        df = pd.DataFrame(data, columns=columns)
       |
       |        styles = [
       |            {'selector': 'th', 'props': [('background-color', '#f2f2f2'),
       |                                          ('color', 'black'),
       |                                          ('font-weight', 'bold'),
       |                                          ('border', '1px solid #ddd'),
       |                                          ('padding', '8px'),
       |                                          ('text-align', 'center')]},
       |            {'selector': 'td', 'props': [('border', '1px solid #ddd'),
       |                                          ('padding', '8px'),
       |                                          ('text-align', 'center')]},
       |            {'selector': 'caption', 'props': [('caption-side', 'top'),
       |                                               ('font-size', '16pt'),
       |                                               ('font-weight', 'bold'),
       |                                               ('text-align', 'left'),
       |                                               ('padding', '10px')]},
       |            {'selector': '.row_heading', 'props': [('text-align', 'left'),
       |                                                    ('font-weight', 'normal')]},
       |            {'selector': '.blank.level0', 'props': [('display', 'none')]}
       |        ]
       |
       |        styled_table = (
       |            df.style
       |            .set_table_styles(styles)
       |            .format(precision=2, na_rep="")
       |            .set_table_attributes('class="dataframe"')
       |            .hide(axis="index")
       |        )
       |"""
  }

  override def generatePythonCode(): String = {
    val finalcode =
      pyb"""
         |from pytexera import *
         |
         |import pandas as pd
         |import numpy as np
         |
         |class ProcessTableOperator(UDFTableOperator):
         |    def render_error(self, error_msg):
         |        return '''<h1>Nested Table is not available.</h1>
         |                  <p>Reason is: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if table.empty:
         |           yield {'html-content': self.render_error("input table is empty.")}
         |           return
         |        ${createNestedTable()}
         |        # convert table to html content
         |        html = styled_table.to_html()
         |        yield {'html-content': html}
         |
         |"""
    finalcode.encode
  }
}
