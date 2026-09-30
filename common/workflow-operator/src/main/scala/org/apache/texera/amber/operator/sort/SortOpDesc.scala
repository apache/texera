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

package org.apache.texera.amber.operator.sort

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.{
  PythonTemplateBuilderStringContext,
  pyStringLiteral
}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.{PythonOperatorDescriptor, StandaloneCodeGenerator}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
class SortOpDesc extends PythonOperatorDescriptor with StandaloneCodeGenerator {

  // Sorting is this operator's contract: output row order is meaningful.
  override def orderSensitive: Boolean = true

  @JsonProperty(required = true)
  @JsonPropertyDescription("column to perform sorting on")
  var attributes: List[SortCriteriaUnit] = List.empty

  override def generatePythonCode(): String = {
    require(attributes.nonEmpty, "Sort operator requires at least one sort key.")
    require(
      attributes.forall(c => c.attributeName != null && c.attributeName.trim.nonEmpty),
      "Each sort key must have an attribute selected."
    )
    val attributeName = "[" + attributes
      .map { criteria =>
        pyb"""${criteria.attributeName}"""
      }
      .mkString(", ") + "]"
    val sortOrders: String = "[" + attributes
      .map { criteria =>
        criteria.sortPreference match {
          case SortPreference.ASC  => "True"
          case SortPreference.DESC => "False"
        }
      }
      .mkString(", ") + "]"

    pyb"""from pytexera import *
       |import pandas as pd
       |from datetime import datetime
       |
       |class ProcessTableOperator(UDFTableOperator):
       |
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        sort_columns = $attributeName
       |        ascending_orders = $sortOrders
       |
       |        sorted_df = table.sort_values(by=sort_columns, ascending=ascending_orders)
       |        yield sorted_df""".encode
  }

  def getOutputSchemas(inputSchemas: Map[PortIdentity, Schema]): Map[PortIdentity, Schema] = {
    Map(operatorInfo.outputPorts.head.id -> inputSchemas.values.head)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Sort",
      "Sort based on the columns and sorting methods",
      OperatorGroupConstants.SORT_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(blocking = true))
    )

  override def generateStandaloneCode(): String = {
    val cols = attributes.map(c => pyStringLiteral(c.attributeName)).mkString("[", ", ", "]")
    val ascending = attributes
      .map(c => if (c.sortPreference == SortPreference.ASC) "True" else "False")
      .mkString("[", ", ", "]")
    s"out1df = in1df.sort_values(by=$cols, ascending=$ascending)"
  }
}
