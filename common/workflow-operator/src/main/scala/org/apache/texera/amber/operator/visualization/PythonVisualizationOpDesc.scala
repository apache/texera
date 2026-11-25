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

package org.apache.texera.amber.operator.visualization

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.{PythonOperatorDescriptor, StandaloneCodeGenerator}

/**
  * A chart drawn by Python code.
  *
  * Its one output port carries a single `html-content` STRING column, whatever
  * the input schemas are: that column is where the frontend finds the rendered
  * chart. Its exported script writes the chart to a file rather than producing
  * a table, so the translator does not print it as a leaf DataFrame. Both
  * members are final, so a chart cannot opt out of either.
  */
trait PythonVisualizationOpDesc extends PythonOperatorDescriptor with StandaloneCodeGenerator {

  final override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] =
    Map(operatorInfo.outputPorts.head.id -> Schema().add("html-content", AttributeType.STRING))

  final override def producesDataFrame(): Boolean = false
}

object PythonVisualizationOpDesc {
  private[visualization] final val NumericValueRuleJson = """
{
  "attributeTypeRules": {
    "value": {
      "enum": ["integer", "long", "double"]
    }
  }
}
"""

  private[visualization] final val ColorPatternJson = """
{
  "pattern": "^\\s*$|^\\s*#(?:\\s*[0-9a-fA-F]){3}(?:(?:\\s*[0-9a-fA-F]){3})?\\s*$|^\\s*(?:[rR]\\s*[gG]\\s*[bB]|[hH]\\s*[sS]\\s*[lL]|[hH]\\s*[sS]\\s*[vV])(?:\\s*[aA])?\\s*\\(\\s*(?:\\s*[0-9.])+(?:\\s*%)?(?:\\s*,(?:\\s*[0-9.])+(?:\\s*%)?){2,3}\\s*\\)\\s*$|^\\s*[vV]\\s*[aA]\\s*[rR]\\s*\\(\\s*-\\s*-[^)]*\\)\\s*$|^\\s*[a-zA-Z][a-zA-Z\\s]*$",
  "examples": ["red"]
}
"""
}
