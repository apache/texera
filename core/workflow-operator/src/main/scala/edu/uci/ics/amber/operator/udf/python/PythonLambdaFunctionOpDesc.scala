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

package edu.uci.ics.amber.operator.udf.python

import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.tuple.{AttributeTypeUtils, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PortIdentity}

class PythonLambdaFunctionOpDesc extends PythonOperatorDescriptor {
  @JsonSchemaTitle("Add/Modify column(s)")
  var lambdaAttributeUnits: List[LambdaAttributeUnit] = List()

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    Preconditions.checkArgument(inputSchemas.size == 1)
    Preconditions.checkArgument(lambdaAttributeUnits.nonEmpty)

    val inputSchema = inputSchemas.values.head
    var outputSchema = inputSchema

    // Add new attributes
    for (unit <- lambdaAttributeUnits) {
      if (unit.attributeName.equalsIgnoreCase("Add New Column")) {
        if (outputSchema.containsAttribute(unit.newAttributeName)) {
          throw new RuntimeException(
            s"Column name ${unit.newAttributeName} already exists!"
          )
        }
        if (unit.newAttributeName != null && unit.newAttributeName.nonEmpty) {
          outputSchema = outputSchema.add(unit.newAttributeName, unit.attributeType)
        }
      }
    }

    // Type casting
    for (unit <- lambdaAttributeUnits) {
      if (!unit.attributeName.equalsIgnoreCase("Add New Column")) {
        outputSchema =
          AttributeTypeUtils.SchemaCasting(outputSchema, unit.attributeName, unit.attributeType)
      }
    }

    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Python Lambda Function",
      "Modify or add a new column with more ease",
      OperatorGroupConstants.PYTHON_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort()),
      supportReconfiguration = true
    )

  override def generatePythonCode(): String = {
    // build the python udf code
    var code: String =
      "from pytexera import *\n" +
        "class ProcessTupleOperator(UDFOperatorV2):\n" +
        "    @overrides\n" +
        "    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:\n"
    if (lambdaAttributeUnits != null) {
      for (unit <- lambdaAttributeUnits) {
        val attrName =
          if (unit.attributeName.equalsIgnoreCase("Add New Column")) unit.newAttributeName
          else unit.attributeName
        if (unit.expression != null && unit.expression.nonEmpty) {
          code += s"""        tuple_['$attrName'] = ${unit.expression}\n"""
        } else
          throw new RuntimeException(
            s"Column name ${attrName}'s expression shouldn't be null or empty!"
          )
      }
    }
    code + "        yield tuple_\n"
  }
}
