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

import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.core.workflow.PortIdentity
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
class PythonLambdaFunctionOpDescSpec extends AnyFlatSpec with BeforeAndAfter {
  val schema = new Schema(
    new Attribute("column_str", AttributeType.STRING),
    new Attribute("column_int", AttributeType.INTEGER),
    new Attribute("column_bool", AttributeType.BOOLEAN)
  )

  var opDesc: PythonLambdaFunctionOpDesc = _

  before {
    opDesc = new PythonLambdaFunctionOpDesc()
  }

  it should "add one new column into schema successfully" in {
    opDesc.lambdaAttributeUnits ++= List(
      new LambdaAttributeUnit(
        "Add New Column",
        "tuple_['column_str']",
        "newColumn1",
        AttributeType.STRING
      )
    )
    val outputSchema = opDesc.getExternalOutputSchemas(Map(PortIdentity() -> schema)).values.head
    assert(outputSchema.getAttributes.length == 4)
  }

  it should "add multiple new columns into schema successfully" in {
    opDesc.lambdaAttributeUnits ++= List(
      new LambdaAttributeUnit(
        "Add New Column",
        "tuple_['column_str']",
        "newColumn1",
        AttributeType.STRING
      ),
      new LambdaAttributeUnit(
        "Add New Column",
        "tuple_['column_int']",
        "newColumn2",
        AttributeType.INTEGER
      )
    )
    val outputSchema = opDesc.getExternalOutputSchemas(Map(PortIdentity() -> schema)).values.head
    assert(outputSchema.getAttributes.length == 5)
  }

  it should "build successfully when there is no new column but with modifying the existing column" in {
    opDesc.lambdaAttributeUnits ++= List(
      new LambdaAttributeUnit(
        "column_str",
        "tuple_['column_str'] + hello",
        "",
        AttributeType.STRING
      )
    )
    val outputSchema = opDesc.getExternalOutputSchemas(Map(PortIdentity() -> schema)).values.head
    assert(outputSchema.getAttributes.length == 3)
  }

  it should "raise exception if the new column name already exists" in {
    opDesc.lambdaAttributeUnits ++= List(
      new LambdaAttributeUnit(
        "Add New Column",
        "tuple_['column_str']",
        "column_str",
        AttributeType.STRING
      )
    )

    assertThrows[RuntimeException] {
      opDesc.getExternalOutputSchemas(Map(PortIdentity() -> schema)).values.head
    }

  }
}
