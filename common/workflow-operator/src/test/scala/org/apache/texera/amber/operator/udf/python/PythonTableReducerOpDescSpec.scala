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

package org.apache.texera.amber.operator.udf.python

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PythonTableReducerOpDescSpec extends AnyFlatSpec with Matchers {

  private def unit(name: String, expr: String, t: AttributeType): LambdaAttributeUnit =
    new LambdaAttributeUnit(name, expr, null, t)

  "PythonTableReducerOpDesc.operatorInfo" should "advertise the name and Python group" in {
    val info = (new PythonTableReducerOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Python Table Reducer"
    info.operatorDescription shouldBe "Reduce Table to Tuple"
    info.operatorGroupName shouldBe OperatorGroupConstants.PYTHON_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "PythonTableReducerOpDesc" should "default lambdaAttributeUnits to an empty list" in {
    (new PythonTableReducerOpDesc).lambdaAttributeUnits shouldBe empty
  }

  "PythonTableReducerOpDesc.getOutputSchemas" should
    "fold each lambda unit into an output column keyed by the declared output port" in {
    val d = new PythonTableReducerOpDesc
    d.lambdaAttributeUnits = List(unit("score", "1 + 1", AttributeType.INTEGER))
    d.getOutputSchemas(Map.empty) shouldBe Map(
      d.operatorInfo.outputPorts.head.id -> Schema().add("score", AttributeType.INTEGER)
    )
  }

  it should "reject an empty lambda list" in {
    intercept[IllegalArgumentException] {
      (new PythonTableReducerOpDesc).getOutputSchemas(Map.empty)
    }
  }

  "PythonTableReducerOpDesc.generatePythonCode" should "emit the reducer table operator" in {
    val d = new PythonTableReducerOpDesc
    d.lambdaAttributeUnits = List(unit("score", "1 + 1", AttributeType.INTEGER))
    val code = d.generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator)")
    code should include("score")
  }

  "PythonTableReducerOpDesc" should "round-trip its lambda units through the polymorphic base" in {
    val d = new PythonTableReducerOpDesc
    d.lambdaAttributeUnits =
      List(new LambdaAttributeUnit("score", "1 + 1", "scoreOut", AttributeType.INTEGER))
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[PythonTableReducerOpDesc]
    val r = restored.asInstanceOf[PythonTableReducerOpDesc]
    r.lambdaAttributeUnits should have length 1
    r.lambdaAttributeUnits.head.attributeName shouldBe "score"
    r.lambdaAttributeUnits.head.expression shouldBe "1 + 1"
    r.lambdaAttributeUnits.head.newAttributeName shouldBe "scoreOut"
    r.lambdaAttributeUnits.head.attributeType shouldBe AttributeType.INTEGER
  }
}
