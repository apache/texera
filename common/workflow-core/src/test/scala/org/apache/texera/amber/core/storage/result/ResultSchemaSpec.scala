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

package org.apache.texera.amber.core.storage.result

import org.apache.texera.amber.core.tuple.AttributeType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ResultSchemaSpec extends AnyFlatSpec with Matchers {

  "ResultSchema.runtimeStatisticsSchema" should "declare the runtime-statistics columns in order" in {
    val schema = ResultSchema.runtimeStatisticsSchema
    schema.getAttributeNames shouldBe List(
      "operatorId",
      "time",
      "inputTupleCnt",
      "inputTupleSize",
      "outputTupleCnt",
      "outputTupleSize",
      "dataProcessingTime",
      "controlProcessingTime",
      "idleTime",
      "numWorkers",
      "status"
    )
    schema.getAttribute("operatorId").getType shouldBe AttributeType.STRING
    schema.getAttribute("time").getType shouldBe AttributeType.TIMESTAMP
    List(
      "inputTupleCnt",
      "inputTupleSize",
      "outputTupleCnt",
      "outputTupleSize",
      "dataProcessingTime",
      "controlProcessingTime",
      "idleTime"
    ).foreach(name => schema.getAttribute(name).getType shouldBe AttributeType.LONG)
    schema.getAttribute("numWorkers").getType shouldBe AttributeType.INTEGER
    schema.getAttribute("status").getType shouldBe AttributeType.INTEGER
  }

  "ResultSchema.consoleMessagesSchema" should "declare a single string message column" in {
    val schema = ResultSchema.consoleMessagesSchema
    schema.getAttributeNames shouldBe List("message")
    schema.getAttribute("message").getType shouldBe AttributeType.STRING
  }
}
