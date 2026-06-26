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

package org.apache.texera.amber.operator.metadata

import org.scalatest.flatspec.AnyFlatSpec

class PropertyNameConstantsSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Logical-plan keys — each is a stable identifier used in every workflow
  // JSON we have ever shipped; a rename breaks every persisted plan.
  // ---------------------------------------------------------------------------

  "PropertyNameConstants logical-plan keys" should "have their canonical String values" in {
    assert(PropertyNameConstants.OPERATOR_ID == "operatorID")
    assert(PropertyNameConstants.OPERATOR_TYPE == "operatorType")
    assert(PropertyNameConstants.ORIGIN_OPERATOR_ID == "origin")
    assert(PropertyNameConstants.DESTINATION_OPERATOR_ID == "destination")
    assert(PropertyNameConstants.OPERATOR_LIST == "operators")
    assert(PropertyNameConstants.OPERATOR_LINK_LIST == "links")
    assert(PropertyNameConstants.OPERATOR_VERSION == "operatorVersion")
  }

  // ---------------------------------------------------------------------------
  // Common operator-property keys
  // ---------------------------------------------------------------------------

  "PropertyNameConstants common-property keys" should "have their canonical String values" in {
    assert(PropertyNameConstants.ATTRIBUTE_NAMES == "attributes")
    assert(PropertyNameConstants.ATTRIBUTE_NAME == "attribute")
    assert(PropertyNameConstants.RESULT_ATTRIBUTE_NAME == "resultAttribute")
    assert(PropertyNameConstants.SPAN_LIST_NAME == "spanListName")
    assert(PropertyNameConstants.TABLE_NAME == "tableName")
  }

  // ---------------------------------------------------------------------------
  // Physical-plan keys
  // ---------------------------------------------------------------------------

  "PropertyNameConstants physical-plan keys" should "have their canonical String values" in {
    assert(PropertyNameConstants.WORKFLOW_ID == "workflowID")
    assert(PropertyNameConstants.EXECUTION_ID == "executionID")
    assert(PropertyNameConstants.PARALLELIZABLE == "parallelizable")
    assert(PropertyNameConstants.LOCATION_PREFERENCE == "locationPreference")
    assert(PropertyNameConstants.PARTITION_REQUIREMENT == "partitionRequirement")
    assert(PropertyNameConstants.INPUT_PORTS == "inputPorts")
    assert(PropertyNameConstants.OUTPUT_PORTS == "outputPorts")
    assert(PropertyNameConstants.IS_ONE_TO_MANY_OP == "isOneToManyOp")
    assert(PropertyNameConstants.SUGGESTED_WORKER_NUM == "suggestedWorkerNum")
  }

  // ---------------------------------------------------------------------------
  // Distinctness — no two constants alias to the same string
  // ---------------------------------------------------------------------------

  "PropertyNameConstants" should "have all constants distinct (no accidental aliases)" in {
    val all = List(
      PropertyNameConstants.OPERATOR_ID,
      PropertyNameConstants.OPERATOR_TYPE,
      PropertyNameConstants.ORIGIN_OPERATOR_ID,
      PropertyNameConstants.DESTINATION_OPERATOR_ID,
      PropertyNameConstants.OPERATOR_LIST,
      PropertyNameConstants.OPERATOR_LINK_LIST,
      PropertyNameConstants.OPERATOR_VERSION,
      PropertyNameConstants.ATTRIBUTE_NAMES,
      PropertyNameConstants.ATTRIBUTE_NAME,
      PropertyNameConstants.RESULT_ATTRIBUTE_NAME,
      PropertyNameConstants.SPAN_LIST_NAME,
      PropertyNameConstants.TABLE_NAME,
      PropertyNameConstants.WORKFLOW_ID,
      PropertyNameConstants.EXECUTION_ID,
      PropertyNameConstants.PARALLELIZABLE,
      PropertyNameConstants.LOCATION_PREFERENCE,
      PropertyNameConstants.PARTITION_REQUIREMENT,
      PropertyNameConstants.INPUT_PORTS,
      PropertyNameConstants.OUTPUT_PORTS,
      PropertyNameConstants.IS_ONE_TO_MANY_OP,
      PropertyNameConstants.SUGGESTED_WORKER_NUM
    )
    assert(all.distinct.size == all.size, s"duplicate constant value(s) in: $all")
  }

  it should "carry no leading/trailing whitespace on any constant" in {
    val all = List(
      PropertyNameConstants.OPERATOR_ID,
      PropertyNameConstants.OPERATOR_TYPE,
      PropertyNameConstants.ORIGIN_OPERATOR_ID,
      PropertyNameConstants.DESTINATION_OPERATOR_ID,
      PropertyNameConstants.OPERATOR_LIST,
      PropertyNameConstants.OPERATOR_LINK_LIST,
      PropertyNameConstants.OPERATOR_VERSION,
      PropertyNameConstants.ATTRIBUTE_NAMES,
      PropertyNameConstants.ATTRIBUTE_NAME,
      PropertyNameConstants.RESULT_ATTRIBUTE_NAME,
      PropertyNameConstants.SPAN_LIST_NAME,
      PropertyNameConstants.TABLE_NAME,
      PropertyNameConstants.WORKFLOW_ID,
      PropertyNameConstants.EXECUTION_ID,
      PropertyNameConstants.PARALLELIZABLE,
      PropertyNameConstants.LOCATION_PREFERENCE,
      PropertyNameConstants.PARTITION_REQUIREMENT,
      PropertyNameConstants.INPUT_PORTS,
      PropertyNameConstants.OUTPUT_PORTS,
      PropertyNameConstants.IS_ONE_TO_MANY_OP,
      PropertyNameConstants.SUGGESTED_WORKER_NUM
    )
    all.foreach(c => assert(c == c.trim, s"constant has surrounding whitespace: '$c'"))
  }
}
