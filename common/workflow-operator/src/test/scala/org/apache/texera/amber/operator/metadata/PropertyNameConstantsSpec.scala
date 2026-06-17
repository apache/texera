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

  private val logicalPlanKeys = Map(
    "OPERATOR_ID" -> PropertyNameConstants.OPERATOR_ID,
    "OPERATOR_TYPE" -> PropertyNameConstants.OPERATOR_TYPE,
    "OPERATOR_LIST" -> PropertyNameConstants.OPERATOR_LIST,
    "OPERATOR_LINK_LIST" -> PropertyNameConstants.OPERATOR_LINK_LIST,
    "OPERATOR_VERSION" -> PropertyNameConstants.OPERATOR_VERSION,
    "ORIGIN_OPERATOR_ID" -> PropertyNameConstants.ORIGIN_OPERATOR_ID,
    "DESTINATION_OPERATOR_ID" -> PropertyNameConstants.DESTINATION_OPERATOR_ID
  )

  private val commonOperatorKeys = Map(
    "ATTRIBUTE_NAMES" -> PropertyNameConstants.ATTRIBUTE_NAMES,
    "ATTRIBUTE_NAME" -> PropertyNameConstants.ATTRIBUTE_NAME,
    "RESULT_ATTRIBUTE_NAME" -> PropertyNameConstants.RESULT_ATTRIBUTE_NAME,
    "SPAN_LIST_NAME" -> PropertyNameConstants.SPAN_LIST_NAME,
    "TABLE_NAME" -> PropertyNameConstants.TABLE_NAME
  )

  private val physicalPlanKeys = Map(
    "WORKFLOW_ID" -> PropertyNameConstants.WORKFLOW_ID,
    "EXECUTION_ID" -> PropertyNameConstants.EXECUTION_ID,
    "PARALLELIZABLE" -> PropertyNameConstants.PARALLELIZABLE,
    "LOCATION_PREFERENCE" -> PropertyNameConstants.LOCATION_PREFERENCE,
    "PARTITION_REQUIREMENT" -> PropertyNameConstants.PARTITION_REQUIREMENT,
    "INPUT_PORTS" -> PropertyNameConstants.INPUT_PORTS,
    "OUTPUT_PORTS" -> PropertyNameConstants.OUTPUT_PORTS,
    "IS_ONE_TO_MANY_OP" -> PropertyNameConstants.IS_ONE_TO_MANY_OP,
    "SUGGESTED_WORKER_NUM" -> PropertyNameConstants.SUGGESTED_WORKER_NUM
  )

  "PropertyNameConstants" should "pin logical-plan key values" in {
    assert(
      logicalPlanKeys == Map(
        "OPERATOR_ID" -> "operatorID",
        "OPERATOR_TYPE" -> "operatorType",
        "OPERATOR_LIST" -> "operators",
        "OPERATOR_LINK_LIST" -> "links",
        "OPERATOR_VERSION" -> "operatorVersion",
        "ORIGIN_OPERATOR_ID" -> "origin",
        "DESTINATION_OPERATOR_ID" -> "destination"
      )
    )
  }

  it should "pin common operator key values" in {
    assert(
      commonOperatorKeys == Map(
        "ATTRIBUTE_NAMES" -> "attributes",
        "ATTRIBUTE_NAME" -> "attribute",
        "RESULT_ATTRIBUTE_NAME" -> "resultAttribute",
        "SPAN_LIST_NAME" -> "spanListName",
        "TABLE_NAME" -> "tableName"
      )
    )
  }

  it should "pin physical-plan key values" in {
    assert(
      physicalPlanKeys == Map(
        "WORKFLOW_ID" -> "workflowID",
        "EXECUTION_ID" -> "executionID",
        "PARALLELIZABLE" -> "parallelizable",
        "LOCATION_PREFERENCE" -> "locationPreference",
        "PARTITION_REQUIREMENT" -> "partitionRequirement",
        "INPUT_PORTS" -> "inputPorts",
        "OUTPUT_PORTS" -> "outputPorts",
        "IS_ONE_TO_MANY_OP" -> "isOneToManyOp",
        "SUGGESTED_WORKER_NUM" -> "suggestedWorkerNum"
      )
    )
  }

  it should "keep all constant values distinct" in {
    val allValues = (logicalPlanKeys ++ commonOperatorKeys ++ physicalPlanKeys).values.toList

    assert(allValues.distinct.size == allValues.size)
  }
}
