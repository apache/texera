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

package org.apache.texera.service.util

import org.apache.texera.dao.jooq.generated.enums.WorkflowComputingUnitTypeEnum
import org.apache.texera.dao.jooq.generated.tables.pojos.WorkflowComputingUnit
import org.apache.texera.service.resource.ComputingUnitManagingResource.WorkflowComputingUnitMetrics
import org.apache.texera.service.resource.ComputingUnitState.{Pending, Running}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ComputingUnitHelpersSpec extends AnyFlatSpec with Matchers {

  private def localUnit(): WorkflowComputingUnit = {
    val unit = new WorkflowComputingUnit()
    unit.setType(WorkflowComputingUnitTypeEnum.local)
    unit
  }

  // WorkflowComputingUnitTypeEnum only defines `local` and `kubernetes`, so an
  // untyped unit (getType == null) is what exercises the pure "unknown" branch.
  private def untypedUnit(): WorkflowComputingUnit = new WorkflowComputingUnit()

  "getComputingUnitStatus" should "return Running for a local unit" in {
    ComputingUnitHelpers.getComputingUnitStatus(localUnit()) shouldBe Running
  }

  it should "return Pending for an unknown (untyped) unit" in {
    ComputingUnitHelpers.getComputingUnitStatus(untypedUnit()) shouldBe Pending
  }

  "getComputingUnitMetrics" should "return NaN metrics for a local unit" in {
    ComputingUnitHelpers.getComputingUnitMetrics(localUnit()) shouldBe
      WorkflowComputingUnitMetrics("NaN", "NaN")
  }

  it should "return NaN metrics for an unknown (untyped) unit" in {
    ComputingUnitHelpers.getComputingUnitMetrics(untypedUnit()) shouldBe
      WorkflowComputingUnitMetrics("NaN", "NaN")
  }
}
