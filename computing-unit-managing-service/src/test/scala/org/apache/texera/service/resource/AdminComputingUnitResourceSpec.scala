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

package org.apache.texera.service.resource

import jakarta.annotation.security.RolesAllowed
import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, WorkflowComputingUnitTypeEnum}
import org.apache.texera.dao.jooq.generated.tables.pojos.WorkflowComputingUnit
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

class AdminComputingUnitResourceSpec extends AnyFlatSpec with Matchers {

  private def localUnit(
      cuid: Int,
      uid: Int,
      name: String,
      terminated: Boolean = false
  ): WorkflowComputingUnit = {
    val unit = new WorkflowComputingUnit()
    unit.setCuid(cuid)
    unit.setUid(uid)
    unit.setName(name)
    unit.setType(WorkflowComputingUnitTypeEnum.local)
    if (terminated) unit.setTerminateTime(new Timestamp(0L))
    unit
  }

  // The class-level @RolesAllowed(Array("ADMIN")) is what makes Jersey's
  // RolesAllowedDynamicFeature return 403 for any non-ADMIN (e.g. REGULAR) caller.
  "AdminComputingUnitResource" should "only permit the ADMIN role (non-ADMIN callers are rejected)" in {
    val annotation = classOf[AdminComputingUnitResource].getAnnotation(classOf[RolesAllowed])
    annotation should not be null
    annotation.value.toSeq shouldBe Seq("ADMIN")
  }

  "buildDashboardUnits" should "return active units owned by multiple users, excluding terminated ones" in {
    val units = List(
      localUnit(cuid = 1, uid = 100, name = "alice-cu"),
      localUnit(cuid = 2, uid = 200, name = "bob-cu"),
      localUnit(cuid = 3, uid = 100, name = "alice-terminated", terminated = true)
    )
    val ownerInfo: Map[Integer, (String, String)] = Map(
      (100: Integer) -> ("alice-avatar", "alice"),
      (200: Integer) -> (null, "bob")
    )

    // caller is an admin who owns none of these units
    val result = AdminComputingUnitResource.buildDashboardUnits(units, ownerInfo, callerUid = 999)

    result.map(_.computingUnit.getCuid) should contain theSameElementsAs Seq(1, 2)
    result.map(_.computingUnit.getUid).distinct should contain theSameElementsAs Seq(100, 200)

    val byCuid = result.map(u => u.computingUnit.getCuid.intValue() -> u).toMap
    byCuid(1).ownerName shouldBe "alice"
    byCuid(1).ownerGoogleAvatar shouldBe "alice-avatar"
    byCuid(2).ownerName shouldBe "bob"
    byCuid(2).ownerGoogleAvatar shouldBe null

    // local units report Running/NaN and the admin caller does not own them
    all(result.map(_.status)) shouldBe "Running"
    all(result.map(_.isOwner)) shouldBe false
    all(result.map(_.accessPrivilege)) shouldBe PrivilegeEnum.WRITE
  }
}
