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

import jakarta.ws.rs.ForbiddenException
import org.apache.texera.auth.SessionUser
import org.apache.texera.common.config.ComputingUnitConfig
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Spec for [[ComputingUnitAccessResource]] when the sharing feature is DISABLED.
  *
  * `ComputingUnitConfig.sharingComputingUnitEnabled` is a load-time val resolved from the
  * COMPUTING_UNIT_SHARING_ENABLED env var, so the disabled branch must be exercised in a
  * forked JVM where that var resolves to false (build.sbt's `Test / testGrouping` isolates any
  * suite whose name ends with "SharingDisabledSpec" and forces the flag off there).
  * The first assertion guards that grouping — if it ever stops applying, this suite fails loudly
  * rather than silently passing against a sharing-enabled JVM.
  */
class ComputingUnitAccessSharingDisabledSpec
    extends AnyFlatSpec
    with Matchers
    with MockTexeraDB
    with BeforeAndAfterAll {

  private val user: User = {
    val u = new User
    u.setName("cu_user")
    u.setPassword("123")
    u.setEmail("cu_user@test.com")
    u.setRole(UserRoleEnum.REGULAR)
    u
  }

  // Access resource construction needs an initialized SqlServer; the DB is otherwise unused
  // here because ensureSharingIsEnabled() throws before any query runs.
  lazy val accessResource = new ComputingUnitAccessResource()
  lazy val session = new SessionUser(user)

  private val cuid: Integer = 1

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    initializeDBAndReplaceDSLContext()
  }

  override protected def afterAll(): Unit = {
    try shutdownDB()
    finally super.afterAll()
  }

  private def expectForbidden(call: => Any): Unit = {
    val ex = intercept[ForbiddenException](call)
    ex.getResponse.getStatus shouldEqual 403
    ex.getMessage should include("sharing feature is disabled")
  }

  "the test environment" should "have computing-unit sharing disabled" in {
    ComputingUnitConfig.sharingComputingUnitEnabled shouldBe false
  }

  "grantAccess" should "be forbidden (403) when sharing is disabled" in {
    expectForbidden(accessResource.grantAccess(session, cuid, user.getEmail, PrivilegeEnum.READ))
  }

  "revokeAccess" should "be forbidden (403) when sharing is disabled" in {
    expectForbidden(accessResource.revokeAccess(session, cuid, user.getEmail))
  }

  "getComputingUnitAccessList" should "be forbidden (403) when sharing is disabled" in {
    expectForbidden(accessResource.getComputingUnitAccessList(session, cuid))
  }

  "getOwner" should "be forbidden (403) when sharing is disabled" in {
    expectForbidden(accessResource.getOwner(session, cuid))
  }
}
