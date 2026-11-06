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

package org.apache.texera

import jakarta.ws.rs.core.{HttpHeaders, MultivaluedHashMap, Response, UriInfo}
import org.apache.texera.auth.JwtAuth
import org.apache.texera.auth.util.HeaderField
import org.apache.texera.common.config.ComputingUnitConfig
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.{
  PrivilegeEnum,
  UserRoleEnum,
  WorkflowComputingUnitTypeEnum
}
import org.apache.texera.dao.jooq.generated.tables.daos.{
  ComputingUnitUserAccessDao,
  UserDao,
  WorkflowComputingUnitDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  ComputingUnitUserAccess,
  User,
  WorkflowComputingUnit
}
import org.apache.texera.service.resource.AccessControlResource
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URI
import java.util

/**
  * The gateway with computing-unit sharing DISABLED. build.sbt runs every suite named
  * "*SharingDisabledSpec" in its own JVM without COMPUTING_UNIT_SHARING_ENABLED; the first case
  * guards that, so this suite fails loudly instead of passing against a sharing-enabled JVM.
  */
class AccessControlSharingDisabledSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with MockTexeraDB {

  private def makeUser(uid: Int, name: String): User = {
    val user = new User()
    user.setUid(uid)
    user.setName(name)
    user.setEmail(s"$name@example.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  private val owner = makeUser(2, "owner")
  private val grantee = makeUser(1, "grantee")

  // Shared with WRITE before sharing was turned off: the grant row stays, but must not work.
  private val sharedCU: WorkflowComputingUnit = {
    val cu = new WorkflowComputingUnit()
    cu.setUid(owner.getUid)
    cu.setType(WorkflowComputingUnitTypeEnum.kubernetes)
    cu.setCuid(2)
    cu.setName("shared-cu")
    cu.setUri("computing-unit-2.compute-unit-svc.default.svc.cluster.local:8888")
    cu
  }

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    val config = getDSLContext.configuration()
    new UserDao(config).insert(owner)
    new UserDao(config).insert(grantee)
    new WorkflowComputingUnitDao(config).insert(sharedCU)
    new ComputingUnitUserAccessDao(config)
      .insert(new ComputingUnitUserAccess(sharedCU.getCuid, grantee.getUid, PrivilegeEnum.WRITE))
  }

  override protected def afterAll(): Unit = closeConnectionPool()

  private def authorize(user: User): Response = {
    val token = JwtAuth.jwtToken(JwtAuth.jwtClaims(user))
    val uriInfo = mock(classOf[UriInfo])
    val headers = mock(classOf[HttpHeaders])
    val queryParams = new MultivaluedHashMap[String, String]()
    queryParams.add("cuid", sharedCU.getCuid.toString)
    val requestHeaders = new MultivaluedHashMap[String, String]()
    requestHeaders.add("Authorization", "Bearer " + token)
    when(uriInfo.getQueryParameters).thenReturn(queryParams)
    when(uriInfo.getRequestUri).thenReturn(new URI("http://localhost:8080/"))
    when(uriInfo.getPath).thenReturn("/wsapi/workflow-websocket")
    when(headers.getRequestHeaders).thenReturn(requestHeaders)
    when(headers.getRequestHeader("Authorization"))
      .thenReturn(util.Arrays.asList("Bearer " + token))
    new AccessControlResource().authorizeGet(uriInfo, headers)
  }

  "the test environment" should "have computing-unit sharing disabled" in {
    ComputingUnitConfig.sharingComputingUnitEnabled shouldBe false
  }

  "AccessControlResource" should "refuse a grantee while sharing is disabled" in {
    authorize(grantee).getStatus shouldBe Response.Status.FORBIDDEN.getStatusCode
  }

  it should "still let the owner through with WRITE" in {
    val response = authorize(owner)
    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getHeaderString(HeaderField.UserComputingUnitAccess) shouldBe
      PrivilegeEnum.WRITE.toString
  }
}
