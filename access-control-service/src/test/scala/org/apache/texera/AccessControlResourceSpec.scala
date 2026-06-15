// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.texera

import jakarta.ws.rs.core.{HttpHeaders, MultivaluedHashMap, Response, UriInfo}
import org.apache.texera.auth.JwtAuth
import org.apache.texera.auth.util.HeaderField
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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.net.URI
import java.util

class AccessControlResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private val testURI: String = "http://localhost:8080/"
  private val testPath: String = "/api/executions/1/stats/1"

  // The host:port the managing service records for a computing unit when it
  // creates the pod. The access-control-service routes to this recorded URI.
  private val testRecordedUri: String =
    "computing-unit-2.compute-unit-svc.default.svc.cluster.local:8888"

  private val testUser1: User = {
    val user = new User()
    user.setUid(1)
    user.setName("testuser")
    user.setEmail("test@example.com")
    user.setRole(UserRoleEnum.REGULAR)
    user.setPassword("password")
    user
  }

  private val testUser2: User = {
    val user = new User()
    user.setUid(2)
    user.setName("testuser2")
    user.setEmail("test2@example.com")
    user.setRole(UserRoleEnum.REGULAR)
    user.setPassword("password")
    user
  }

  private val testCU: WorkflowComputingUnit = {
    val cu = new WorkflowComputingUnit()
    cu.setUid(2)
    cu.setType(WorkflowComputingUnitTypeEnum.kubernetes)
    cu.setCuid(2)
    cu.setName("test-cu")
    cu.setUri(testRecordedUri)
    cu
  }

  // A computing unit the user can access but for which no URI was ever recorded
  // (e.g. the pod was never created). Such a unit is not routable and must be
  // refused.
  private val testCUNoUri: WorkflowComputingUnit = {
    val cu = new WorkflowComputingUnit()
    cu.setUid(2)
    cu.setType(WorkflowComputingUnitTypeEnum.kubernetes)
    cu.setCuid(3)
    cu.setName("test-cu-no-uri")
    cu
  }

  // A computing unit whose recorded URI is blank/whitespace-only — also treated
  // as "no URI recorded" and refused.
  private val testCUBlankUri: WorkflowComputingUnit = {
    val cu = new WorkflowComputingUnit()
    cu.setUid(2)
    cu.setType(WorkflowComputingUnitTypeEnum.kubernetes)
    cu.setCuid(4)
    cu.setName("test-cu-blank-uri")
    cu.setUri("   ")
    cu
  }

  private var token: String = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    val userDao = new UserDao(getDSLContext.configuration())
    val computingUnitDao = new WorkflowComputingUnitDao(getDSLContext.configuration())
    val computingUnitOfUserDao = new ComputingUnitUserAccessDao(getDSLContext.configuration())

    // insert user, computing unit, and access privilege into the mock database
    userDao.insert(testUser1)
    userDao.insert(testUser2)
    computingUnitDao.insert(testCU)
    computingUnitDao.insert(testCUNoUri)
    computingUnitDao.insert(testCUBlankUri)

    // Grant testUser1 WRITE access to every test computing unit so the routing
    // logic (not the access check) is what each routing test exercises.
    Seq(testCU, testCUNoUri, testCUBlankUri).foreach { cu =>
      val cuAccess = new ComputingUnitUserAccess()
      cuAccess.setUid(testUser1.getUid)
      cuAccess.setCuid(cu.getCuid)
      cuAccess.setPrivilege(PrivilegeEnum.WRITE)
      computingUnitOfUserDao.insert(cuAccess)
    }

    val claims = JwtAuth.jwtClaims(testUser1, 1)
    token = JwtAuth.jwtToken(claims)
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

  "AccessControlResource" should "return FORBIDDEN for a GET request without a token" in {
    val mockUriInfo = mock(classOf[UriInfo])
    val mockHttpHeaders = mock(classOf[HttpHeaders])
    val queryParams = new MultivaluedHashMap[String, String]()
    queryParams.add("cuid", "1")
    val requestHeaders = new MultivaluedHashMap[String, String]()

    when(mockUriInfo.getQueryParameters).thenReturn(queryParams)
    when(mockUriInfo.getRequestUri).thenReturn(new URI(testURI))
    when(mockUriInfo.getPath).thenReturn(testPath)
    when(mockHttpHeaders.getRequestHeaders).thenReturn(requestHeaders)
    when(mockHttpHeaders.getRequestHeader("Authorization")).thenReturn(new util.ArrayList[String]())

    val accessControlResource = new AccessControlResource()
    val response = accessControlResource.authorizeGet(mockUriInfo, mockHttpHeaders)

    response.getStatus shouldBe Response.Status.FORBIDDEN.getStatusCode
  }

  it should "return FORBIDDEN for a GET request with a non-integer cuid" in {
    val mockUriInfo = mock(classOf[UriInfo])
    val mockHttpHeaders = mock(classOf[HttpHeaders])
    val queryParams = new MultivaluedHashMap[String, String]()
    queryParams.add("cuid", "abc")
    val requestHeaders = new MultivaluedHashMap[String, String]()
    requestHeaders.add("Authorization", "Bearer dummy-token")

    when(mockUriInfo.getQueryParameters).thenReturn(queryParams)
    when(mockUriInfo.getRequestUri).thenReturn(new URI(testURI))
    when(mockUriInfo.getPath).thenReturn(testPath)
    when(mockHttpHeaders.getRequestHeaders).thenReturn(requestHeaders)
    when(mockHttpHeaders.getRequestHeader("Authorization"))
      .thenReturn(util.Arrays.asList("Bearer dummy-token"))

    val accessControlResource = new AccessControlResource()
    val response = accessControlResource.authorizeGet(mockUriInfo, mockHttpHeaders)

    response.getStatus shouldBe Response.Status.FORBIDDEN.getStatusCode
  }

  it should "return FORBIDDEN for a POST request without a token" in {
    val mockUriInfo = mock(classOf[UriInfo])
    val mockHttpHeaders = mock(classOf[HttpHeaders])
    val queryParams = new MultivaluedHashMap[String, String]()
    queryParams.add("cuid", "1")
    val requestHeaders = new MultivaluedHashMap[String, String]()

    when(mockUriInfo.getQueryParameters).thenReturn(queryParams)
    when(mockUriInfo.getRequestUri).thenReturn(new URI(testURI))
    when(mockUriInfo.getPath).thenReturn(testPath)
    when(mockHttpHeaders.getRequestHeaders).thenReturn(requestHeaders)
    when(mockHttpHeaders.getRequestHeader("Authorization")).thenReturn(new util.ArrayList[String]())

    val accessControlResource = new AccessControlResource()
    val response = accessControlResource.authorizePost(mockUriInfo, mockHttpHeaders, null)

    response.getStatus shouldBe Response.Status.FORBIDDEN.getStatusCode
  }

  "AccessControlResource" should "return FORBIDDEN when user does not have access to the computing unit" in {
    // Mock the request context
    val mockUriInfo = mock(classOf[UriInfo])
    val mockHttpHeaders = mock(classOf[HttpHeaders])

    // Prepare query parameters with a computing unit ID (cuid)
    val queryParams = new MultivaluedHashMap[String, String]()
    queryParams.add("cuid", "1") // Assuming user 1 does not have access to cuid 1

    // Prepare request headers with the generated JWT
    val requestHeaders = new MultivaluedHashMap[String, String]()
    requestHeaders.add("Authorization", "Bearer " + token)

    // Stub the mock objects to return the prepared data
    when(mockUriInfo.getQueryParameters).thenReturn(queryParams)
    when(mockUriInfo.getRequestUri).thenReturn(new URI(testURI))
    when(mockUriInfo.getPath).thenReturn(testPath)
    when(mockHttpHeaders.getRequestHeaders).thenReturn(requestHeaders)
    when(mockHttpHeaders.getRequestHeader("Authorization"))
      .thenReturn(util.Arrays.asList("Bearer " + token))

    // Instantiate the resource and call the method under test
    val accessControlResource = new AccessControlResource()
    val response = accessControlResource.authorizeGet(mockUriInfo, mockHttpHeaders)

    // Assert that the response status is FORBIDDEN
    response.getStatus shouldBe Response.Status.FORBIDDEN.getStatusCode
  }

  it should "return OK and correct headers when user has access" in {
    // Mock the request context
    val mockUriInfo = mock(classOf[UriInfo])
    val mockHttpHeaders = mock(classOf[HttpHeaders])

    // Prepare query parameters with a computing unit ID the user HAS access to
    val queryParams = new MultivaluedHashMap[String, String]()
    queryParams.add("cuid", testCU.getCuid.toString)

    // Prepare request headers with the generated JWT
    val requestHeaders = new MultivaluedHashMap[String, String]()
    requestHeaders.add("Authorization", "Bearer " + token)

    // Stub the mock objects to return the prepared data
    when(mockUriInfo.getQueryParameters).thenReturn(queryParams)
    when(mockUriInfo.getRequestUri).thenReturn(new URI(testURI))
    when(mockUriInfo.getPath).thenReturn(testPath)
    when(mockHttpHeaders.getRequestHeaders).thenReturn(requestHeaders)
    when(mockHttpHeaders.getRequestHeader("Authorization"))
      .thenReturn(util.Arrays.asList("Bearer " + token))

    // Instantiate the resource and call the method under test
    val accessControlResource = new AccessControlResource()
    val response = accessControlResource.authorizeGet(mockUriInfo, mockHttpHeaders)

    // Assert that the response status is OK and headers are correct
    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getHeaderString(
      HeaderField.UserComputingUnitAccess
    ) shouldBe PrivilegeEnum.WRITE.toString
    response.getHeaderString(HeaderField.UserId) shouldBe testUser1.getUid.toString
    response.getHeaderString(HeaderField.UserName) shouldBe testUser1.getName
    response.getHeaderString(HeaderField.UserEmail) shouldBe testUser1.getEmail
    // Envoy routes by the rewritten Host header, which must be the URI recorded
    // for the computing unit.
    response.getHeaderString("Host") shouldBe testRecordedUri
  }

  it should "refuse the connection when no URI is recorded for the computing unit" in {
    val (uri, headers) = mockRequest(testPath, Some(testCUNoUri.getCuid.toString))
    val response = new AccessControlResource().authorizeGet(uri, headers)

    response.getStatus shouldBe Response.Status.FORBIDDEN.getStatusCode
  }

  it should "refuse the connection when the recorded URI is blank" in {
    val (uri, headers) = mockRequest(testPath, Some(testCUBlankUri.getCuid.toString))
    val response = new AccessControlResource().authorizeGet(uri, headers)

    response.getStatus shouldBe Response.Status.FORBIDDEN.getStatusCode
  }

  private def mockRequest(
      path: String,
      cuidQueryParam: Option[String]
  ): (UriInfo, HttpHeaders) = {
    val mockUriInfo = mock(classOf[UriInfo])
    val mockHttpHeaders = mock(classOf[HttpHeaders])

    val queryParams = new MultivaluedHashMap[String, String]()
    cuidQueryParam.foreach(queryParams.add("cuid", _))

    val requestHeaders = new MultivaluedHashMap[String, String]()
    requestHeaders.add("Authorization", "Bearer " + token)

    when(mockUriInfo.getQueryParameters).thenReturn(queryParams)
    when(mockUriInfo.getRequestUri).thenReturn(new URI(testURI))
    when(mockUriInfo.getPath).thenReturn(path)
    when(mockHttpHeaders.getRequestHeaders).thenReturn(requestHeaders)
    when(mockHttpHeaders.getRequestHeader("Authorization"))
      .thenReturn(util.Arrays.asList("Bearer " + token))

    (mockUriInfo, mockHttpHeaders)
  }

  it should "return OK for /pve/system with cuid as query parameter" in {
    val (uri, headers) = mockRequest("/pve/system", Some(testCU.getCuid.toString))
    val response = new AccessControlResource().authorizeGet(uri, headers)

    response.getStatus shouldBe Response.Status.OK.getStatusCode
  }

  it should "return OK for /pve/pves/{cuid} (cuid extracted from path)" in {
    val (uri, headers) = mockRequest(s"/pve/pves/${testCU.getCuid}", None)
    val response = new AccessControlResource().authorizeDelete(uri, headers)

    response.getStatus shouldBe Response.Status.OK.getStatusCode
  }

  it should "return OK for /pve/{cuid}/{pveName}/packages/{packageName} (cuid extracted from path)" in {
    val (uri, headers) = mockRequest(s"/pve/${testCU.getCuid}/myenv/packages/numpy", None)
    val response = new AccessControlResource().authorizeDelete(uri, headers)

    response.getStatus shouldBe Response.Status.OK.getStatusCode
  }

  it should "return FORBIDDEN for a PVE path with no cuid in query or path" in {
    val (uri, headers) = mockRequest("/pve/no-cuid-anywhere", None)
    val response = new AccessControlResource().authorizeGet(uri, headers)

    response.getStatus shouldBe Response.Status.FORBIDDEN.getStatusCode
  }

  it should "return FORBIDDEN for a non-PVE / non-whitelisted path" in {
    val (uri, headers) = mockRequest("/random/garbage", Some(testCU.getCuid.toString))
    val response = new AccessControlResource().authorizeGet(uri, headers)

    response.getStatus shouldBe Response.Status.FORBIDDEN.getStatusCode
  }

  it should "return OK for a PUT request when user has access" in {
    val (uri, headers) = mockRequest("/pve/system", Some(testCU.getCuid.toString))
    val response = new AccessControlResource().authorizePut(uri, headers, """{"name":"env"}""")

    response.getStatus shouldBe Response.Status.OK.getStatusCode
  }
}
