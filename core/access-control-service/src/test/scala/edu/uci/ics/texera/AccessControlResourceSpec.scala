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

package edu.uci.ics.texera

import edu.uci.ics.texera.auth.JwtAuth
import edu.uci.ics.texera.auth.util.HeaderField
import edu.uci.ics.texera.dao.MockTexeraDB
import edu.uci.ics.texera.dao.jooq.generated.enums.{PrivilegeEnum, UserRoleEnum, WorkflowComputingUnitTypeEnum}
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.{ComputingUnitUserAccessDao, UserDao, WorkflowComputingUnitDao}
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.{ComputingUnitUserAccess, User, WorkflowComputingUnit}
import edu.uci.ics.texera.service.resource.AccessControlResource
import jakarta.ws.rs.core.{HttpHeaders, MultivaluedHashMap, Response, UriInfo}
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.net.URI
import java.util

class AccessControlResourceSpec extends AnyFlatSpec
  with Matchers
  with BeforeAndAfterAll
  with BeforeAndAfterEach
  with MockTexeraDB {

  private val testURI: String = "http://localhost:8080/"
  private val testPath: String = "/api/executions/1/stats/1"

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

    val cuAccess = new ComputingUnitUserAccess()
    cuAccess.setUid(testUser1.getUid)
    cuAccess.setCuid(testCU.getCuid)
    cuAccess.setPrivilege(PrivilegeEnum.WRITE)
    computingUnitOfUserDao.insert(cuAccess)

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
    when(mockHttpHeaders.getRequestHeader("Authorization")).thenReturn(util.Arrays.asList("Bearer dummy-token"))

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
    val response = accessControlResource.authorizePost(mockUriInfo, mockHttpHeaders)

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
    when(mockHttpHeaders.getRequestHeader("Authorization")).thenReturn(util.Arrays.asList("Bearer " + token))

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
    when(mockHttpHeaders.getRequestHeader("Authorization")).thenReturn(util.Arrays.asList("Bearer " + token))

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
  }
}