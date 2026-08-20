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

import java.net.{URI, URLEncoder}
import java.nio.charset.StandardCharsets
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

  // A second, deliberately different recorded URI. Routing tests that target the
  // read-only unit assert on this value so a Host header that ignored the looked-up
  // row (or hard-coded the other unit's host) would fail.
  private val testReadOnlyRecordedUri: String =
    "computing-unit-5.compute-unit-svc.default.svc.cluster.local:9999"

  // Recorded URI of the routable unit testUser1 has no privilege on. It exists so a
  // request refused by the privilege check cannot be confused with one refused for
  // being unroutable.
  private val testNoAccessRecordedUri: String =
    "computing-unit-6.compute-unit-svc.default.svc.cluster.local:7777"

  private val testUser1: User = {
    val user = new User()
    user.setUid(1)
    user.setName("testuser")
    user.setEmail("test@example.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  private val testUser2: User = {
    val user = new User()
    user.setUid(2)
    user.setName("testuser2")
    user.setEmail("test2@example.com")
    user.setRole(UserRoleEnum.REGULAR)
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

  // A routable computing unit on which testUser1 holds READ (not WRITE). Every other
  // fixture grants WRITE, so this is the only unit whose privilege header can tell a
  // real lookup apart from a hard-coded WRITE.
  private val testCUReadOnly: WorkflowComputingUnit = {
    val cu = new WorkflowComputingUnit()
    cu.setUid(2)
    cu.setType(WorkflowComputingUnitTypeEnum.kubernetes)
    cu.setCuid(5)
    cu.setName("test-cu-read-only")
    cu.setUri(testReadOnlyRecordedUri)
    cu
  }

  // A perfectly routable computing unit owned by testUser2 on which testUser1 is
  // granted nothing at all. It is the only fixture whose refusal can come from the
  // privilege check itself: every other refusal in this suite is produced either by
  // the unit not existing or by it having no recorded URI.
  private val testCUNoAccess: WorkflowComputingUnit = {
    val cu = new WorkflowComputingUnit()
    cu.setUid(testUser2.getUid)
    cu.setType(WorkflowComputingUnitTypeEnum.kubernetes)
    cu.setCuid(6)
    cu.setName("test-cu-no-access")
    cu.setUri(testNoAccessRecordedUri)
    cu
  }

  private var token: String = _

  // A token for the *other* user. Tests that must tell "the authenticated user" apart
  // from "user 1" need a second identity to compare against.
  private var token2: String = _

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
    computingUnitDao.insert(testCUReadOnly)
    // Deliberately inserted with NO computing_unit_user_access row for testUser1.
    computingUnitDao.insert(testCUNoAccess)

    // Grant testUser1 WRITE access to every test computing unit so the routing
    // logic (not the access check) is what each routing test exercises.
    Seq(testCU, testCUNoUri, testCUBlankUri).foreach { cu =>
      val cuAccess = new ComputingUnitUserAccess()
      cuAccess.setUid(testUser1.getUid)
      cuAccess.setCuid(cu.getCuid)
      cuAccess.setPrivilege(PrivilegeEnum.WRITE)
      computingUnitOfUserDao.insert(cuAccess)
    }

    val readOnlyAccess = new ComputingUnitUserAccess()
    readOnlyAccess.setUid(testUser1.getUid)
    readOnlyAccess.setCuid(testCUReadOnly.getCuid)
    readOnlyAccess.setPrivilege(PrivilegeEnum.READ)
    computingUnitOfUserDao.insert(readOnlyAccess)

    token = JwtAuth.jwtToken(JwtAuth.jwtClaims(testUser1))
    token2 = JwtAuth.jwtToken(JwtAuth.jwtClaims(testUser2))
  }

  override protected def afterAll(): Unit = {
    closeConnectionPool()
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

  "AccessControlResource" should "return FORBIDDEN for an unknown computing unit" in {
    // Mock the request context
    val mockUriInfo = mock(classOf[UriInfo])
    val mockHttpHeaders = mock(classOf[HttpHeaders])

    // cuid 1 is not in the fixture DB at all, so this exercises the "no such unit"
    // path. The privilege check itself is exercised by the no-privilege test below,
    // which uses a unit that exists and is routable.
    val queryParams = new MultivaluedHashMap[String, String]()
    queryParams.add("cuid", "1")

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

  it should "return FORBIDDEN when the user has no privilege on a routable computing unit" in {
    // testCUNoAccess exists, is owned by testUser2, and has a recorded URI — so the
    // routing check would happily let this through. The only thing that can refuse it
    // is the PrivilegeEnum.NONE guard.
    val (uri, headers) = mockRequest("/pve/system", Some(testCUNoAccess.getCuid.toString))
    val response = new AccessControlResource().authorizeGet(uri, headers)

    response.getStatus shouldBe Response.Status.FORBIDDEN.getStatusCode
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

  /**
    * Builds a mocked request context.
    *
    * @param authorizationHeader `None` models a client that sent no Authorization
    *                            header at all — JAX-RS returns `null` from
    *                            `getRequestHeader` in that case, which is what the
    *                            body-token tests need so the body is the only token
    *                            source left.
    * @param accessTokenQueryParam value for the `access-token` query parameter, when
    *                              the token travels in the URL instead.
    */
  private def mockRequest(
      path: String,
      cuidQueryParam: Option[String],
      authorizationHeader: Option[String] = Some("Bearer " + token),
      accessTokenQueryParam: Option[String] = None
  ): (UriInfo, HttpHeaders) = {
    val mockUriInfo = mock(classOf[UriInfo])
    val mockHttpHeaders = mock(classOf[HttpHeaders])

    val queryParams = new MultivaluedHashMap[String, String]()
    cuidQueryParam.foreach(queryParams.add("cuid", _))
    accessTokenQueryParam.foreach(queryParams.add("access-token", _))

    val requestHeaders = new MultivaluedHashMap[String, String]()

    when(mockUriInfo.getQueryParameters).thenReturn(queryParams)
    when(mockUriInfo.getRequestUri).thenReturn(new URI(testURI))
    when(mockUriInfo.getPath).thenReturn(path)

    authorizationHeader match {
      case Some(header) =>
        requestHeaders.add("Authorization", header)
        when(mockHttpHeaders.getRequestHeader("Authorization"))
          .thenReturn(util.Arrays.asList(header))
      case None =>
        // JAX-RS hands back null for a header the client never sent.
        when(mockHttpHeaders.getRequestHeader("Authorization"))
          .thenReturn(null.asInstanceOf[util.List[String]])
    }
    when(mockHttpHeaders.getRequestHeaders).thenReturn(requestHeaders)

    (mockUriInfo, mockHttpHeaders)
  }

  it should "return OK for /pve/system with cuid as query parameter" in {
    val (uri, headers) = mockRequest("/pve/system", Some(testCU.getCuid.toString))
    val response = new AccessControlResource().authorizeGet(uri, headers)

    response.getStatus shouldBe Response.Status.OK.getStatusCode
  }

  // The three path shapes below use the form Jersey actually hands to the resource:
  // UriInfo.getPath returns the path relative to the base URI, so a check for
  // /auth/api/pve/... arrives as "auth/api/pve/..." — no leading slash, and with the
  // "auth/" and "api/" segments still attached. That is exactly what the optional
  // `^/?(?:auth/)?(?:api/|wsapi/)?` prefix groups on each pve regex exist to absorb.
  it should "return OK for the gateway-relative form of a pve route" in {
    val (uri, headers) = mockRequest("auth/api/pve/system", Some(testCU.getCuid.toString))
    val response = new AccessControlResource().authorizeGet(uri, headers)

    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getHeaderString("Host") shouldBe testRecordedUri
  }

  it should "return OK for pve/pves/{cuid} (cuid extracted from path)" in {
    val (uri, headers) = mockRequest(s"auth/api/pve/pves/${testCU.getCuid}", None)
    val response = new AccessControlResource().authorizeDelete(uri, headers)

    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getHeaderString("Host") shouldBe testRecordedUri
  }

  it should "return OK for pve/{cuid}/{pveName}/packages/{packageName} (cuid extracted from path)" in {
    val (uri, headers) =
      mockRequest(s"auth/api/pve/${testCU.getCuid}/myenv/packages/numpy", None)
    val response = new AccessControlResource().authorizeDelete(uri, headers)

    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getHeaderString("Host") shouldBe testRecordedUri
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

  it should "report the privilege and the recorded URI of the requested computing unit" in {
    val (uri, headers) = mockRequest("/pve/system", Some(testCUReadOnly.getCuid.toString))
    val response = new AccessControlResource().authorizeGet(uri, headers)

    // testUser1 holds READ (not WRITE) on this unit, and this unit's recorded URI
    // differs from every other fixture's, so both headers are answered from the
    // row actually looked up rather than from a constant.
    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getHeaderString(
      HeaderField.UserComputingUnitAccess
    ) shouldBe PrivilegeEnum.READ.toString
    response.getHeaderString("Host") shouldBe testReadOnlyRecordedUri
  }

  it should "resolve the privilege and identity of the authenticated user, not a fixed one" in {
    // testUser2 owns testCUReadOnly, so the owner branch of the lookup gives it WRITE,
    // while testUser1 — the identity every other test authenticates as — only holds
    // READ on that same unit. Answering for the wrong user is therefore visible in
    // both the privilege header and the forwarded identity headers.
    val (uri, headers) = mockRequest(
      "/pve/system",
      Some(testCUReadOnly.getCuid.toString),
      authorizationHeader = Some("Bearer " + token2)
    )
    val response = new AccessControlResource().authorizeGet(uri, headers)

    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getHeaderString(
      HeaderField.UserComputingUnitAccess
    ) shouldBe PrivilegeEnum.WRITE.toString
    response.getHeaderString(HeaderField.UserId) shouldBe testUser2.getUid.toString
    response.getHeaderString(HeaderField.UserName) shouldBe testUser2.getName
    response.getHeaderString(HeaderField.UserEmail) shouldBe testUser2.getEmail
  }

  it should "fall back to the cuid in the path when the cuid query parameter is empty" in {
    // Envoy forwards `?cuid=` verbatim when the client sends the parameter with no
    // value; an empty parameter must not shadow the cuid embedded in the path.
    val (uri, headers) = mockRequest(s"/pve/pves/${testCUReadOnly.getCuid}", Some(""))
    val response = new AccessControlResource().authorizeDelete(uri, headers)

    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getHeaderString("Host") shouldBe testReadOnlyRecordedUri
  }

  it should "prefer the cuid query parameter over the cuid embedded in the path" in {
    // Path says unit 5 (READ, its own host), query says unit 2 (WRITE, another host):
    // both headers show which one the resolution actually used.
    val (uri, headers) =
      mockRequest(s"/pve/pves/${testCUReadOnly.getCuid}", Some(testCU.getCuid.toString))
    val response = new AccessControlResource().authorizeDelete(uri, headers)

    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getHeaderString(
      HeaderField.UserComputingUnitAccess
    ) shouldBe PrivilegeEnum.WRITE.toString
    response.getHeaderString("Host") shouldBe testRecordedUri
  }

  it should "authorize the /wsapi/workflow-websocket route using the access-token query parameter" in {
    // The Authorization header carries a token that cannot be parsed, so a 200 is
    // only reachable if the access-token query parameter is read AND preferred.
    val (uri, headers) = mockRequest(
      "/wsapi/workflow-websocket",
      Some(testCU.getCuid.toString),
      authorizationHeader = Some("Bearer not-a-real-jwt"),
      accessTokenQueryParam = Some(token)
    )
    val response = new AccessControlResource().authorizeGet(uri, headers)

    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getHeaderString("Host") shouldBe testRecordedUri
  }

  it should "authorize the /api/executions/result/export route" in {
    val (uri, headers) =
      mockRequest("/api/executions/result/export", Some(testCU.getCuid.toString))
    val response = new AccessControlResource().authorizeGet(uri, headers)

    response.getStatus shouldBe Response.Status.OK.getStatusCode
  }

  it should "ignore an empty access-token query parameter and fall back to the Authorization header" in {
    val (uri, headers) = mockRequest(
      "/pve/system",
      Some(testCU.getCuid.toString),
      accessTokenQueryParam = Some("")
    )
    val response = new AccessControlResource().authorizeGet(uri, headers)

    response.getStatus shouldBe Response.Status.OK.getStatusCode
  }

  it should "accept a lower-case bearer scheme" in {
    // RFC 7235 makes the auth scheme case-insensitive, so a client sending "bearer"
    // must not be turned away.
    val (uri, headers) = mockRequest(
      "/pve/system",
      Some(testCU.getCuid.toString),
      authorizationHeader = Some("bearer " + token)
    )
    val response = new AccessControlResource().authorizeGet(uri, headers)

    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getHeaderString("Host") shouldBe testRecordedUri
  }

  it should "accept an Authorization value that is a padded bare token" in {
    // The scheme is stripped only if present, so a client that sends the raw token is
    // still understood — and the surrounding whitespace has to be removed before the
    // JWT parser sees it. (Padding after a "Bearer " scheme would prove nothing: the
    // strip regex already eats the leading whitespace, and jose4j ignores trailing
    // whitespace because it lands in the base64url signature segment. Only a
    // scheme-less value can carry *leading* whitespace this far, and leading
    // whitespace corrupts the signing input.)
    val (uri, headers) = mockRequest(
      "/pve/system",
      Some(testCU.getCuid.toString),
      authorizationHeader = Some("  " + token + "  ")
    )
    val response = new AccessControlResource().authorizeGet(uri, headers)

    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getHeaderString("Host") shouldBe testRecordedUri
  }

  // ---------------------------------------------------------------------------
  // Token carried in the request body.
  //
  // Each of these sends no Authorization header and no access-token query
  // parameter, so the body is the only token source left: the 200 can only come
  // from the body-parsing branch under test.
  // ---------------------------------------------------------------------------

  private val multipartBoundary: String = "----TexeraTestBoundary"
  private val crlf: String = "\r\n"

  /**
    * A realistic two-part CRLF multipart document whose first part is named "token".
    *
    * Everything about its shape is load-bearing: the Content-Disposition line carries
    * a trailing attribute after `name="token"`, the value is padded with spaces, and a
    * second part follows before the closing boundary — so the capture has to stop at
    * the *next* boundary rather than running to the last one.
    */
  private def multipartTwoParts(value: String): String =
    s"--$multipartBoundary$crlf" +
      s"""Content-Disposition: form-data; name="token"; filename="t.txt"$crlf""" +
      crlf +
      s" $value $crlf" +
      s"--$multipartBoundary$crlf" +
      s"""Content-Disposition: form-data; name="other"$crlf""" +
      crlf +
      s"hello$crlf" +
      s"--$multipartBoundary--"

  /**
    * A single-part document that stops at the value, so only the run-to-end fallback
    * can recover the token. Deliberately built unlike [[multipartTwoParts]]: LF-only
    * line breaks (a client that does not send CRLF), whitespace around the `=` in the
    * Content-Disposition, a trailing attribute, and a padded value.
    */
  private def multipartRunToEnd(value: String): String =
    s"--$multipartBoundary\n" +
      "Content-Disposition: form-data; name = \"token\"; filename=\"t.txt\"\n" +
      "\n" +
      s" $value "

  private def bodyOnlyRequest(cuid: Int): (UriInfo, HttpHeaders) =
    mockRequest("/pve/system", Some(cuid.toString), authorizationHeader = None)

  it should "read the token out of an x-www-form-urlencoded body" in {
    // A single `token=<value>` pair — the shape a form encoder produces for a one-field
    // form, and the one body with no '&' in it at all.
    // Encoded the way a form encoder would: the surrounding spaces become '+', so
    // a reader that skipped URL-decoding would see "+<jwt>+" and reject it.
    val encoded = URLEncoder.encode(s" $token ", StandardCharsets.UTF_8.name())
    val (uri, headers) = bodyOnlyRequest(testCU.getCuid)

    val response = new AccessControlResource().authorizePost(uri, headers, s"token=$encoded")

    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getHeaderString("Host") shouldBe testRecordedUri
  }

  it should "keep scanning an urlencoded body past a valueless token pair" in {
    val (uri, headers) = bodyOnlyRequest(testCU.getCuid)

    // The leading "token" pair has no '=' and therefore no value; it must not be
    // accepted as the token, and the scan must continue to the real pair.
    val response =
      new AccessControlResource().authorizePost(uri, headers, s"token&other=1&token=$token")

    response.getStatus shouldBe Response.Status.OK.getStatusCode
  }

  it should "take the first token pair when an urlencoded body carries duplicates" in {
    val (uri, headers) = bodyOnlyRequest(testCU.getCuid)

    // Duplicate keys are not specified by the form-encoding spec, so the resolution
    // rule is a deliberate choice: the scan stops at the first usable value. Both
    // tokens here are valid and testUser2 owns this unit, so both orderings return
    // 200 — only the forwarded identity says which credential was selected.
    val response =
      new AccessControlResource().authorizePost(uri, headers, s"token=$token&token=$token2")

    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getHeaderString(HeaderField.UserId) shouldBe testUser1.getUid.toString
  }

  it should "read the token out of a JSON body" in {
    val (uri, headers) = bodyOnlyRequest(testCU.getCuid)

    // Jackson preserves the padding, so the value has to be trimmed before it reaches
    // the JWT parser.
    val response =
      new AccessControlResource().authorizePost(uri, headers, s"""{"token":" $token "}""")

    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getHeaderString("Host") shouldBe testRecordedUri
  }

  it should "read the token out of a multipart body terminated by a boundary" in {
    val (uri, headers) = bodyOnlyRequest(testCU.getCuid)

    val response =
      new AccessControlResource().authorizePost(uri, headers, multipartTwoParts(token))

    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getHeaderString("Host") shouldBe testRecordedUri
  }

  it should "read the token out of a multipart body that ends with the token" in {
    val (uri, headers) = bodyOnlyRequest(testCU.getCuid)

    // No closing boundary follows the value, so only the run-to-end fallback can
    // recover the token.
    val response =
      new AccessControlResource().authorizePost(uri, headers, multipartRunToEnd(token))

    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getHeaderString("Host") shouldBe testRecordedUri
  }

  it should "prefer the Authorization header over a token in the request body" in {
    // Header and body carry *different* valid identities. testUser2 owns this unit, so
    // either choice returns 200; only the forwarded identity reveals which token was
    // used to authenticate.
    val (uri, headers) = mockRequest("/pve/system", Some(testCU.getCuid.toString))
    val response =
      new AccessControlResource().authorizePost(uri, headers, s"token=$token2")

    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getHeaderString(HeaderField.UserId) shouldBe testUser1.getUid.toString
  }

  it should "fall back to the request body when the Authorization header is a bare bearer scheme" in {
    // "Bearer " with nothing after it leaves an empty token; it must be discarded so
    // the next source in the chain still gets a chance.
    val (uri, headers) = mockRequest(
      "/pve/system",
      Some(testCU.getCuid.toString),
      authorizationHeader = Some("Bearer ")
    )
    val response = new AccessControlResource().authorizePost(uri, headers, s"token=$token")

    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getHeaderString("Host") shouldBe testRecordedUri
  }
}
