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
  UserJupyterDao,
  ComputingUnitUserAccessDao,
  UserDao,
  WorkflowComputingUnitDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  UserJupyter,
  ComputingUnitUserAccess,
  User,
  WorkflowComputingUnit
}
import org.apache.texera.service.resource.AccessControlResource
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import java.net.{URI, URLEncoder}
import java.nio.charset.StandardCharsets
import java.util

class AccessControlResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
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

  // What the provisioner records for a user's Jupyter: scheme, authority and the base path
  // the pod serves under. Only the authority may reach Envoy as a Host header.
  private val testJupyterInternalUrl: String =
    "http://jupyter-1.jupyter-svc.texera-jupyter-pool.svc.cluster.local:8888/jupyter/1"

  /** A regular user with the given id, name and email. */
  private def makeUser(uid: Int, name: String, email: String): User = {
    val user = new User
    user.setUid(uid)
    user.setName(name)
    user.setEmail(email)
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  private val testUser1 = makeUser(1, "testuser", "test@example.com")
  private val testUser2 = makeUser(2, "testuser2", "test2@example.com")

  /**
    * A Kubernetes computing unit, routable when `uri` is given. testUser2 owns every unit:
    * the cases that check the owner's WRITE access and the duplicate-token case rely on it.
    */
  private def makeCU(cuid: Int, name: String, uri: Option[String] = None): WorkflowComputingUnit = {
    val cu = new WorkflowComputingUnit
    cu.setUid(testUser2.getUid)
    cu.setType(WorkflowComputingUnitTypeEnum.kubernetes)
    cu.setCuid(cuid)
    cu.setName(name)
    uri.foreach(cu.setUri)
    cu
  }

  private val testCU = makeCU(2, "test-cu", Some(testRecordedUri))

  // A computing unit the user can access but for which no URI was ever recorded
  // (e.g. the pod was never created). Such a unit is not routable and must be
  // refused.
  private val testCUNoUri = makeCU(3, "test-cu-no-uri")

  // A computing unit whose recorded URI is blank/whitespace-only — also treated
  // as "no URI recorded" and refused.
  private val testCUBlankUri = makeCU(4, "test-cu-blank-uri", Some("   "))

  // A routable computing unit on which testUser1 holds READ (not WRITE). Every other
  // fixture grants WRITE, so this is the only unit whose privilege header can tell a
  // real lookup apart from a hard-coded WRITE.
  private val testCUReadOnly = makeCU(5, "test-cu-read-only", Some(testReadOnlyRecordedUri))

  // A perfectly routable computing unit owned by testUser2 on which testUser1 is
  // granted nothing at all. It is the only fixture whose refusal can come from the
  // privilege check itself: every other refusal in this suite is produced either by
  // the unit not existing or by it having no recorded URI.
  private val testCUNoAccess = makeCU(6, "test-cu-no-access", Some(testNoAccessRecordedUri))

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

    // Per-user Jupyter: user 1 has one registered, user 2 deliberately does not.
    val jupyterDao = new UserJupyterDao(getDSLContext.configuration())
    val jupyter = new UserJupyter()
    jupyter.setUid(testUser1.getUid)
    jupyter.setInternalUrl(testJupyterInternalUrl)
    jupyter.setPublicUrl("https://texera.example.com/jupyter/1")
    jupyterDao.insert(jupyter)

    token = JwtAuth.jwtToken(JwtAuth.jwtClaims(testUser1))
    token2 = JwtAuth.jwtToken(JwtAuth.jwtClaims(testUser2))
  }

  override protected def afterAll(): Unit = {
    closeConnectionPool()
  }

  /** A mocked request context; each method sends it to a fresh resource. */
  private class Request(val uri: UriInfo, val headers: HttpHeaders) {
    def get(): Response = new AccessControlResource().authorizeGet(uri, headers)
    def post(body: String): Response = new AccessControlResource().authorizePost(uri, headers, body)
    def put(body: String): Response = new AccessControlResource().authorizePut(uri, headers, body)
    def delete(): Response = new AccessControlResource().authorizeDelete(uri, headers)
  }

  /** Asserts that the request was refused. */
  private def assertForbidden(response: Response): Unit =
    response.getStatus shouldBe Response.Status.FORBIDDEN.getStatusCode

  /** Asserts that the request was let through and routed to testCU's recorded URI. */
  private def assertOkHost(response: Response): Unit = {
    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getHeaderString("Host") shouldBe testRecordedUri
  }

  "AccessControlResource" should "return FORBIDDEN for a GET request without a token" in {
    val request = mockRequest(testPath, Some("1"), authorizationHeader = None)
    when(request.headers.getRequestHeader("Authorization")).thenReturn(new util.ArrayList[String]())
    assertForbidden(request.get())
  }

  it should "return FORBIDDEN for a GET request with a non-integer cuid" in {
    assertForbidden(mockRequest(testPath, Some("abc"), Some("Bearer dummy-token")).get())
  }

  it should "return FORBIDDEN for a POST request without a token" in {
    val request = mockRequest(testPath, Some("1"), authorizationHeader = None)
    when(request.headers.getRequestHeader("Authorization")).thenReturn(new util.ArrayList[String]())
    assertForbidden(request.post(null))
  }

  "AccessControlResource" should "return FORBIDDEN for an unknown computing unit" in {
    // cuid 1 is not in the fixture DB at all, so this exercises the "no such unit"
    // path. The privilege check itself is exercised by the no-privilege test below,
    // which uses a unit that exists and is routable.
    assertForbidden(mockRequest(testPath, Some("1")).get())
  }

  it should "return OK and correct headers when user has access" in {
    val response = mockRequest(testPath, Some(testCU.getCuid.toString)).get()

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
    assertForbidden(mockRequest("/pve/system", Some(testCUNoAccess.getCuid.toString)).get())
  }

  it should "refuse the connection when no URI is recorded for the computing unit" in {
    assertForbidden(mockRequest(testPath, Some(testCUNoUri.getCuid.toString)).get())
  }

  it should "refuse the connection when the recorded URI is blank" in {
    assertForbidden(mockRequest(testPath, Some(testCUBlankUri.getCuid.toString)).get())
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
  ): Request = {
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

    new Request(mockUriInfo, mockHttpHeaders)
  }

  it should "return OK for /pve/system with cuid as query parameter" in {
    val response = mockRequest("/pve/system", Some(testCU.getCuid.toString)).get()

    response.getStatus shouldBe Response.Status.OK.getStatusCode
  }

  // The three path shapes below use the form Jersey actually hands to the resource:
  // UriInfo.getPath returns the path relative to the base URI, so a check for
  // /auth/api/pve/... arrives as "auth/api/pve/..." — no leading slash, and with the
  // "auth/" and "api/" segments still attached. That is exactly what the optional
  // `^/?(?:auth/)?(?:api/|wsapi/)?` prefix groups on each pve regex exist to absorb.
  it should "return OK for the gateway-relative form of a pve route" in {
    assertOkHost(mockRequest("auth/api/pve/system", Some(testCU.getCuid.toString)).get())
  }

  it should "return OK for pve/pves/{cuid} (cuid extracted from path)" in {
    assertOkHost(mockRequest(s"auth/api/pve/pves/${testCU.getCuid}", None).delete())
  }

  it should "return OK for pve/{cuid}/{pveName}/packages/{packageName} (cuid extracted from path)" in {
    assertOkHost(mockRequest(s"auth/api/pve/${testCU.getCuid}/myenv/packages/numpy", None).delete())
  }

  it should "return FORBIDDEN for a PVE path with no cuid in query or path" in {
    assertForbidden(mockRequest("/pve/no-cuid-anywhere", None).get())
  }

  it should "return FORBIDDEN for a non-PVE / non-whitelisted path" in {
    assertForbidden(mockRequest("/random/garbage", Some(testCU.getCuid.toString)).get())
  }

  it should "return OK for a PUT request when user has access" in {
    val response =
      mockRequest("/pve/system", Some(testCU.getCuid.toString)).put("""{"name":"env"}""")

    response.getStatus shouldBe Response.Status.OK.getStatusCode
  }

  it should "report the privilege and the recorded URI of the requested computing unit" in {
    val response = mockRequest("/pve/system", Some(testCUReadOnly.getCuid.toString)).get()

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
    val response = mockRequest(
      "/pve/system",
      Some(testCUReadOnly.getCuid.toString),
      authorizationHeader = Some("Bearer " + token2)
    ).get()

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
    val response = mockRequest(s"/pve/pves/${testCUReadOnly.getCuid}", Some("")).delete()

    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getHeaderString("Host") shouldBe testReadOnlyRecordedUri
  }

  it should "prefer the cuid query parameter over the cuid embedded in the path" in {
    // Path says unit 5 (READ, its own host), query says unit 2 (WRITE, another host):
    // both headers show which one the resolution actually used.
    val response =
      mockRequest(s"/pve/pves/${testCUReadOnly.getCuid}", Some(testCU.getCuid.toString)).delete()

    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getHeaderString(
      HeaderField.UserComputingUnitAccess
    ) shouldBe PrivilegeEnum.WRITE.toString
    response.getHeaderString("Host") shouldBe testRecordedUri
  }

  it should "authorize the /wsapi/workflow-websocket route using the access-token query parameter" in {
    // The Authorization header carries a token that cannot be parsed, so a 200 is
    // only reachable if the access-token query parameter is read AND preferred.
    val response = mockRequest(
      "/wsapi/workflow-websocket",
      Some(testCU.getCuid.toString),
      authorizationHeader = Some("Bearer not-a-real-jwt"),
      accessTokenQueryParam = Some(token)
    ).get()

    assertOkHost(response)
  }

  it should "authorize the /api/executions/result/export route" in {
    val response = mockRequest("/api/executions/result/export", Some(testCU.getCuid.toString)).get()

    response.getStatus shouldBe Response.Status.OK.getStatusCode
  }

  it should "ignore an empty access-token query parameter and fall back to the Authorization header" in {
    val response = mockRequest(
      "/pve/system",
      Some(testCU.getCuid.toString),
      accessTokenQueryParam = Some("")
    ).get()

    response.getStatus shouldBe Response.Status.OK.getStatusCode
  }

  it should "accept a lower-case bearer scheme" in {
    // RFC 7235 makes the auth scheme case-insensitive, so a client sending "bearer"
    // must not be turned away.
    val response = mockRequest(
      "/pve/system",
      Some(testCU.getCuid.toString),
      authorizationHeader = Some("bearer " + token)
    ).get()

    assertOkHost(response)
  }

  it should "accept an Authorization value that is a padded bare token" in {
    // The scheme is stripped only if present, so a client that sends the raw token is
    // still understood — and the surrounding whitespace has to be removed before the
    // JWT parser sees it. (Padding after a "Bearer " scheme would prove nothing: the
    // strip regex already eats the leading whitespace, and jose4j ignores trailing
    // whitespace because it lands in the base64url signature segment. Only a
    // scheme-less value can carry *leading* whitespace this far, and leading
    // whitespace corrupts the signing input.)
    val response = mockRequest(
      "/pve/system",
      Some(testCU.getCuid.toString),
      authorizationHeader = Some("  " + token + "  ")
    ).get()

    assertOkHost(response)
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

  private def bodyOnlyRequest(cuid: Int): Request =
    mockRequest("/pve/system", Some(cuid.toString), authorizationHeader = None)

  it should "read the token out of an x-www-form-urlencoded body" in {
    // A single `token=<value>` pair — the shape a form encoder produces for a one-field
    // form, and the one body with no '&' in it at all.
    // Encoded the way a form encoder would: the surrounding spaces become '+', so
    // a reader that skipped URL-decoding would see "+<jwt>+" and reject it.
    val encoded = URLEncoder.encode(s" $token ", StandardCharsets.UTF_8.name())

    assertOkHost(bodyOnlyRequest(testCU.getCuid).post(s"token=$encoded"))
  }

  it should "keep scanning an urlencoded body past a valueless token pair" in {
    // The leading "token" pair has no '=' and therefore no value; it must not be
    // accepted as the token, and the scan must continue to the real pair.
    val response = bodyOnlyRequest(testCU.getCuid).post(s"token&other=1&token=$token")

    response.getStatus shouldBe Response.Status.OK.getStatusCode
  }

  it should "take the first token pair when an urlencoded body carries duplicates" in {
    // Duplicate keys are not specified by the form-encoding spec, so the resolution
    // rule is a deliberate choice: the scan stops at the first usable value. Both
    // tokens here are valid and testUser2 owns this unit, so both orderings return
    // 200 — only the forwarded identity says which credential was selected.
    val response = bodyOnlyRequest(testCU.getCuid).post(s"token=$token&token=$token2")

    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getHeaderString(HeaderField.UserId) shouldBe testUser1.getUid.toString
  }

  it should "read the token out of a JSON body" in {
    // Jackson preserves the padding, so the value has to be trimmed before it reaches
    // the JWT parser.
    assertOkHost(bodyOnlyRequest(testCU.getCuid).post(s"""{"token":" $token "}"""))
  }

  it should "read the token out of a multipart body terminated by a boundary" in {
    assertOkHost(bodyOnlyRequest(testCU.getCuid).post(multipartTwoParts(token)))
  }

  it should "read the token out of a multipart body that ends with the token" in {
    // No closing boundary follows the value, so only the run-to-end fallback can
    // recover the token.
    assertOkHost(bodyOnlyRequest(testCU.getCuid).post(multipartRunToEnd(token)))
  }

  it should "prefer the Authorization header over a token in the request body" in {
    // Header and body carry *different* valid identities. testUser2 owns this unit, so
    // either choice returns 200; only the forwarded identity reveals which token was
    // used to authenticate.
    val response = mockRequest("/pve/system", Some(testCU.getCuid.toString)).post(s"token=$token2")

    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getHeaderString(HeaderField.UserId) shouldBe testUser1.getUid.toString
  }

  it should "fall back to the request body when the Authorization header is a bare bearer scheme" in {
    // "Bearer " with nothing after it leaves an empty token; it must be discarded so
    // the next source in the chain still gets a chance.
    val response = mockRequest(
      "/pve/system",
      Some(testCU.getCuid.toString),
      authorizationHeader = Some("Bearer ")
    ).post(s"token=$token")

    assertOkHost(response)
  }

  // -- per-user JupyterLab routing --------------------------------------------

  it should "route a Jupyter request to the pod recorded for the uid in the path" in {
    val response = mockRequest("/jupyter/1/notebooks/work/notebook.ipynb", None).get()

    response.getStatus shouldBe Response.Status.OK.getStatusCode
    // The scheme and base path are stripped: Envoy routes on an authority alone.
    response.getHeaderString("Host") shouldBe
      "jupyter-1.jupyter-svc.texera-jupyter-pool.svc.cluster.local:8888"
  }

  it should "route Jupyter's own subrequests, which carry no token" in {
    // The iframe's asset and API calls cannot present Texera credentials, so routing has to
    // work without one. The per-user Jupyter token is what authorizes them.
    val response = mockRequest("/jupyter/1/api/contents", None, authorizationHeader = None).get()

    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getHeaderString("Host") should startWith("jupyter-1.")
  }

  it should "refuse a Jupyter request for a user with none registered" in {
    assertForbidden(mockRequest("/jupyter/2/tree", None).get())
  }

  it should "refuse a Jupyter request for a uid that does not exist" in {
    assertForbidden(mockRequest("/jupyter/999999/tree", None).get())
  }

  it should "not treat a Jupyter path without a uid as routable" in {
    // Falls through to the catch-all, which denies.
    assertForbidden(mockRequest("/jupyter/tree", None).get())
  }

  it should "route the gateway-relative form of a Jupyter path" in {
    mockRequest("auth/jupyter/1/tree", None)
      .get()
      .getStatus shouldBe Response.Status.OK.getStatusCode
  }

  it should "refuse a Jupyter uid too large to be a user id" in {
    // The path regex accepts any run of digits, so the conversion to Int has to be caught
    // rather than allowed out as a 500.
    assertForbidden(mockRequest("/jupyter/9999999999/tree", None).get())
  }

  // Registers an address for testUser2, who otherwise has none, for the rows that should be
  // refused rather than routed.
  private def withRecordedJupyter(internalUrl: String)(check: => Unit): Unit = {
    val jupyterDao = new UserJupyterDao(getDSLContext.configuration())
    val row = new UserJupyter()
    row.setUid(testUser2.getUid)
    row.setInternalUrl(internalUrl)
    row.setPublicUrl(internalUrl)
    jupyterDao.insert(row)
    try check
    finally jupyterDao.deleteById(testUser2.getUid)
  }

  it should "refuse a Jupyter request whose recorded address has no authority" in {
    // Parses cleanly but yields a null authority, which must not become a Host header of
    // "null" for Envoy to route on.
    withRecordedJupyter("jupyter-2-with-no-scheme:8888") {
      assertForbidden(mockRequest(s"/jupyter/${testUser2.getUid}/tree", None).get())
    }
  }

  it should "build the uid regex from the configured base path" in {
    val custom = AccessControlResource.jupyterPathRegex("/lab/notebooks")
    "/lab/notebooks/7/tree" should fullyMatch regex custom
    "auth/lab/notebooks/7" should fullyMatch regex custom
    "/jupyter/7/tree" should not(fullyMatch regex custom)
  }

  it should "tolerate a base path written with surrounding slashes" in {
    val slashed = AccessControlResource.jupyterPathRegex("/jupyter/")
    "/jupyter/7/tree" should fullyMatch regex slashed
  }

  it should "treat the base path as a path, not a pattern" in {
    // Unquoted, the dot would match any character and route /axb to the /a.b pool.
    val dotted = AccessControlResource.jupyterPathRegex("/a.b")
    "/a.b/7" should fullyMatch regex dotted
    "/axb/7" should not(fullyMatch regex dotted)
  }

  it should "refuse a Jupyter request whose recorded address will not parse" in {
    // A stray escape throws from the URI constructor. That parse sits inside the same guard
    // as the lookup, so this is denied like any other unusable row instead of raising a 500.
    withRecordedJupyter("http://jupyter-2:8888/%zz") {
      assertForbidden(mockRequest(s"/jupyter/${testUser2.getUid}/tree", None).get())
    }
  }
}
