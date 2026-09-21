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

package org.apache.texera.web

import ch.qos.logback.classic.{Level, Logger => LogbackLogger}
import org.apache.texera.auth.JwtAuth
import org.apache.texera.auth.util.HeaderField
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.jose4j.jwt.JwtClaims
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import javax.websocket.server.{HandshakeRequest, ServerEndpointConfig}
import scala.jdk.CollectionConverters._

/**
  * Unit tests for the websocket handshake configurator, which is the single place a websocket
  * connection acquires its identity. It runs in two mutually exclusive modes:
  *
  *   - KUBERNETES, selected when all four `x-user-*` headers are present. Envoy is trusted to have
  *     authenticated the caller, so the user is built straight from header values.
  *   - SINGLE NODE otherwise, where the identity comes from an `access-token` JWT in the query
  *     string and the computing-unit privilege is hard-coded to WRITE.
  *
  * Both modes write into `ServerEndpointConfig.getUserProperties`, which is the map
  * `WorkflowWebsocketResource.myOnOpen`/`myOnMsg` later read. Every assertion below therefore reads
  * values back OUT of that map rather than trusting an intermediate.
  *
  * Tokens are minted with the production `JwtAuth.jwtToken`, so they verify against the very
  * consumer the configurator uses, whatever `auth.jwt.256-bit-secret` resolves to in this JVM (the
  * same trick `JwtParserSpec` uses). No socket, container, database or filesystem is involved.
  *
  * Deliberately not asserted: the exact wording of the info/debug log lines. They are not a
  * contract, and pinning them would break on any rewording. (Worth reporting rather than testing:
  * the info line writes the user's e-mail address into the log.)
  */
class ServletAwareConfiguratorSpec extends AnyFlatSpec with Matchers with MockFactory {

  private val userId = "77"
  private val userName = "k8s-subject"
  private val userEmail = "k8s-mail@example.com"

  /** All four headers the kubernetes branch requires, each value distinct from the others. */
  private val k8sHeaders: Map[String, Seq[String]] = Map(
    // READ, never WRITE: WRITE is what the single-node branch hard-codes, so a test that fed WRITE
    // here could not tell the two branches apart.
    HeaderField.UserComputingUnitAccess -> Seq(PrivilegeEnum.READ.name()),
    HeaderField.UserId -> Seq(userId),
    HeaderField.UserName -> Seq(userName),
    HeaderField.UserEmail -> Seq(userEmail)
  )

  private def handshake(
      headers: Map[String, Seq[String]] = Map.empty,
      queryString: String = ""
  ): (ServerEndpointConfig, HandshakeRequest, java.util.Map[String, AnyRef]) = {
    val javaHeaders = headers.map { case (name, values) => name -> values.asJava }.asJava
    val request = mock[HandshakeRequest]
    (() => request.getHeaders).expects().returning(javaHeaders).anyNumberOfTimes()
    (() => request.getQueryString).expects().returning(queryString).anyNumberOfTimes()

    val properties = new java.util.HashMap[String, AnyRef]()
    val config = mock[ServerEndpointConfig]
    (() => config.getUserProperties).expects().returning(properties).anyNumberOfTimes()

    (config, request, properties)
  }

  private def userIn(properties: java.util.Map[String, AnyRef]): User =
    properties.get(classOf[User].getName).asInstanceOf[User]

  private def tokenFor(subject: String, uid: Int, email: String): String = {
    val claims = new JwtClaims
    claims.setSubject(subject)
    claims.setClaim("userId", uid)
    claims.setClaim("email", email)
    // The shared consumer is built with setRequireExpirationTime(); an exp-less token is rejected.
    claims.setExpirationTimeMinutesInTheFuture(10f)
    JwtAuth.jwtToken(claims)
  }

  /**
    * amber ships no logback-test.xml, so `amber/src/main/resources/logback.xml` governs this JVM
    * and pins `org.apache` at WARN — which leaves the info/debug lines of the kubernetes branch
    * unexecuted. Raising this one logger for the duration of a single call executes them. The
    * previous level is restored in `finally` because every amber suite shares one JVM.
    */
  private def withDebugLogging[T](body: => T): T = {
    val logger = org.slf4j.LoggerFactory
      .getLogger(classOf[ServletAwareConfigurator])
      .asInstanceOf[LogbackLogger]
    val previousLevel = logger.getLevel
    logger.setLevel(Level.DEBUG)
    try body
    finally logger.setLevel(previousLevel)
  }

  // -- kubernetes mode ----------------------------------------------------------

  "modifyHandshake" should "build the user from the trusted headers in kubernetes mode" in {
    val (config, request, properties) = handshake(headers = k8sHeaders)

    withDebugLogging {
      new ServletAwareConfigurator().modifyHandshake(config, request, null)
    }

    // Read back by key: the privilege and the user live under two different keys, and both
    // `myOnOpen` (privilege) and `myOnMsg` (user) look them up by exactly these names.
    properties.get(HeaderField.UserComputingUnitAccess) shouldBe PrivilegeEnum.READ.name()
    val user = userIn(properties)
    // Three mutually distinct values, so a setName/setEmail transposition cannot pass.
    user.getUid.intValue() shouldBe userId.toInt
    user.getName shouldBe userName
    user.getEmail shouldBe userEmail
  }

  it should "take the first value of a repeated header, not the last" in {
    // HTTP headers are legitimately multi-valued, and `_.asScala.headOption` is what decides WHICH
    // occurrence wins. In kubernetes mode that choice decides the identity and the privilege a
    // websocket is granted, so a duplicate appended after Envoy's own must not be able to override
    // it. `k8sHeaders` gives every header exactly one value, where head and last agree, so nothing
    // else here can see the difference.
    val (config, request, properties) = handshake(headers =
      k8sHeaders ++ Map(
        HeaderField.UserId -> Seq(userId, "99"),
        HeaderField.UserComputingUnitAccess -> Seq(
          PrivilegeEnum.READ.name(),
          PrivilegeEnum.WRITE.name()
        )
      )
    )

    new ServletAwareConfigurator().modifyHandshake(config, request, null)

    properties.get(HeaderField.UserComputingUnitAccess) shouldBe PrivilegeEnum.READ.name()
    userIn(properties).getUid.intValue() shouldBe userId.toInt
  }

  it should "write an empty privilege string that myOnOpen cannot parse when the access header carries no value" in {
    // CHARACTERIZATION OF A DEFECT, NOT A CONTRACT — and a strict fix SHOULD turn this test red.
    //
    // Reachability: the enclosing guard only checks that the key is PRESENT, so an empty value list
    // still selects the kubernetes branch and the privilege falls through to `getOrElse("")`. No
    // servlet container produces that shape: a header that is present always carries at least one
    // value, `""` at worst, which yields `Some("")` rather than None. The fallback is therefore
    // defensive dead code, reachable only by handing `getHeaders` an empty list directly as below.
    //
    // What it produces is not a graceful default but a deferred crash: `PrivilegeEnum` is a Java
    // enum, so `WorkflowWebsocketResource.myOnOpen`'s `PrivilegeEnum.valueOf` on this value throws
    // straight out of the @OnOpen handler. The last assertion records that consequence, so that
    // changing the fallback to `PrivilegeEnum.NONE.name()` reads as the fix it would be.
    val (config, request, properties) =
      handshake(headers = k8sHeaders + (HeaderField.UserComputingUnitAccess -> Seq.empty))

    new ServletAwareConfigurator().modifyHandshake(config, request, null)

    properties.get(HeaderField.UserComputingUnitAccess) shouldBe ""
    // The user is still built: the fallback must not abort the branch.
    userIn(properties).getName shouldBe userName
    an[IllegalArgumentException] should be thrownBy PrivilegeEnum.valueOf(
      properties.get(HeaderField.UserComputingUnitAccess).asInstanceOf[String]
    )
  }

  it should "fall back to single-node mode when any one of the four headers is missing" in {
    // One case per header, because a single "none of them present" case only proves the
    // conjunction is false — it would stay green with any one of the four `contains` calls deleted.
    k8sHeaders.keys.foreach { missing =>
      withClue(s"with $missing missing: ") {
        val (config, request, properties) =
          handshake(headers = k8sHeaders - missing, queryString = "wid=1&cuid=2")

        new ServletAwareConfigurator().modifyHandshake(config, request, null)

        // WRITE is the single-node constant; the surviving headers say READ, so the value in the
        // map names which branch ran. (With `x-user-id` or `x-user-name`/`x-user-email` missing the
        // kubernetes branch would instead throw before writing anything, leaving the map empty.)
        properties.get(HeaderField.UserComputingUnitAccess) shouldBe PrivilegeEnum.WRITE.name()
        properties.containsKey(classOf[User].getName) shouldBe false
      }
    }
  }

  // -- single-node mode ---------------------------------------------------------

  it should "build the user from the access-token query parameter in single-node mode" in {
    // The token is surrounded by other parameters on purpose: with it alone in the query string a
    // naive "the query string IS the token" implementation would pass too.
    val token = tokenFor("token-subject", 4242, "token-mail@example.com")
    val (config, request, properties) =
      handshake(queryString = s"wid=1&access-token=$token&cuid=2")

    new ServletAwareConfigurator().modifyHandshake(config, request, null)

    properties.get(HeaderField.UserComputingUnitAccess) shouldBe PrivilegeEnum.WRITE.name()
    val user = userIn(properties)
    // The uid claim survives a JSON round trip as a Long, which is why the production cast is to
    // Long and not Integer; asserting the Int value pins the narrowing too.
    user.getUid.intValue() shouldBe 4242
    user.getName shouldBe "token-subject"
    user.getEmail shouldBe "token-mail@example.com"
  }

  // -- the catch arm ------------------------------------------------------------

  it should "swallow a malformed user-id header, leaving the properties untouched" in {
    // "no exception escaped" on its own would pass with the whole method body deleted, so the
    // assertion that carries the weight is the PARTIAL state the failure leaves behind: the parse
    // of `x-user-id` happens before any write, so nothing at all reaches the map.
    val (config, request, properties) =
      handshake(headers = k8sHeaders + (HeaderField.UserId -> Seq("not-a-number")))

    noException should be thrownBy
      new ServletAwareConfigurator().modifyHandshake(config, request, null)

    properties.keySet.asScala shouldBe empty
  }

  it should "swallow a tampered access token, keeping the privilege it already granted" in {
    // The other half of the partial-state contract: the single-node branch writes the privilege
    // BEFORE it parses the token, so a rejected token leaves that entry behind and no user.
    val token = tokenFor("token-subject", 4242, "token-mail@example.com")
    val parts = token.split('.')
    parts.length shouldBe 3
    val tampered = s"${parts(0)}.${parts(1)}.${parts(2).reverse}"
    val (config, request, properties) =
      handshake(queryString = s"wid=1&access-token=$tampered&cuid=2")

    noException should be thrownBy
      new ServletAwareConfigurator().modifyHandshake(config, request, null)

    properties.get(HeaderField.UserComputingUnitAccess) shouldBe PrivilegeEnum.WRITE.name()
    properties.containsKey(classOf[User].getName) shouldBe false
  }
}
