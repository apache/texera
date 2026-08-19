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

package org.apache.texera.web.resource.auth

import org.apache.texera.web.resource.EmailMessage
import org.apache.texera.web.resource.auth.EmailCodeVerifier.{ADD_EMAIL, REGISTER}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import javax.ws.rs.WebApplicationException
import scala.collection.mutable.ArrayBuffer

/**
  * No database and no mail server: the verifier holds no persisted state, so its clock, its sender
  * and its "is SMTP configured" probe are all constructor arguments. That is what makes expiry, the
  * attempt cap and the resend cooldown testable without waiting or sending.
  */
class EmailCodeVerifierSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val secret = "0123456789abcdef0123456789abcdef"
  private val start = Instant.parse("2026-08-18T12:00:00Z")

  private var now: Instant = _
  private var sent: ArrayBuffer[(EmailMessage, String)] = _
  private var smtpConfigured: Boolean = _
  private var sendResult: Either[String, Unit] = _
  private var verifier: EmailCodeVerifier = _

  override def beforeEach(): Unit = {
    now = start
    sent = ArrayBuffer.empty
    smtpConfigured = true
    sendResult = Right(())
    verifier = newVerifier()
  }

  private def newVerifier(): EmailCodeVerifier =
    new EmailCodeVerifier(
      secret = secret,
      clock = () => now,
      send = (message, recipient) => { sent += ((message, recipient)); sendResult },
      smtpConfigured = () => smtpConfigured
    )

  private def statusOf(thrown: WebApplicationException): Int = thrown.getResponse.getStatus

  /** The code the verifier would mail right now, read off the message it hands the sender. */
  private def issueAndRead(purpose: String, scope: String, email: String): String = {
    verifier.issue(purpose, scope, email)
    val body = sent.last._1.content
    "\\d{6}".r.findFirstIn(body).getOrElse(fail(s"no six-digit code in the mail body: $body"))
  }

  private def advance(seconds: Long): Unit = now = now.plusSeconds(seconds)

  "issue" should "mail a six-digit code to the address being verified" in {
    verifier.issue(ADD_EMAIL, "7", "someone@example.com")

    sent should have size 1
    val (message, recipient) = sent.head
    recipient shouldBe "someone@example.com"
    message.content should include regex "\\d{6}"
  }

  it should "log the code instead of mailing it when SMTP is unconfigured" in {
    smtpConfigured = false

    verifier.issue(ADD_EMAIL, "7", "someone@example.com")

    // Nothing handed to the sender: with no sender configured the send would fail, and failing the
    // request would make the feature impossible to run locally.
    sent shouldBe empty
  }

  it should "refuse when the address is configured but the send fails" in {
    sendResult = Left("smtp exploded")

    val thrown = intercept[WebApplicationException](verifier.issue(ADD_EMAIL, "7", "a@b.com"))

    statusOf(thrown) shouldBe 503
  }

  it should "refuse a second code for the same scope inside the cooldown" in {
    verifier.issue(ADD_EMAIL, "7", "a@b.com")
    advance(EmailCodeVerifier.RESEND_INTERVAL_SECONDS - 1)

    val thrown = intercept[WebApplicationException](verifier.issue(ADD_EMAIL, "7", "a@b.com"))

    statusOf(thrown) shouldBe 429
    sent should have size 1
  }

  it should "mail again once the cooldown has elapsed" in {
    verifier.issue(ADD_EMAIL, "7", "a@b.com")
    advance(EmailCodeVerifier.RESEND_INTERVAL_SECONDS)

    verifier.issue(ADD_EMAIL, "7", "a@b.com")

    sent should have size 2
  }

  it should "not let one scope's cooldown block another" in {
    verifier.issue(ADD_EMAIL, "7", "a@b.com")
    verifier.issue(ADD_EMAIL, "8", "b@b.com")

    sent should have size 2
  }

  "check" should "accept the code that was issued" in {
    val code = issueAndRead(ADD_EMAIL, "7", "a@b.com")

    noException should be thrownBy verifier.check(ADD_EMAIL, "7", "a@b.com", code)
  }

  it should "derive the same code twice inside one step without storing anything" in {
    val first = issueAndRead(ADD_EMAIL, "7", "a@b.com")
    advance(EmailCodeVerifier.RESEND_INTERVAL_SECONDS)
    val second = issueAndRead(ADD_EMAIL, "7", "a@b.com")

    second shouldBe first
  }

  it should "survive a restart: a fresh instance accepts a code issued by the previous one" in {
    val code = issueAndRead(ADD_EMAIL, "7", "a@b.com")

    // Same secret, same clock — nothing about the code lives in the instance that issued it.
    noException should be thrownBy newVerifier().check(ADD_EMAIL, "7", "a@b.com", code)
  }

  it should "still accept a code from the previous step" in {
    val code = issueAndRead(ADD_EMAIL, "7", "a@b.com")
    advance(EmailCodeVerifier.STEP_SECONDS)

    noException should be thrownBy verifier.check(ADD_EMAIL, "7", "a@b.com", code)
  }

  it should "reject a code once two steps have passed" in {
    val code = issueAndRead(ADD_EMAIL, "7", "a@b.com")
    advance(EmailCodeVerifier.STEP_SECONDS * 2)

    statusOf(
      intercept[WebApplicationException](verifier.check(ADD_EMAIL, "7", "a@b.com", code))
    ) shouldBe 406
  }

  it should "reject a code minted for a different address" in {
    val code = issueAndRead(ADD_EMAIL, "7", "a@b.com")

    statusOf(
      intercept[WebApplicationException](verifier.check(ADD_EMAIL, "7", "other@b.com", code))
    ) shouldBe 406
  }

  it should "reject a code minted for a different scope" in {
    val code = issueAndRead(ADD_EMAIL, "7", "a@b.com")

    statusOf(
      intercept[WebApplicationException](verifier.check(ADD_EMAIL, "8", "a@b.com", code))
    ) shouldBe 406
  }

  it should "reject a code minted for a different purpose" in {
    val code = issueAndRead(REGISTER, "handle", "a@b.com")

    statusOf(
      intercept[WebApplicationException](verifier.check(ADD_EMAIL, "handle", "a@b.com", code))
    ) shouldBe 406
  }

  it should "reject a code minted under a different secret" in {
    val code = issueAndRead(ADD_EMAIL, "7", "a@b.com")
    val other = new EmailCodeVerifier(
      secret = "ffffffffffffffffffffffffffffffff",
      clock = () => now,
      send = (_, _) => Right(()),
      smtpConfigured = () => true
    )

    statusOf(
      intercept[WebApplicationException](other.check(ADD_EMAIL, "7", "a@b.com", code))
    ) shouldBe 406
  }

  it should "match the address case-insensitively, since stored addresses keep their casing" in {
    val code = issueAndRead(ADD_EMAIL, "7", "Someone@Example.com")

    noException should be thrownBy verifier.check(ADD_EMAIL, "7", "someone@example.com", code)
  }

  it should "reject an empty or malformed code without consuming the attempt budget" in {
    issueAndRead(ADD_EMAIL, "7", "a@b.com")

    Seq(null, "", "12345", "abcdef").foreach { bad =>
      statusOf(
        intercept[WebApplicationException](verifier.check(ADD_EMAIL, "7", "a@b.com", bad))
      ) shouldBe 406
    }
  }

  it should "stop accepting guesses once the attempt cap is reached" in {
    val code = issueAndRead(ADD_EMAIL, "7", "a@b.com")
    val wrong = if (code == "000000") "111111" else "000000"

    (1 to EmailCodeVerifier.MAX_ATTEMPTS).foreach { _ =>
      statusOf(
        intercept[WebApplicationException](verifier.check(ADD_EMAIL, "7", "a@b.com", wrong))
      ) shouldBe 406
    }

    // Even the right code is refused now, and with 429 rather than 406 — the caller has been
    // rate-limited, not told anything about the code.
    statusOf(
      intercept[WebApplicationException](verifier.check(ADD_EMAIL, "7", "a@b.com", code))
    ) shouldBe 429
  }

  it should "not let one scope's spent attempts lock out another" in {
    val code = issueAndRead(ADD_EMAIL, "8", "b@b.com")
    (1 to EmailCodeVerifier.MAX_ATTEMPTS).foreach { _ =>
      intercept[WebApplicationException](verifier.check(ADD_EMAIL, "7", "a@b.com", "000000"))
    }

    noException should be thrownBy verifier.check(ADD_EMAIL, "8", "b@b.com", code)
  }

  it should "give a fresh attempt budget once the guesses age out" in {
    val code = issueAndRead(ADD_EMAIL, "7", "a@b.com")
    (1 to EmailCodeVerifier.MAX_ATTEMPTS).foreach { _ =>
      intercept[WebApplicationException](verifier.check(ADD_EMAIL, "7", "a@b.com", "000000"))
    }
    statusOf(
      intercept[WebApplicationException](verifier.check(ADD_EMAIL, "7", "a@b.com", code))
    ) shouldBe 429

    // The cap bounds guesses per window, not forever; the code itself has expired by now anyway.
    advance(EmailCodeVerifier.STEP_SECONDS * 2)
    val fresh = issueAndRead(ADD_EMAIL, "7", "a@b.com")

    noException should be thrownBy verifier.check(ADD_EMAIL, "7", "a@b.com", fresh)
  }

  "evictStale" should "drop entries nobody has touched and keep the live ones" in {
    verifier.issue(ADD_EMAIL, "7", "a@b.com")
    verifier.trackedSize shouldBe 1

    advance(EmailCodeVerifier.STEP_SECONDS * 4)
    verifier.issue(ADD_EMAIL, "8", "b@b.com")
    verifier.evictStale()

    verifier.trackedSize shouldBe 1
  }
}
