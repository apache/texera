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

package org.apache.texera.web.resource

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.{Level, Logger => LogbackLogger}
import ch.qos.logback.core.read.ListAppender
import org.apache.texera.auth.SessionUser
import org.apache.texera.common.config.UserSystemConfig
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.USER
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.daos.UserDao
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.slf4j.LoggerFactory

import java.util.UUID
import javax.ws.rs.{BadRequestException, ForbiddenException, WebApplicationException}
import scala.jdk.CollectionConverters._

class GmailResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private def newSessionUser(): SessionUser = {
    val user = new User
    user.setUid(Integer.valueOf(1))
    user.setName("test")
    user.setRole(UserRoleEnum.REGULAR)
    user.setEmail("test@example.com")
    new SessionUser(user)
  }

  it should "throw BadRequestException (HTTP 400) when the receiver fails email-format validation" in {
    val resource = new GmailResource()
    val msg = EmailMessage(
      receiver = "not-a-valid-email",
      subject = "subj",
      content = "body"
    )
    val ex = intercept[BadRequestException] {
      resource.sendEmailRequest(msg, newSessionUser())
    }
    assert(ex.getResponse.getStatus == 400)
  }

  it should "throw WebApplicationException with HTTP 502 when sendEmail fails for a non-validation reason" in {
    // In the test environment `UserSystemConfig.gmail` defaults to "", so
    // `createMimeMessage`'s `new InternetAddress(senderGmail)` raises an
    // `AddressException` deterministically — without any network or SMTP
    // server contact — and `sendEmail` returns `Left("Failed to send email:
    // ...")`. The resource then maps that `Left` to a 502 BadGateway.
    val resource = new GmailResource()
    val msg = EmailMessage(
      receiver = "valid@example.com",
      subject = "subj",
      content = "body"
    )
    val ex = intercept[WebApplicationException] {
      resource.sendEmailRequest(msg, newSessionUser())
    }
    assert(
      !ex.isInstanceOf[BadRequestException],
      s"expected non-validation failure, but got BadRequestException: ${ex.getMessage}"
    )
    assert(ex.getResponse.getStatus == 502)
  }

  /**
    * Every test here that reaches `sendEmail` relies on `UserSystemConfig.gmail` being empty, which
    * is the shipped default: it makes `new InternetAddress(senderGmail)` throw inside sendEmail's own
    * Try, so nothing is dialled. If it is ever overridden (USER_SYS_GOOGLE_SMTP_GMAIL) the code
    * reaches `Transport.send` against smtp.gmail.com:465 with no connect/read timeout configured —
    * an unbounded hang on a runner without egress, and a real email where there is one. Fail loudly
    * rather than skip, so the misconfiguration is visible instead of silently deleting coverage.
    */
  private def requireNoRealGmailSender(): Unit =
    if (UserSystemConfig.gmail.nonEmpty) {
      fail(
        s"UserSystemConfig.gmail is set to '${UserSystemConfig.gmail}'; this suite would open a real " +
          "SMTP connection. Unset USER_SYS_GOOGLE_SMTP_GMAIL and re-run."
      )
    }

  requireNoRealGmailSender()
  it should "fall back to the session user's email when the request carries an empty receiver" in {
    requireNoRealGmailSender()
    // An empty receiver reaching `sendEmail` unchanged fails the format regex and surfaces as a
    // 400; a 502 (the AddressException path described above) means something valid-looking got
    // through, i.e. the fallback fired. Strictly this pins "whatever reached sendEmail passed
    // isValidEmail" rather than the substitution itself — widening that regex to accept "" would
    // keep this green with the fallback deleted — but as production stands it is a genuine
    // change-detector, and sendEmailRequest returns Unit so there is no seam for a direct assertion.
    val resource = new GmailResource()
    val msg = EmailMessage(
      receiver = "",
      subject = "subj",
      content = "body"
    )
    val ex = intercept[WebApplicationException] {
      resource.sendEmailRequest(msg, newSessionUser())
    }
    assert(
      !ex.isInstanceOf[BadRequestException],
      s"empty receiver was not replaced by the session user's email: ${ex.getMessage}"
    )
    assert(ex.getResponse.getStatus == 502)
  }

  // ─── isValidEmail, observed through the public sendEmail ────────────────────

  "sendEmail" should "reject malformed recipient addresses before attempting delivery" in {
    requireNoRealGmailSender()
    val msg = EmailMessage(receiver = "unused@example.com", subject = "subj", content = "body")
    val malformed = Seq(
      null, // the `email != null` short-circuit: the regex would NPE on its own
      "plainaddress", // no '@' at all
      "@example.com", // empty local part
      "user name@example.com", // space is outside the local-part character class
      "user@localhost", // domain carries no dot, so there is no suffix to match
      "a@b.c" // one-letter suffix, below the {2,} floor
    )
    malformed.foreach { address =>
      withClue(s"address '$address': ") {
        GmailResource.sendEmail(msg, address) shouldBe Left("Invalid email format")
      }
    }
  }

  it should "get a well-formed address past the format guard and fail later in delivery" in {
    requireNoRealGmailSender()
    // Distinguishes the two Left kinds: this one comes from the Try around createMimeMessage
    // (empty sender ⇒ AddressException), i.e. the guard let the address through. Without it the
    // test above could stay green under a regex that rejects everything.
    val msg = EmailMessage(receiver = "unused@example.com", subject = "subj", content = "body")
    GmailResource.sendEmail(msg, "ok@example.com") match {
      case Left(error) => error should startWith("Failed to send email:")
      case Right(_) =>
        fail("delivery unexpectedly succeeded — this suite must never reach a real SMTP server")
    }
  }

  // ─── notifyUnauthorizedUser ─────────────────────────────────────────────────

  // Random suffix keeps this suite's rows from colliding with data other suites may
  // have left in the shared (singleton) embedded DB; suites run sequentially.
  private val runId = UUID.randomUUID().toString.substring(0, 8)
  private var userDao: UserDao = _

  override protected def beforeAll(): Unit = initializeDBAndReplaceDSLContext()

  override protected def afterAll(): Unit = shutdownDB()

  override protected def beforeEach(): Unit = {
    userDao = new UserDao(getDSLContext.configuration())
    cleanup()
  }

  override protected def afterEach(): Unit = cleanup()

  // startsWith escapes SQL LIKE wildcards, so the literal "gmailspec_" prefix is matched exactly.
  private def cleanup(): Unit =
    getDSLContext.deleteFrom(USER).where(USER.NAME.startsWith("gmailspec_")).execute()

  /**
    * A value the `email` column happily stores but `isValidEmail` rejects. Seeding admins with such
    * values is what makes the fan-out observable: `notifyUnauthorizedUser` discards sendEmail's
    * Either, and sendEmail's only externally visible act for a rejected address is the warning it
    * logs naming that address. It carries no '@' rather than a merely odd domain, so tightening or
    * loosening the regex's domain rules cannot quietly turn these into deliverable addresses and
    * make the fan-out invisible again.
    */
  private def unroutable(tag: String): String = s"gmailspec-$tag-$runId-no-at-sign"

  private def seedUser(tag: String, role: UserRoleEnum, email: String): String = {
    val user = new User
    user.setName(s"gmailspec_${tag}_$runId")
    user.setEmail(email)
    user.setRole(role)
    userDao.insert(user)
    email
  }

  private def adminEmailsInDb: Set[String] =
    userDao.fetchByRole(UserRoleEnum.ADMIN).asScala.map(_.getEmail).toSet

  private val attemptedRecipient = """invalid address: (.+)$""".r.unanchored

  /**
    * Runs `body` with a capturing appender on the logger `GmailResource.sendEmail` writes to (the
    * companion object's own class), and returns the addresses it reported as unsendable.
    */
  private def attemptedRecipientsDuring(body: => Unit): Seq[String] = {
    val logger = LoggerFactory.getLogger(GmailResource.getClass).asInstanceOf[LogbackLogger]
    val appender = new ListAppender[ILoggingEvent]
    appender.start()
    val previousLevel = logger.getLevel
    logger.setLevel(Level.WARN)
    logger.addAppender(appender)
    try body
    finally {
      logger.detachAppender(appender)
      appender.stop()
      logger.setLevel(previousLevel)
    }
    appender.list.asScala.toSeq.map(_.getFormattedMessage).collect {
      case attemptedRecipient(address) => address
    }
  }

  /*
   * Two things in notifyUnauthorizedUser are deliberately left unasserted here.
   *
   * Both `catch { case ex: Exception => logger.warn(...) }` arms are unreachable: sendEmail wraps
   * everything in a Try and returns a Left, it never throws. Do not contort a test into reaching
   * them — delete them instead.
   *
   * The applicant acknowledgement that follows the fan-out has no observable effect offline. Its
   * address must already be well-formed to get past the ForbiddenException guard, so sendEmail
   * logs nothing for it, and the Either it returns is discarded. Deleting that whole block leaves
   * this suite green; pinning it needs a configured sender, which means a real SMTP connection.
   *
   * Nor is the admin notification's PAYLOAD pinned, only its recipient. Replacing the whole
   * `userRegistrationNotification(...)` construction with the incoming request leaves this suite
   * green: sendEmail lives on the companion object, so there is no seam to intercept what it was
   * handed, and the only offline observable is which address it rejected. The `affiliation` and
   * `reason` values in the fixture below are therefore scene-setting, not assertions — the builder
   * itself, including its toAdmin branch, is already pinned by EmailTemplateSpec, which is where
   * that contract belongs.
   */
  "notifyUnauthorizedUser" should "attempt one notification per ADMIN row and none for other roles" in {
    requireNoRealGmailSender()
    val adminA = seedUser("admina", UserRoleEnum.ADMIN, unroutable("admin-a"))
    val adminB = seedUser("adminb", UserRoleEnum.ADMIN, unroutable("admin-b"))
    // a non-admin whose address would show up just as loudly if the role filter were dropped
    seedUser("regular", UserRoleEnum.REGULAR, unroutable("regular"))
    adminEmailsInDb shouldBe Set(adminA, adminB)

    val msg = EmailMessage(
      receiver = s"gmailspec-applicant-$runId@example.com",
      subject = "subj",
      content = "body",
      affiliation = Some("UCI"),
      reason = Some("research")
    )
    val attempted = attemptedRecipientsDuring(new GmailResource().notifyUnauthorizedUser(msg))

    // one attempt per admin (size pins the iteration, the set pins who) — the applicant's own
    // acknowledgement leaves no trace here because its address is well-formed by construction
    attempted should have size 2
    attempted.toSet shouldBe Set(adminA, adminB)
  }

  it should "complete without error when no administrator is registered" in {
    requireNoRealGmailSender()
    adminEmailsInDb shouldBe empty

    val msg = EmailMessage(
      receiver = s"gmailspec-applicant-$runId@example.com",
      subject = "subj",
      content = "body"
    )
    noException should be thrownBy new GmailResource().notifyUnauthorizedUser(msg)
  }

  it should "reject a malformed receiver with HTTP 403 before any DB access" in {
    // The format guard is the first statement, ahead of the admin lookup. Drop it and the method
    // runs to completion instead of throwing — the admin fan-out finds nothing and the applicant
    // acknowledgement returns a Left that the method discards — so `intercept` fails.
    val resource = new GmailResource()
    val msg = EmailMessage(
      receiver = "not-a-valid-email",
      subject = "subj",
      content = "body"
    )
    val ex = intercept[ForbiddenException] {
      resource.notifyUnauthorizedUser(msg)
    }
    assert(ex.getResponse.getStatus == 403)
  }

  "adminRegistrationNotification" should "resolve the requester's stored name by email" in {
    val requesterEmail = s"gmailspec-requester-$runId@example.com"
    val requesterName = s"gmailspec_requester_$runId"
    seedUser("requester", UserRoleEnum.REGULAR, requesterEmail)

    val message = GmailResource.adminRegistrationNotification(
      "admin@example.com",
      EmailMessage(receiver = requesterEmail, subject = "", content = "")
    )

    message.content should include(s"Name: $requesterName")
    message.content should include(s"Email: $requesterEmail")
  }

  it should "render the fallback when no user matches the requester email" in {
    val message = GmailResource.adminRegistrationNotification(
      "admin@example.com",
      EmailMessage(receiver = "missing-requester@example.com", subject = "", content = "")
    )

    message.content should include("Name: Not provided")
  }
}
