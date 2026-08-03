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

import org.apache.texera.auth.SessionUser
import org.apache.texera.common.config.UserSystemConfig
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.scalatest.flatspec.AnyFlatSpec

import javax.ws.rs.{BadRequestException, ForbiddenException, WebApplicationException}

class GmailResourceSpec extends AnyFlatSpec {

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

  "notifyUnauthorizedUser" should "reject a malformed receiver with HTTP 403 before any DB access" in {
    // The format guard is the first statement, ahead of the admin lookup, so this needs no
    // database: `new GmailResource()` touches none either, since the companion's context/userDao
    // are defs and senderGmail is lazy. Remove the guard and `userDao.fetchByRole` throws something
    // else — NoSuchElementException on a virgin JVM, or a jOOQ DataAccessException once another
    // MockTexeraDB suite has initialised and closed the shared SqlServer — and either way
    // `intercept[ForbiddenException]` rejects it.
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
}
