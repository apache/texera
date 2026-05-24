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
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.scalatest.flatspec.AnyFlatSpec

import javax.ws.rs.{BadRequestException, WebApplicationException}

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
}
