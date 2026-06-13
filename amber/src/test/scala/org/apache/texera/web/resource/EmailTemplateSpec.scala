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

import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EmailTemplateSpec extends AnyFlatSpec with Matchers {

  // EmailTemplate captures `UserSystemConfig.appDomain` at class init. Under
  // CI / local test runs no USER_SYS_DOMAIN is set, so `appDomain` is None
  // and the templates render without the trailing `[$deployment]` suffix.
  // Cases that require the suffix-present branch would need a JVM-level
  // env override before EmailTemplate loads — out of scope here.

  // -- userRegistrationNotification (admin branch) ----------------------------

  "userRegistrationNotification(toAdmin = true)" should
    "produce a subject and receiver and include the requesting user's email" in {
    val msg = EmailTemplate.userRegistrationNotification(
      receiverEmail = "admin@example.com",
      userEmail = Some("alice@example.com"),
      affiliation = Some("UC Irvine"),
      reason = Some("research"),
      toAdmin = true
    )
    msg.receiver shouldBe "admin@example.com"
    msg.subject should startWith("New Account Request Pending Approval")
    msg.content should include("Email: alice@example.com")
    msg.content should include("Affiliation: UC Irvine")
    msg.content should include("Reason: research")
    msg.content should include("Visit the admin panel at:")
  }

  it should "fall back to 'Unknown' when userEmail is None" in {
    val msg = EmailTemplate.userRegistrationNotification(
      receiverEmail = "admin@example.com",
      userEmail = None,
      affiliation = Some("UC Irvine"),
      reason = Some("research"),
      toAdmin = true
    )
    msg.content should include("Email: Unknown")
  }

  it should "render 'Not provided' for affiliation/reason when None or whitespace-only" in {
    val withNone = EmailTemplate.userRegistrationNotification(
      receiverEmail = "admin@example.com",
      userEmail = Some("alice@example.com"),
      affiliation = None,
      reason = None,
      toAdmin = true
    )
    withNone.content should include("Affiliation: Not provided")
    withNone.content should include("Reason: Not provided")

    val withBlank = EmailTemplate.userRegistrationNotification(
      receiverEmail = "admin@example.com",
      userEmail = Some("alice@example.com"),
      affiliation = Some("   "),
      reason = Some(""),
      toAdmin = true
    )
    // The `.filter(_.trim.nonEmpty)` guard treats whitespace-only and empty
    // strings the same as None.
    withBlank.content should include("Affiliation: Not provided")
    withBlank.content should include("Reason: Not provided")
  }

  // -- userRegistrationNotification (user branch) -----------------------------

  "userRegistrationNotification(toAdmin = false)" should
    "produce the acknowledgement template addressed to the user" in {
    val msg = EmailTemplate.userRegistrationNotification(
      receiverEmail = "alice@example.com",
      userEmail = Some("ignored@example.com"),
      affiliation = Some("ignored"),
      reason = Some("ignored"),
      toAdmin = false
    )
    msg.receiver shouldBe "alice@example.com"
    msg.subject should startWith("Account Request Received")
    msg.content should include("Thank you for submitting your account request")
    // The user-facing template intentionally does NOT echo the admin-only
    // fields back to the requester — if a refactor accidentally surfaces
    // them, this assertion will catch the leak.
    msg.content should not include "Email: ignored"
    msg.content should not include "Affiliation: ignored"
    msg.content should not include "Reason: ignored"
  }

  // -- createRoleChangeTemplate -----------------------------------------------

  "createRoleChangeTemplate" should "embed the new role name in the content" in {
    val msg = EmailTemplate.createRoleChangeTemplate("alice@example.com", UserRoleEnum.ADMIN)
    msg.receiver shouldBe "alice@example.com"
    msg.subject should startWith("Your Role Has Been Updated")
    msg.content should include("Your user role has been updated to: ADMIN")
  }

  it should "render distinct content for distinct enum values" in {
    val admin = EmailTemplate.createRoleChangeTemplate("alice@example.com", UserRoleEnum.ADMIN)
    val regular = EmailTemplate.createRoleChangeTemplate("alice@example.com", UserRoleEnum.REGULAR)
    val inactive =
      EmailTemplate.createRoleChangeTemplate("alice@example.com", UserRoleEnum.INACTIVE)
    Set(admin.content, regular.content, inactive.content) should have size 3
  }
}
