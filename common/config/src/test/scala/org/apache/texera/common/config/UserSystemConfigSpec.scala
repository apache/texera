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

package org.apache.texera.common.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Spec for [[UserSystemConfig]]. Reading each value forces resolution from user-system.conf, so a
  * renamed or mistyped key surfaces here as a ConfigException. Every value carries a `${?ENV}`
  * override, so exact-value assertions are guarded on the env var being unset.
  */
class UserSystemConfigSpec extends AnyFlatSpec with Matchers {

  // `${?VAR}` in HOCON can be satisfied by an OS env var or a JVM system property,
  // so treat either as an override.
  private def isOverridden(name: String): Boolean =
    sys.env.contains(name) || sys.props.contains(name)
  private def ifUnset(name: String)(assertion: => Any): Unit =
    if (!isOverridden(name)) assertion

  "UserSystemConfig" should "resolve every value from user-system.conf" in {
    // reference each val to force resolution regardless of environment
    UserSystemConfig.adminUsername should not be null
    UserSystemConfig.adminPassword should not be null
    UserSystemConfig.googleClientId should not be null
    UserSystemConfig.orcidClientId should not be null
    UserSystemConfig.orcidClientSecret should not be null
    UserSystemConfig.orcidBaseUrl should not be null
    UserSystemConfig.orcidRedirectUri should not be null
    UserSystemConfig.gmail should not be null
    UserSystemConfig.smtpPassword should not be null
    UserSystemConfig.projectName should not be null
    UserSystemConfig.workflowVersionCollapseIntervalInMinutes should be >= 0
  }

  it should "match the user-system.conf defaults when no env override is set" in {
    ifUnset("USER_SYS_ADMIN_USERNAME")(UserSystemConfig.adminUsername shouldBe "texera")
    ifUnset("USER_SYS_ADMIN_PASSWORD")(UserSystemConfig.adminPassword shouldBe "texera")
    ifUnset("USER_SYS_GOOGLE_CLIENT_ID")(UserSystemConfig.googleClientId shouldBe "")
    ifUnset("USER_SYS_GOOGLE_SMTP_GMAIL")(UserSystemConfig.gmail shouldBe "")
    ifUnset("USER_SYS_GOOGLE_SMTP_PASSWORD")(UserSystemConfig.smtpPassword shouldBe "")
    // The two credentials ship blank so ORCID sign-in cannot be half-configured by accident;
    // `OrcidAuthResource.getConfig` reports the provider unavailable until an operator fills them
    // in. baseUrl and redirectUri ship usable dev defaults: the sandbox registry, and the callback
    // URL of `ng serve --host 127.0.0.1`.
    ifUnset("USER_SYS_ORCID_CLIENT_ID")(UserSystemConfig.orcidClientId shouldBe "")
    ifUnset("USER_SYS_ORCID_CLIENT_SECRET")(UserSystemConfig.orcidClientSecret shouldBe "")
    ifUnset("USER_SYS_ORCID_BASE_URL")(
      UserSystemConfig.orcidBaseUrl shouldBe "https://sandbox.orcid.org"
    )
    ifUnset("USER_SYS_ORCID_REDIRECT_URI")(
      UserSystemConfig.orcidRedirectUri shouldBe "http://127.0.0.1:4200/callback/orcid"
    )
    ifUnset("USER_SYS_PROJECT_NAME")(UserSystemConfig.projectName shouldBe "Texera")
    ifUnset("USER_SYS_INVITE_ONLY")(UserSystemConfig.inviteOnly shouldBe false)
    ifUnset("USER_SYS_EMAIL_VERIFICATION")(UserSystemConfig.emailVerification shouldBe false)
    ifUnset("USER_SYS_VERSION_TIME_LIMIT_IN_MINUTES")(
      UserSystemConfig.workflowVersionCollapseIntervalInMinutes shouldBe 60
    )
  }

  it should "return None for appDomain when the domain is blank/unset" in {
    val overrideValue = sys.env.get("USER_SYS_DOMAIN").orElse(sys.props.get("USER_SYS_DOMAIN"))
    if (overrideValue.forall(_.trim.isEmpty)) {
      UserSystemConfig.appDomain shouldBe None
    } else {
      UserSystemConfig.appDomain shouldBe defined
    }
  }
}
