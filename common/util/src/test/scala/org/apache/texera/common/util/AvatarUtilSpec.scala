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

package org.apache.texera.common.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AvatarUtilSpec extends AnyFlatSpec with Matchers {

  private val avatar = "https://lh3.googleusercontent.com/a/AVATAR-ID"

  "sanitize" should "keep a complete https URL on an allowlisted host" in {
    AvatarUtil.sanitize(Some(avatar)) shouldBe Some(avatar)
  }

  it should "keep an avatar on a subdomain of an allowlisted host" in {
    val url = "https://lh5.googleusercontent.com/a/OTHER-ID"
    AvatarUtil.sanitize(Some(url)) shouldBe Some(url)
  }

  it should "accept http as well as https" in {
    val url = "http://lh3.googleusercontent.com/a/AVATAR-ID"
    AvatarUtil.sanitize(Some(url)) shouldBe Some(url)
  }

  it should "trim surrounding whitespace" in {
    AvatarUtil.sanitize(Some(s"  $avatar  ")) shouldBe Some(avatar)
  }

  // Dropping rather than rejecting: the caller treats None as "the provider supplied no avatar",
  // which leaves the stored value alone instead of failing the login.
  it should "drop a URL on a host outside the allowlist" in {
    AvatarUtil.sanitize(Some("https://evil.example.com/a/AVATAR-ID")) shouldBe None
  }

  // A suffix match must not let a lookalike domain through.
  it should "drop a host that merely ends with the allowlisted string" in {
    AvatarUtil.sanitize(Some("https://notgoogleusercontent.com/a/ID")) shouldBe None
  }

  it should "drop a non-http(s) URL" in {
    AvatarUtil.sanitize(Some("javascript:alert(1)")) shouldBe None
    AvatarUtil.sanitize(Some("data:image/png;base64,AAAA")) shouldBe None
  }

  it should "drop a value that is not a URL at all" in {
    AvatarUtil.sanitize(Some("AVATAR-ID")) shouldBe None
    AvatarUtil.sanitize(Some("not a uri at all")) shouldBe None
  }

  it should "map an absent, empty, or blank picture to None" in {
    AvatarUtil.sanitize(None) shouldBe None
    AvatarUtil.sanitize(Some("")) shouldBe None
    AvatarUtil.sanitize(Some("   ")) shouldBe None
  }

  "isAllowedHost" should "match the bare host and its subdomains, case-insensitively" in {
    AvatarUtil.isAllowedHost("googleusercontent.com") shouldBe true
    AvatarUtil.isAllowedHost("LH3.GoogleUserContent.com") shouldBe true
    AvatarUtil.isAllowedHost("example.com") shouldBe false
    AvatarUtil.isAllowedHost("") shouldBe false
    AvatarUtil.isAllowedHost(null) shouldBe false
  }
}
