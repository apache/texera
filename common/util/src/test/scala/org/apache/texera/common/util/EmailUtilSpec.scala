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

class EmailUtilSpec extends AnyFlatSpec with Matchers {

  "isValid" should "accept a well-formed address" in {
    EmailUtil.isValid("user@example.com") shouldBe true
    EmailUtil.isValid("first.last+tag@sub.example.co") shouldBe true
  }

  it should "reject malformed addresses" in {
    EmailUtil.isValid("not-an-email") shouldBe false
    EmailUtil.isValid("missing@tld") shouldBe false
    EmailUtil.isValid("two words@example.com") shouldBe false
    EmailUtil.isValid("") shouldBe false
  }

  "normalize" should "trim whitespace and lowercase" in {
    EmailUtil.normalize("  User@Example.COM  ") shouldBe "user@example.com"
    EmailUtil.normalize("already@lower.case") shouldBe "already@lower.case"
  }
}
