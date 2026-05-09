/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.texera.dao

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SiteSettingsSpec extends AnyFlatSpec with Matchers {

  "parseOrDefault" should "return the parsed value when the raw string is present and valid" in {
    SiteSettings.parseOrDefault(Some("42"), 0)(_.toInt) shouldBe 42
  }

  it should "return the default when the Option is None" in {
    SiteSettings.parseOrDefault(None, 99)(_.toInt) shouldBe 99
  }

  it should "return the default when the string cannot be parsed" in {
    SiteSettings.parseOrDefault(Some("not-a-number"), 7)(_.toInt) shouldBe 7
  }

  it should "trim whitespace before parsing" in {
    SiteSettings.parseOrDefault(Some("  100  "), 0)(_.toInt) shouldBe 100
  }

  it should "work for Long values" in {
    SiteSettings.parseOrDefault(Some("9999999999"), 0L)(_.toLong) shouldBe 9999999999L
  }
}
