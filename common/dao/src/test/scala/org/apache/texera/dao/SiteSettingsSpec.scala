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

import org.apache.texera.dao.jooq.generated.Tables.SITE_SETTINGS
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SiteSettingsSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  // Point SqlServer (used by SiteSettings.getInt/getLong internally) and the test's
  // own getDSLContext at the same embedded Postgres.
  override protected def beforeAll(): Unit = {
    super.beforeAll()
    initializeDBAndReplaceDSLContext()
  }

  override protected def afterAll(): Unit =
    try closeConnectionPool()
    finally super.afterAll()

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    getDSLContext.deleteFrom(SITE_SETTINGS).execute()
  }

  private def storedUpdatedBy(key: String): String =
    getDSLContext
      .select(SITE_SETTINGS.UPDATED_BY)
      .from(SITE_SETTINGS)
      .where(SITE_SETTINGS.KEY.eq(key))
      .fetchOneInto(classOf[String])

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

  "getInt" should "return the stored value when the key is present" in {
    SiteSettings.upsert(getDSLContext, "max_x", "42", "admin")
    SiteSettings.getInt("max_x", 0) shouldBe 42
  }

  it should "return the default when the key is absent" in {
    SiteSettings.getInt("missing_key", 7) shouldBe 7
  }

  it should "return the default when the stored value is not numeric" in {
    SiteSettings.upsert(getDSLContext, "bad_int", "not-a-number", "admin")
    SiteSettings.getInt("bad_int", 7) shouldBe 7
  }

  "getLong" should "return the stored long value when the key is present" in {
    SiteSettings.upsert(getDSLContext, "big", "9999999999", "admin")
    SiteSettings.getLong("big", 0L) shouldBe 9999999999L
  }

  "upsert" should "insert a new row and then overwrite it with the latest value and writer" in {
    SiteSettings.upsert(getDSLContext, "k", "1", "admin")
    SiteSettings.getInt("k", 0) shouldBe 1

    SiteSettings.upsert(getDSLContext, "k", "2", "admin2")
    SiteSettings.getInt("k", 0) shouldBe 2
    storedUpdatedBy("k") shouldBe "admin2"
  }

  "insertIfAbsent" should "insert when absent but leave an existing value untouched" in {
    SiteSettings.insertIfAbsent(getDSLContext, "seed", "10", "seeder")
    SiteSettings.getInt("seed", 0) shouldBe 10

    SiteSettings.insertIfAbsent(getDSLContext, "seed", "20", "seeder2")
    SiteSettings.getInt("seed", 0) shouldBe 10
    storedUpdatedBy("seed") shouldBe "seeder"
  }
}
