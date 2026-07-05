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

package org.apache.texera.service.resource

import org.apache.texera.auth.SessionUser
import org.apache.texera.common.config.DefaultsConfig
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.SITE_SETTINGS
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// Positive-path coverage for the /config/settings endpoints against an
// embedded Postgres: ConfigResourceAuthSpec pins the auth gates (401/403),
// this spec exercises the bodies — read-miss, insert, upsert-on-conflict,
// reset-to-default — by calling the resource methods directly.
class ConfigSettingsCrudSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with MockTexeraDB {

  private val resource = new ConfigResource

  private def adminSession(name: String = "test-admin"): SessionUser = {
    val u = new User()
    u.setUid(1)
    u.setName(name)
    new SessionUser(u)
  }

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

  "GET /config/settings/{key}" should "return null for a key that has no row" in {
    resource.getSetting("no-such-key") shouldBe null
  }

  "PUT /config/settings/{key}" should "insert a new row and record who wrote it" in {
    val response =
      resource.updateSetting(adminSession(), "logo", ConfigSettingPojo("logo", "custom.png"))
    response.getStatus shouldBe 200

    val stored = resource.getSetting("logo")
    stored.settingKey shouldBe "logo"
    stored.settingValue shouldBe "custom.png"

    getDSLContext
      .select(SITE_SETTINGS.UPDATED_BY)
      .from(SITE_SETTINGS)
      .where(SITE_SETTINGS.KEY.eq("logo"))
      .fetchOne(SITE_SETTINGS.UPDATED_BY) shouldBe "test-admin"
  }

  it should "update the existing row on a repeated PUT (upsert conflict path)" in {
    resource.updateSetting(adminSession(), "logo", ConfigSettingPojo("logo", "v1.png"))
    resource.updateSetting(
      adminSession("second-admin"),
      "logo",
      ConfigSettingPojo("logo", "v2.png")
    )

    resource.getSetting("logo").settingValue shouldBe "v2.png"
    getDSLContext.fetchCount(SITE_SETTINGS, SITE_SETTINGS.KEY.eq("logo")) shouldBe 1
    getDSLContext
      .select(SITE_SETTINGS.UPDATED_BY)
      .from(SITE_SETTINGS)
      .where(SITE_SETTINGS.KEY.eq("logo"))
      .fetchOne(SITE_SETTINGS.UPDATED_BY) shouldBe "second-admin"
  }

  it should "be a no-op when the payload carries a null value" in {
    resource.updateSetting(adminSession(), "logo", ConfigSettingPojo("logo", "kept.png"))
    val response = resource.updateSetting(adminSession(), "logo", ConfigSettingPojo("logo", null))
    response.getStatus shouldBe 200
    resource.getSetting("logo").settingValue shouldBe "kept.png"
  }

  "GET /config/settings/public" should "serve whitelisted keys and hide management-only ones" in {
    resource.updateSetting(adminSession(), "favicon", ConfigSettingPojo("favicon", "fav.ico"))
    resource.updateSetting(
      adminSession(),
      "csv_parser_max_columns",
      ConfigSettingPojo("csv_parser_max_columns", "4096")
    )

    val publicSettings = resource.getPublicSettings
    publicSettings("favicon") shouldBe "fav.ico"
    publicSettings should not contain key("csv_parser_max_columns")
  }

  "POST /config/settings/reset/{key}" should "restore the default.conf value for a known key" in {
    resource.updateSetting(adminSession(), "logo", ConfigSettingPojo("logo", "overridden.png"))

    val response = resource.resetSetting(adminSession(), "logo")
    response.getStatus shouldBe 200
    resource.getSetting("logo").settingValue shouldBe DefaultsConfig.allDefaults("logo")
  }

  it should "return 404 for a key that has no default" in {
    resource.resetSetting(adminSession(), "no-such-key").getStatus shouldBe 404
  }
}
