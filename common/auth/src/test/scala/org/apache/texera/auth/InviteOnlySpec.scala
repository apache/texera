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

package org.apache.texera.auth

import org.apache.texera.common.config.DefaultsConfig
import org.apache.texera.dao.{MockTexeraDB, SiteSettings}
import org.apache.texera.dao.jooq.generated.Tables.SITE_SETTINGS
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

/**
  * Spec for [[InviteOnly]]: the switch is the `invite_only` site setting the admin settings page
  * writes, read again on every call, with default.conf's value standing in while no usable row
  * is stored.
  */
class InviteOnlySpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  override protected def beforeAll(): Unit = initializeDBAndReplaceDSLContext()

  override protected def afterAll(): Unit = closeConnectionPool()

  override protected def beforeEach(): Unit =
    getDSLContext
      .deleteFrom(SITE_SETTINGS)
      .where(SITE_SETTINGS.KEY.eq(InviteOnly.SettingKey))
      .execute()

  private def store(value: String): Unit =
    SiteSettings.upsert(getDSLContext, InviteOnly.SettingKey, value, "spec-admin")

  private def configuredDefault: Boolean = DefaultsConfig.inviteOnly

  // The settings API only writes keys that have a default.conf entry, so a key without one could
  // never be switched from the admin page.
  "InviteOnly.SettingKey" should "name a default.conf entry, so the settings API accepts it" in {
    DefaultsConfig.allDefaults should contain key InviteOnly.SettingKey
  }

  "InviteOnly.enabled" should "follow the stored setting, including a change made after startup" in {
    store("true")
    InviteOnly.enabled shouldBe true

    store("false")
    InviteOnly.enabled shouldBe false
  }

  it should "use the default.conf value while no row is stored" in {
    InviteOnly.enabled shouldBe configuredDefault
  }

  it should "use the default.conf value when the stored row is not a boolean" in {
    store("sometimes")
    InviteOnly.enabled shouldBe configuredDefault
  }
}
