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
import org.apache.texera.dao.SiteSettings

/**
  * Whether a new account has to wait for an admin's approval before it can be used.
  *
  * The switch is the `invite_only` site setting, which admins flip on the Admin Settings page. It
  * is read on every call, so a change reaches the next sign-up without a restart. default.conf's
  * `user-sys.invite_only` (USER_SYS_INVITE_ONLY) seeds that row and stands in whenever the row is
  * missing, unreadable or not a boolean.
  */
object InviteOnly {

  /** The `site_settings` key, which is also the default.conf leaf that seeds it. */
  val SettingKey = "invite_only"

  def enabled: Boolean = SiteSettings.getBoolean(SettingKey, DefaultsConfig.inviteOnly)
}
