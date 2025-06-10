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

package edu.uci.ics.texera.config

import com.typesafe.config.{Config, ConfigFactory}

import java.util.logging.Logger

object UserSystemConfig {
  private val conf: Config = ConfigFactory.parseResources("user-system.conf").resolve()
  private val logger = Logger.getLogger(getClass.getName)

  // User system
  val isUserSystemEnabled: Boolean = conf.getBoolean("user-sys.enabled")
  val adminUsername: String = conf.getString("user-sys.admin-username")
  val adminPassword: String = conf.getString("user-sys.admin-password")
  val googleClientId: String = conf.getString("user-sys.google.clientId")
  val gmail: String = conf.getString("user-sys.google.smtp.gmail")
  val smtpPassword: String = conf.getString("user-sys.google.smtp.password")
  val inviteOnly: Boolean = conf.getBoolean("user-sys.invite-only")
  val workflowVersionCollapseIntervalInMinutes: Int =
    conf.getInt("user-sys.version-time-limit-in-minutes")
  val appDomain: Option[String] = {
    val domain = conf.getString("user-sys.domain").trim
    if (domain.isEmpty) {
      logger.warning(
        """
          |=======================================================
          |[WARN] The user-sys.domain is not configured, and the "Sent from:" field in the email will not include a domain!
          |Please configure user-sys.domain=your.domain.com or set the environment variable/system property USER_SYS_DOMAIN
          |=======================================================
          |""".stripMargin
      )
      None
    } else {
      Some(domain)
    }
  }
}
