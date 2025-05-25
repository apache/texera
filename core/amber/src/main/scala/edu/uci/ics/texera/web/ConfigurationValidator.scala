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

package edu.uci.ics.texera.web

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.AmberConfig

object ConfigurationValidator extends LazyLogging {

  def validateConfiguration(): Unit = {
    if (AmberConfig.appDomain.isEmpty) {
      logger.warn(
        s"""
           |=======================================================
           |[WARN] The user-sys.google.domain is not configured, and the "Sent from:" field in the email will not include a domain!
           |Please configure user-sys.google.domain=your.domain.com or set the environment variable/system property APP_DOMAIN.
           |=======================================================
           |""".stripMargin
      )
    }
  }
}
