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

package edu.uci.ics.texera.auth

import com.typesafe.config.{Config, ConfigFactory}
import java.util.Random

object AuthConfig {
  // Load configuration
  private val conf: Config = ConfigFactory.parseResources("auth.conf").resolve()

  // Read JWT expiration time
  val jwtExpirationDays: Int = conf.getInt("auth.jwt.exp-in-days")

  // For storing the generated/configured secret
  @volatile private var secretKey: String = _

  // Read JWT secret key with support for random generation
  def jwtSecretKey: String = {
    synchronized {
      if (secretKey == null) {
        secretKey = conf.getString("auth.jwt.256-bit-secret").toLowerCase() match {
          case "random" => getRandomHexString
          case key      => key
        }
      }
    }
    secretKey
  }

  private def getRandomHexString: String = {
    val bytes = 32
    val r = new Random()
    val sb = new StringBuffer
    while (sb.length < bytes)
      sb.append(Integer.toHexString(r.nextInt()))
    sb.toString.substring(0, bytes)
  }
}
