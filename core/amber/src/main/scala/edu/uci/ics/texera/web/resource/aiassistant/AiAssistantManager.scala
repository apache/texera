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

package edu.uci.ics.texera.web.resource.aiassistant

import com.typesafe.config.Config
import edu.uci.ics.amber.config.ApplicationConfig

import java.net.{HttpURLConnection, URL}

object AiAssistantManager {
  // Optionally retrieve the configuration
  private val aiAssistantConfigOpt: Option[Config] = ApplicationConfig.aiAssistantConfig
  private val noAssistant: String = "NoAiAssistant"
  // Public variables, accessible from outside the object
  var accountKey: String = _
  var sharedUrl: String = _

  // Initialize accountKey and sharedUrl if the configuration is present
  aiAssistantConfigOpt.foreach { aiAssistantConfig =>
    accountKey = aiAssistantConfig.getString("ai-service-key")
    sharedUrl = aiAssistantConfig.getString("ai-service-url")
  }

  val validAIAssistant: String = aiAssistantConfigOpt match {
    case Some(aiAssistantConfig) =>
      val assistantType: String = aiAssistantConfig.getString("assistant")
      assistantType match {
        case "none"   => noAssistant
        case "openai" => initOpenAI()
        case _        => noAssistant
      }
    case None =>
      noAssistant
  }

  private def initOpenAI(): String = {
    var connection: HttpURLConnection = null
    try {
      val url = new URL(s"${sharedUrl}/models")
      connection = url.openConnection().asInstanceOf[HttpURLConnection]
      connection.setRequestMethod("GET")
      connection.setRequestProperty(
        "Authorization",
        s"Bearer ${accountKey.trim.replaceAll("^\"|\"$", "")}"
      )
      val responseCode = connection.getResponseCode
      if (responseCode == 200) {
        "OpenAI"
      } else {
        noAssistant
      }
    } catch {
      case e: Exception =>
        noAssistant
    } finally {
      if (connection != null) {
        connection.disconnect()
      }
    }
  }
}
