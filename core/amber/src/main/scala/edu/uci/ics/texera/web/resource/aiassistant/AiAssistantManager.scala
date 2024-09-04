package edu.uci.ics.texera.web.resource.aiassistant

import edu.uci.ics.amber.engine.common.AmberConfig
import java.net.{HttpURLConnection, URL}
import com.typesafe.config.Config

object AiAssistantManager {
  // Optionally retrieve the configuration
  private val aiAssistantConfigOpt: Option[Config] = AmberConfig.aiAssistantConfig

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
        case "none"   => "NoAiAssistant"
        case "openai" => initOpenAI()
        case _        => "NoAiAssistant"
      }
    case None =>
      "NoAiAssistant"
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
        "NoAiAssistant"
      }
    } catch {
      case e: Exception =>
        "NoAiAssistant"
    } finally {
      if (connection != null) {
        connection.disconnect()
      }
    }
  }
}
