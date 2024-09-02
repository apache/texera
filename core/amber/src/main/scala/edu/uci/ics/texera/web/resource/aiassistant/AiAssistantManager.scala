package edu.uci.ics.texera.web.resource.aiassistant
import edu.uci.ics.amber.engine.common.AmberConfig
import java.net.{HttpURLConnection, URL}

object AiAssistantManager {
  private val aiAssistantConfig = AmberConfig.aiAssistantConfig.getOrElse(
    throw new Exception("ai-assistant-server configuration is missing in application.conf")
  )
  val assistantType: String = aiAssistantConfig.getString("assistant")
  // The accountKey is the OpenAI authentication key used to authenticate API requests and obtain responses from the OpenAI service.

  val accountKey: String = aiAssistantConfig.getString("ai-service-key")
  val sharedUrl: String = aiAssistantConfig.getString("ai-service-url")

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

  val validAIAssistant: String = assistantType match {
    case "none" =>
      "NoAiAssistant"

    case "openai" =>
      initOpenAI()

    case _ =>
      "NoAiAssistant"
  }
}
