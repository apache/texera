package edu.uci.ics.texera.web.resource.aiassistant
import edu.uci.ics.amber.engine.common.AmberConfig
import java.net.{HttpURLConnection, URL}

object AiAssistantManager {
  private val aiAssistantConfig = AmberConfig.aiAssistantConfig
  val assistantType: String = aiAssistantConfig.getString("assistant")
  // The accountKey is the OpenAI authentication key used to authenticate API requests and obtain responses from the OpenAI service.

  val accountKey: String = aiAssistantConfig.getString("ai-service-key")
  val sharedUrl: String = aiAssistantConfig.getString("ai-service-url")

  private def initOpenAI(): Boolean = {
    var isKeyValid: Boolean = false
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
      isKeyValid = responseCode == 200
    } catch {
      case e: Exception =>
        isKeyValid = false
    } finally {
      if (connection != null) {
        connection.disconnect()
      }
    }
    isKeyValid
  }

  val validAIAssistant: Boolean = assistantType match {
    case "none" =>
      false

    case "openai" =>
      initOpenAI()

    case _ =>
      false
  }
}
