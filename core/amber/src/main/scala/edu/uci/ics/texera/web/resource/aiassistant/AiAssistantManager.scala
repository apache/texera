package edu.uci.ics.texera.web.resource.aiassistant
import edu.uci.ics.amber.engine.common.AmberConfig
import java.net.{HttpURLConnection, URL}

object AiAssistantManager {
  private val aiAssistantConfig = AmberConfig.aiAssistantConfig
  val assistantType: String = aiAssistantConfig.getString("assistant")
  val accountKey: String = aiAssistantConfig.getString("ai-service-key")
  val validAIAssistant: Boolean = assistantType match {
    case "none" =>
      false

    case "openai" =>
      var isKeyValid: Boolean = false
      var connection: HttpURLConnection = null
      try {
        val url = new URL("https://api.openai.com/v1/models")
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

    case _ =>
      false
  }
}
