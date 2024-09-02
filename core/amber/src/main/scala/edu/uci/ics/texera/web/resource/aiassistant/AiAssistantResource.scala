package edu.uci.ics.texera.web.resource
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.resource.aiassistant.AiAssistantManager
import io.dropwizard.auth.Auth
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.Response

@Path("/aiassistant")
class AiAssistantResource {
  final private lazy val isEnabled = AiAssistantManager.validAIAssistant
  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/isenabled")
  def isAiAssistantEnable: String = isEnabled

  /**
    * To get the type annotation suggestion from OpenAI
    */
  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/getresult")
  def getAiResponse(prompt: String, @Auth user: SessionUser): Response = {
    val finalPrompt = prompt.replace("\\", "\\\\").replace("\"", "\\\"")
    val requestBody =
      s"""
         |{
         |  "model": "gpt-4",
         |  "messages": [{"role": "user", "content": "$finalPrompt"}],
         |  "max_tokens": 15
         |}
            """.stripMargin

    try {
      val url = new java.net.URL("https://api.openai.com/v1/chat/completions")
      val connection = url.openConnection().asInstanceOf[java.net.HttpURLConnection]
      connection.setRequestMethod("POST")
      connection.setRequestProperty("Authorization", s"Bearer ${AiAssistantManager.accountKey}")
      connection.setRequestProperty("Content-Type", "application/json")
      connection.setDoOutput(true)
      connection.getOutputStream.write(requestBody.getBytes("UTF-8"))
      val responseCode = connection.getResponseCode
      val responseStream = connection.getInputStream
      val responseString = scala.io.Source.fromInputStream(responseStream).mkString
      responseStream.close()
      connection.disconnect()
      Response.status(responseCode).entity(responseString).build()
    } catch {
      case e: Exception =>
        e.printStackTrace()
        Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Error occurred").build()
    }
  }
}
