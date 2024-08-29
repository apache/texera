package edu.uci.ics.texera.web.resource

import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.resource.aiassistant.AiAssistantManager
import io.dropwizard.auth.Auth

import javax.annotation.security.RolesAllowed
import javax.ws.rs._

import javax.ws.rs.core.Response
import java.util.Base64
import scala.sys.process._
import java.util.logging.Logger

@Path("/aiassistant")
class AiAssistantResource {
  private val logger = Logger.getLogger(classOf[AiAssistantResource].getName)

  final private lazy val isEnabled = AiAssistantManager.validAIAssistant

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/isenabled")
  def isAiAssistantEnabled: Boolean = isEnabled

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
      if (responseCode == 200) {
        logger.info(s"Response from OpenAI API: $responseString")
      } else {
        logger.warning(s"Error response from OpenAI API: $responseString")
      }
      responseStream.close()
      connection.disconnect()
      Response.status(responseCode).entity(responseString).build()
    } catch {
      case e: Exception =>
        logger.warning(s"Exception occurred: ${e.getMessage}")
        e.printStackTrace()
        Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Error occurred").build()
    }
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/getArgument")
  def locateUnannotated(requestBody: AiAssistantResource.LocateUnannotatedRequest): Response = {
    val selectedCode = requestBody.selectedCode
    val startLine = requestBody.startLine

    val encodedCode = Base64.getEncoder.encodeToString(selectedCode.getBytes("UTF-8"))
    try {
      val pythonScriptPath =
        "src/main/scala/edu/uci/ics/texera/web/resource/aiassistant/python_abstract_syntax_tree.py"
      val command = s"""python $pythonScriptPath "$encodedCode" $startLine"""
      val result = command.!!
      // Parse the string to the Json
      val parsedJson = parseJson(result)
      parsedJson match {
        case Some(data: List[List[Any]]) =>
          val unannotatedArgs = data.map {
            case List(
                  name: String,
                  startLine: Double,
                  startColumn: Double,
                  endLine: Double,
                  endColumn: Double
                ) =>
              List(name, startLine.toInt, startColumn.toInt, endLine.toInt, endColumn.toInt)
          }
          logger.info(s"Unannotated arguments: $unannotatedArgs")
          Response.ok(Map("result" -> unannotatedArgs)).build()
        case _ =>
          Response.status(400).entity("Invalid JSON").build()
      }
    } catch {
      case e: Exception =>
        Response.status(500).entity("Error with executing the python code").build()
    }
  }

  def parseJson(jsonString: String): Option[List[List[Any]]] = {
    val cleanJson = jsonString.trim.drop(1).dropRight(1)
    val rows = cleanJson.split("], \\[").toList
    val parsedRows = rows.map { row =>
      val elements = row.replaceAll("[\\[\\]]", "").split(",").toList
      if (elements.length == 5) {
        val name = elements.head.trim.replace("\"", "")
        val startLine = elements(1).trim.toDouble
        val startColumn = elements(2).trim.toDouble
        val endLine = elements(3).trim.toDouble
        val endColumn = elements(4).trim.toDouble
        List(name, startLine, startColumn, endLine, endColumn)
      } else {
        logger.warning("The Json format is wrong")
        List.empty
      }
    }
    val result = parsedRows.filter(_.nonEmpty)
    Some(result)
  }
}

object AiAssistantResource {
  case class LocateUnannotatedRequest(selectedCode: String, startLine: Int)
}
