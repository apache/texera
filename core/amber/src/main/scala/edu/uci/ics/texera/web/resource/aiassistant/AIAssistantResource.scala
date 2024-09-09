package edu.uci.ics.texera.web.resource
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.resource.aiassistant.AIAssistantManager
import io.dropwizard.auth.Auth
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.Response
import javax.ws.rs.Consumes
import javax.ws.rs.core.MediaType
import play.api.libs.json.{Json, JsValue}

case class AIAssistantRequest(code: String, lineNumber: Int, allcode: String)

@Path("/aiassistant")
class AIAssistantResource {
  final private lazy val isEnabled = AIAssistantManager.validAIAssistant
  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/isenabled")
  def isAIAssistantEnable: String = isEnabled

  /**
    * To get the type annotation suggestion from OpenAI
    */
  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/annotationresult")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def getAnnotation(
                     request: AIAssistantRequest,
                     @Auth user: SessionUser
                   ): Response = {
    val finalPrompt = generatePrompt(request.code, request.lineNumber, request.allcode)
    val requestBodyJson: JsValue = Json.obj(
      "model" -> "gpt-4",
      "messages" -> Json.arr(
        Json.obj(
          "role" -> "user",
          "content" -> finalPrompt
        )
      ),
      "max_tokens" -> 15
    )

    val requestBodyString = Json.stringify(requestBodyJson)
    try {
      val url = new java.net.URL(s"${AIAssistantManager.sharedUrl}/chat/completions")
      val connection = url.openConnection().asInstanceOf[java.net.HttpURLConnection]
      connection.setRequestMethod("POST")
      connection.setRequestProperty("Authorization", s"Bearer ${AIAssistantManager.accountKey}")
      connection.setRequestProperty("Content-Type", "application/json")
      connection.setDoOutput(true)
      connection.getOutputStream.write(requestBodyString.getBytes("UTF-8"))
      val responseCode = connection.getResponseCode
      val responseStream = if (responseCode >= 200 && responseCode < 300) {
        connection.getInputStream
      } else {
        connection.getErrorStream
      }
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

  // Helper function to get the type annotation
  def generatePrompt(code: String, lineNumber: Int, allcode: String): String = {
    s"""
       |Your task is to analyze the given Python code and provide only the type annotation as stated in the instructions.
       |Instructions:
       |- The provided code will only be one of the 2 situations below:
       |- First situation: The input is not start with "def". If the provided code only contains variable, output the result in the format ":type".
       |- Second situation: The input is start with "def". If the provided code starts with "def" (a longer line than just a variable, indicative of a function or method), output the result in the format " -> type".
       |- The type should only be one word, such as "str", "int", etc.
       |Examples:
       |- First situation:
       |    - Provided code is "name", then the output may be : str
       |    - Provided code is "age", then the output may be : int
       |    - Provided code is "data", then the output may be : Tuple[int, str]
       |    - Provided code is "new_user", then the output may be : User
       |    - A special case: provided code is "self" and the context is something like "def __init__(self, username :str , age :int)", if the user requires the type annotation for the first parameter "self", then you should generate nothing.
       |- Second situation: (actual output depends on the complete code content)
       |    - Provided code is "process_data(data: List[Tuple[int, str]], config: Dict[str, Union[int, str]])", then the output may be -> Optional[str]
       |    - Provided code is "def add(a: int, b: int)", then the output may be -> int
       |Counterexamples:
       |    - Provided code is "def __init__(self, username: str, age: int)" and you generate the result:
       |    The result is The provided code is "def __init__(self, username: str, age: int)", so it fits the second situation, which means the result should be in " -> type" format. However, the __init__ method in Python doesn't return anything or in other words, it implicitly returns None. Hence the correct type hint would be: -> None.
       |Details:
       |- Provided code: $code
       |- Line number of the provided code in the complete code context: $lineNumber
       |- Complete code context: $allcode
       |Important: (you must follow!!)
       |- For the first situation: you must return strictly according to the format ": type", without adding any extra characters. No need for an explanation, just the result : type is enough!
       |- For the second situation: you return strictly according to the format " -> type", without adding any extra characters. No need for an explanation, just the result -> type is enough!
     """.stripMargin
  }
}
