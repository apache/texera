package edu.uci.ics.texera.web.resource
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.resource.aiassistant.AiAssistantManager
import io.dropwizard.auth.Auth
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.Response
import javax.ws.rs.Consumes
import javax.ws.rs.core.MediaType
import play.api.libs.json.Json
import kong.unirest.Unirest
import java.util.Base64
import scala.sys.process._
import play.api.libs.json._
import java.nio.file.Paths
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

case class AIAssistantRequest(code: String, lineNumber: Int, allcode: String)
case class LocateUnannotatedRequest(selectedCode: String, startLine: Int)
case class UnannotatedArgument(
    name: String,
    startLine: Int,
    startColumn: Int,
    endLine: Int,
    endColumn: Int
)
object UnannotatedArgument {
  implicit val format: Format[UnannotatedArgument] = Json.format[UnannotatedArgument]
}

@Path("/aiassistant")
class AIAssistantResource {
  val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)
  final private lazy val isEnabled = AiAssistantManager.validAIAssistant
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
    val requestBodyJson = Json.obj(
      "model" -> "gpt-4",
      "messages" -> Json.arr(
        Json.obj(
          "role" -> "user",
          "content" -> finalPrompt
        )
      ),
      "max_tokens" -> 15
    )

    val response = Unirest
      .post(s"${AiAssistantManager.sharedUrl}/chat/completions")
      .header("Authorization", s"Bearer ${AiAssistantManager.accountKey}")
      .header("Content-Type", "application/json")
      .body(Json.stringify(requestBodyJson))
      .asString()
    if (response.getStatus >= 400) {
      throw new RuntimeException(s"getAnnotation error: ${response.getStatus}: ${response.getBody}")
    }
    Response.status(response.getStatus).entity(response.getBody).build()
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

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/annotate-argument")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def locateUnannotated(request: LocateUnannotatedRequest, @Auth user: SessionUser): Response = {
    // Encoding the code to transmit multi-line code as a single command-line argument
    val encodedCode = Base64.getEncoder.encodeToString(request.selectedCode.getBytes("UTF-8"))
    val pythonScriptPath =
      Paths
        .get(
          "src",
          "main",
          "scala",
          "edu",
          "uci",
          "ics",
          "texera",
          "web",
          "resource",
          "aiassistant",
          "type_annotation_visitor.py"
        )
        .toString

    try {
      val command = s"""python $pythonScriptPath "$encodedCode" ${request.startLine}"""
      val result = command.!!
      val parsedResult = objectMapper.readValue(result, classOf[List[List[Any]]]).map {
        case List(name: String, startLine: Int, startColumn: Int, endLine: Int, endColumn: Int) =>
          UnannotatedArgument(name, startLine, startColumn, endLine, endColumn)
        case _ =>
          throw new RuntimeException("Unexpected format in Python script result")
      }
      Response.ok(Json.obj("result" -> Json.toJson(parsedResult))).build()
    } catch {
      case e: Exception =>
        e.printStackTrace()
        Response.status(500).entity(s"Error executing the Python code: ${e.getMessage}").build()
    }
  }
}
