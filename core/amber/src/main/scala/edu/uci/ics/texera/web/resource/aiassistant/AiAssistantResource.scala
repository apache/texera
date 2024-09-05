package edu.uci.ics.texera.web.resource
import edu.uci.ics.texera.web.resource.aiassistant.AiAssistantManager
import javax.annotation.security.RolesAllowed
import javax.ws.rs._

@Path("/aiassistant")
class AiAssistantResource {
  final private lazy val isEnabled = AiAssistantManager.validAIAssistant
  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/isenabled")
  def isAiAssistantEnable: String = isEnabled
}
