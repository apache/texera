package edu.uci.ics.texera.web.resource

import edu.uci.ics.texera.web.resource.auth.UserResource
import edu.uci.ics.texera.workflow.common.{Utils, WorkflowContext}
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute
import edu.uci.ics.texera.workflow.common.workflow.{WorkflowCompiler, WorkflowInfo}
import io.dropwizard.jersey.sessions.Session

import javax.servlet.http.HttpSession
import javax.ws.rs.{Consumes, Path, POST, Produces}
import javax.ws.rs.core.MediaType
case class SchemaPropagationResponse(
    code: Int,
    result: Map[String, List[Option[List[Attribute]]]],
    message: String
)

@Path("/queryplan")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class SchemaPropagationResource {

  @POST
  @Path("/autocomplete")
  def suggestAutocompleteSchema(
      @Session httpSession: HttpSession,
      workflowStr: String
  ): SchemaPropagationResponse = {
    try {
      val workflow = Utils.objectMapper.readValue(workflowStr, classOf[WorkflowInfo])

      val context = new WorkflowContext
      context.userID = UserResource
        .getUser(httpSession)
        .map(u => u.getUid)

      val texeraWorkflowCompiler = new WorkflowCompiler(
        WorkflowInfo(workflow.operators, workflow.links, workflow.breakpoints),
        context
      )

      val schemaPropagationResult = texeraWorkflowCompiler
        .propagateWorkflowSchema()
        .map(e => (e._1.operatorID, e._2.map(s => s.map(o => o.getAttributesScala))))
      SchemaPropagationResponse(0, schemaPropagationResult, null)
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        SchemaPropagationResponse(1, null, e.getMessage)
    }
  }

}
