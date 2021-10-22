package edu.uci.ics.texera.web.resource

import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.http.response.SchemaPropagationResponse
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.workflow.{WorkflowCompiler, WorkflowInfo}
import io.dropwizard.auth.Auth

import javax.annotation.security.PermitAll
import javax.ws.rs.core.MediaType
import javax.ws.rs.{Consumes, POST, Path, Produces}

@PermitAll
@Path("/queryplan")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class SchemaPropagationResource {

  @POST
  @Path("/autocomplete")
  def suggestAutocompleteSchema(
      workflowStr: String,
      @Auth sessionUser: SessionUser
  ): SchemaPropagationResponse = {
    try {
      val workflow = Utils.objectMapper.readValue(workflowStr, classOf[WorkflowInfo])

      val context = new WorkflowContext
      context.userId = Option(sessionUser.getUser.getUid)

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
