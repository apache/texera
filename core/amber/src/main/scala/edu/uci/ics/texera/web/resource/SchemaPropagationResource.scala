package edu.uci.ics.texera.web.resource
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.http.response.SchemaPropagationResponse
import edu.uci.ics.texera.web.model.websocket.request.LogicalPlanPojo
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.workflow.{LogicalPlan, WorkflowCompiler}
import io.dropwizard.auth.Auth
import org.jooq.types.UInteger

import javax.annotation.security.RolesAllowed
import javax.ws.rs.core.MediaType
import javax.ws.rs.{Consumes, POST, Path, PathParam, Produces}

@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/queryplan")
class SchemaPropagationResource {

  @POST
  @Path("/autocomplete/{wid}")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def suggestAutocompleteSchema(
      workflowStr: String,
      @PathParam("wid") wid: UInteger,
      @Auth sessionUser: SessionUser
  ): SchemaPropagationResponse = {
    try {
      val workflow = Utils.objectMapper.readValue(workflowStr, classOf[LogicalPlanPojo])

      val context = new WorkflowContext
      context.userId = Option(sessionUser.getUser.getUid)
      context.wId = wid

      val texeraWorkflowCompiler = new WorkflowCompiler(LogicalPlan(workflow), context)

      // ignore errors during propagation.
      val (schemaPropagationResult, _) =
        texeraWorkflowCompiler.logicalPlan.propagateWorkflowSchema()
      val responseContent = schemaPropagationResult.map(e =>
        (e._1.operator, e._2.map(s => s.map(o => o.getAttributesScala)))
      )
      // remove internal incremental computation columns
      val responseContentCleaned = responseContent.map(kv => {
        val schemaWithoutInternalAttrs = kv._2.map(portSchema => {
          portSchema.map(attrs => attrs.filter(attr => !attr.getName.startsWith("__internal")))
        })
        (kv._1, schemaWithoutInternalAttrs)
      })
      SchemaPropagationResponse(0, responseContentCleaned, null)
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        SchemaPropagationResponse(1, null, e.getMessage)
    }
  }

}
