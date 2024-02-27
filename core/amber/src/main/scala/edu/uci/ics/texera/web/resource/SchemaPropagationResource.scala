package edu.uci.ics.texera.web.resource
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.http.response.SchemaPropagationResponse
import edu.uci.ics.texera.web.model.websocket.request.LogicalPlanPojo
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.workflow.LogicalPlan
import io.dropwizard.auth.Auth
import org.jooq.types.UInteger

import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.MediaType

@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/queryplan")
class SchemaPropagationResource extends LazyLogging {

  @POST
  @Path("/autocomplete/{wid}")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def suggestAutocompleteSchema(
      workflowStr: String,
      @PathParam("wid") wid: UInteger,
      @Auth sessionUser: SessionUser
  ): SchemaPropagationResponse = {

    val logicalPlanPojo = Utils.objectMapper.readValue(workflowStr, classOf[LogicalPlanPojo])

    val context = new WorkflowContext(
      userId = Option(sessionUser.getUser.getUid),
      workflowId = WorkflowIdentity(wid.toString.toLong)
    )

    val logicalPlan = LogicalPlan(logicalPlanPojo)

    // ignore errors during propagation. errors are reported through EditingTimeCompilationRequest
    logicalPlan.propagateWorkflowSchema(context, errorList = None)
    val responseContent = logicalPlan.getInputSchemaMap
      .map(e => (e._1.id, e._2.map(s => s.map(o => o.getAttributes))))
    SchemaPropagationResponse(0, responseContent, null)

  }

}
