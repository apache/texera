package edu.uci.ics.texera.web.resource
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.Utils
import edu.uci.ics.amber.engine.common.model.{PhysicalPlan, WorkflowContext}
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.http.response.SchemaPropagationResponse
import edu.uci.ics.texera.web.model.websocket.request.LogicalPlanPojo
import edu.uci.ics.texera.workflow.common.workflow.LogicalPlan
import io.dropwizard.auth.Auth
import org.jooq.types.UInteger

import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.MediaType

/**
  * The SchemaPropagation functionality will be included by the standalone compiling service
  */
@Deprecated
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/queryplan")
class SchemaPropagationResource extends LazyLogging {

  @Deprecated
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
      workflowId = WorkflowIdentity(wid.toString.toLong)
    )

    val logicalPlan = LogicalPlan(logicalPlanPojo)
    logicalPlan.resolveScanSourceOpFileName(None)

    // the PhysicalPlan with topology expanded.
    val physicalPlan = PhysicalPlan(context, logicalPlan)

    // Extract physical input schemas, excluding internal ports
    val physicalInputSchemas = physicalPlan.operators.map { physicalOp =>
      physicalOp.id -> physicalOp.inputPorts.values
        .filterNot(_._1.id.internal)
        .map {
          case (port, _, schema) => port.id -> schema.toOption
        }
    }

    // Group the physical input schemas by their logical operator ID and consolidate the schemas
    val logicalInputSchemas = physicalInputSchemas
      .groupBy(_._1.logicalOpId)
      .view
      .mapValues(_.flatMap(_._2).toList.sortBy(_._1.id).map(_._2))
      .toMap

    // Prepare the response content by extracting attributes from the schemas,
    // ignoring errors (errors are reported through EditingTimeCompilationRequest)
    val responseContent = logicalInputSchemas.map {
      case (logicalOpId, schemas) =>
        logicalOpId.id -> schemas.map(_.map(_.getAttributes))
    }
    SchemaPropagationResponse(0, responseContent, null)

  }

}
