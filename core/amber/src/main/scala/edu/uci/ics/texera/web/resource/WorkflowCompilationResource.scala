package edu.uci.ics.texera.web.resource

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.http.response.SchemaPropagationResponse
import edu.uci.ics.texera.web.model.websocket.request.LogicalPlanPojo
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute
import edu.uci.ics.texera.workflow.common.workflow.{PhysicalPlan, WorkflowCompiler}
import io.dropwizard.auth.Auth
import org.jooq.types.UInteger

import javax.annotation.security.RolesAllowed
import javax.ws.rs.{Consumes, POST, Path, PathParam, Produces}
import javax.ws.rs.core.MediaType

case class WorkflowCompilationResponse(
    physicalPlan: PhysicalPlan,
    operatorInputSchemas: Map[String, List[Option[List[Attribute]]]],
    operatorErrors: Map[String, String]
)

@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/compilation")
class WorkflowCompilationResource extends LazyLogging {
  @POST
  @Path("/{wid}")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def compileWorkflow(
      workflowStr: String,
      @PathParam("wid") wid: UInteger,
      @Auth sessionUser: SessionUser
  ): WorkflowCompilationResponse = {
    val logicalPlanPojo = Utils.objectMapper.readValue(workflowStr, classOf[LogicalPlanPojo])

    val context = new WorkflowContext(
      userId = Option(sessionUser.getUser.getUid),
      workflowId = WorkflowIdentity(wid.toString.toLong)
    )

    // compile the pojo
    val workflowCompilationResult = new WorkflowCompiler(context).compileToPhysicalPlan(logicalPlanPojo)

    // return the result
    WorkflowCompilationResponse(
      physicalPlan = workflowCompilationResult.physicalPlan,
      operatorInputSchemas = workflowCompilationResult.operatorIdToInputSchemas.map {
        case (operatorIdentity, schemas) =>
          val opId = operatorIdentity.id
          val attributes = schemas.map { schema =>
            if (schema.isEmpty)
              None
            else
              Some(schema.get.attributes)
          }

          (opId, attributes)
      },
      operatorErrors = workflowCompilationResult.operatorIdToError.map {
        case (operatorIdentity, error) => (operatorIdentity.id, error.toString)
      }
    )
  }
}
