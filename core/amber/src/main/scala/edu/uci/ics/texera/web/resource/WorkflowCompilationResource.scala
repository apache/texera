package edu.uci.ics.texera.web.resource

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.web.model.websocket.request.LogicalPlanPojo
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute
import edu.uci.ics.texera.workflow.common.workflow.{PhysicalPlan, WorkflowCompiler}
import org.jooq.types.UInteger

import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.MediaType

trait WorkflowCompilationResponse
case class WorkflowCompilationSuccess(
    physicalPlan: PhysicalPlan,
    operatorInputSchemas: Map[String, List[Option[List[Attribute]]]]
) extends WorkflowCompilationResponse

case class WorkflowCompilationFailure(
    operatorErrors: Map[String, String],
    operatorInputSchemas: Map[String, List[Option[List[Attribute]]]]
) extends WorkflowCompilationResponse

@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
@RolesAllowed(Array("REGULAR", "ADMIN"))
@Path("/compile")
class WorkflowCompilationResource extends LazyLogging {

  @POST
  @Path("/{wid}")
  def compileWorkflow(
      logicalPlanPojo: LogicalPlanPojo,
      @PathParam("wid") wid: UInteger
  ): WorkflowCompilationResponse = {
    // Create workflow context from wid
    val context = new WorkflowContext(
      workflowId = WorkflowIdentity(wid.toString.toLong)
    )

    // Compile the pojo using WorkflowCompiler
    val compilationResult = new WorkflowCompiler(context).compile(logicalPlanPojo)

    val operatorInputSchemas = compilationResult.operatorIdToInputSchemas.map {
      case (operatorIdentity, schemas) =>
        val opId = operatorIdentity.id
        val attributes = schemas.map { schema =>
          if (schema.isEmpty)
            None
          else
            Some(schema.get.attributes)
        }
        (opId, attributes)
    }

    // Handle success case: No errors in the compilation result
    if (compilationResult.operatorIdToError.isEmpty && compilationResult.physicalPlan.nonEmpty) {
      WorkflowCompilationSuccess(
        physicalPlan = compilationResult.physicalPlan.get,
        operatorInputSchemas
      )
    }
    // Handle failure case: Errors found during compilation
    else {
      WorkflowCompilationFailure(
        operatorErrors = compilationResult.operatorIdToError.map {
          case (operatorIdentity, error) => (operatorIdentity.id, error.toString)
        },
        operatorInputSchemas
      )
    }
  }
}
