package edu.uci.ics.texera.web.resource.dashboard.workflow

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{WORKFLOW, WORKFLOW_EXECUTIONS}
import edu.uci.ics.texera.web.resource.dashboard.workflow.WorkflowExecutionsResource.{
  WorkflowExecutionEntry,
  context
}
import io.dropwizard.auth.Auth
import org.jooq.types.UInteger

import java.sql.Timestamp
import javax.annotation.security.PermitAll
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object WorkflowExecutionsResource {
  final private lazy val context = SqlServer.createDSLContext()

  case class WorkflowExecutionEntry(
      eId: UInteger,
      vId: UInteger,
      startingTime: Timestamp,
      completionTime: Timestamp,
      status: Byte,
      result: String
  )

}

@PermitAll
@Path("/executions")
@Produces(Array(MediaType.APPLICATION_JSON))
class WorkflowExecutionsResource {

  /**
    * This method returns the executions of a workflow given by its ID
    *
    * @return executions[]
    */
  @GET
  @Path("/{wid}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def retrieveExecutionsOfWorkflow(
      @PathParam("wid") wid: UInteger,
      @Auth sessionUser: SessionUser
  ): List[WorkflowExecutionEntry] = {
    val user = sessionUser.getUser
    if (
      WorkflowAccessResource.hasNoWorkflowAccess(wid, user.getUid) ||
      WorkflowAccessResource.hasNoWorkflowAccessRecord(wid, user.getUid)
    ) {
      List()
    } else {
      context
        .select(
          WORKFLOW_EXECUTIONS.EID,
          WORKFLOW_EXECUTIONS.VID,
          WORKFLOW_EXECUTIONS.STARTING_TIME,
          WORKFLOW_EXECUTIONS.COMPLETION_TIME,
          WORKFLOW_EXECUTIONS.STATUS,
          WORKFLOW_EXECUTIONS.RESULT
        )
        .from(WORKFLOW_EXECUTIONS)
        .leftJoin(WORKFLOW)
        .on(WORKFLOW_EXECUTIONS.WID.eq(WORKFLOW.WID))
        .where(WORKFLOW_EXECUTIONS.WID.eq(wid))
        .fetchInto(classOf[WorkflowExecutionEntry])
        .toList
    }
  }
}
