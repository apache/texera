package edu.uci.ics.texera.web.resource.dashboard.admin.execution

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables._
import edu.uci.ics.texera.web.resource.dashboard.admin.execution.AdminExecutionResource._
import io.dropwizard.auth.Auth
import org.jooq.types.UInteger

import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.jdk.CollectionConverters.IterableHasAsScala

/**
  * This file handles various request related to saved-executions.
  */

object AdminExecutionResource {
  final private lazy val context = SqlServer.createDSLContext()

  case class dashboardExecution(
      workflowName: String,
      workflowId: UInteger,
      userName: String,
      userId: UInteger,
      executionId: UInteger,
      executionStatus: String,
      executionTime: Double,
      executionName: String,
      startTime: Long,
      endTime: Long,
      access: Boolean
  )

  def mapToName(code: Byte): String = {
    code match {
      case 0 => "READY"
      case 1 => "RUNNING"
      case 2 => "PAUSED"
      case 3 => "COMPLETED"
      case 4 => "FAILED"
      case 5 => "KILLED"
      case _ => "UNKNOWN" // or throw an exception, depends on your needs
    }
  }

}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/admin/execution")
@RolesAllowed(Array("ADMIN"))
class AdminExecutionResource {

  /**
    * This method retrieves all existing executions
    */
  @GET
  @Path("/executionList")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def listWorkflows(@Auth current_user: SessionUser): List[dashboardExecution] = {
    val workflowEntries = context
      .select(
        WORKFLOW_EXECUTIONS.UID,
        USER.NAME,
        WORKFLOW_VERSION.WID,
        WORKFLOW.NAME,
        WORKFLOW_EXECUTIONS.EID,
        WORKFLOW_EXECUTIONS.VID,
        WORKFLOW_EXECUTIONS.STARTING_TIME,
        WORKFLOW_EXECUTIONS.LAST_UPDATE_TIME,
        WORKFLOW_EXECUTIONS.STATUS,
        WORKFLOW_EXECUTIONS.NAME
      )
      .from(WORKFLOW_EXECUTIONS)
      .leftJoin(WORKFLOW_VERSION)
      .on(WORKFLOW_EXECUTIONS.VID.eq(WORKFLOW_VERSION.VID))
      .leftJoin(USER)
      .on(WORKFLOW_EXECUTIONS.UID.eq(USER.UID))
      .leftJoin(WORKFLOW)
      .on(WORKFLOW.WID.eq(WORKFLOW_VERSION.WID))
      .fetch()

    val availableWorkflowIds = context
      .select(WORKFLOW_USER_ACCESS.WID)
      .from(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.UID.eq(current_user.getUid))
      .fetchInto(classOf[UInteger])

    workflowEntries
      .map(workflowRecord => {
        val startingTime =
          workflowRecord.get(WORKFLOW_EXECUTIONS.STARTING_TIME).getTime

        var lastUpdateTime: Long = 0
        if (workflowRecord.get(WORKFLOW_EXECUTIONS.LAST_UPDATE_TIME) == null) {
          lastUpdateTime = 0
        } else {
          lastUpdateTime = workflowRecord.get(WORKFLOW_EXECUTIONS.LAST_UPDATE_TIME).getTime
        }

        val timeDifferenceSeconds = (lastUpdateTime - startingTime) / 1000.0
        val hasAccess = availableWorkflowIds.contains(workflowRecord.get(WORKFLOW_VERSION.WID))
        dashboardExecution(
          workflowRecord.get(WORKFLOW.NAME),
          workflowRecord.get(WORKFLOW_VERSION.WID),
          workflowRecord.get(USER.NAME),
          workflowRecord.get(WORKFLOW_EXECUTIONS.UID),
          workflowRecord.get(WORKFLOW_EXECUTIONS.EID),
          mapToName(workflowRecord.get(WORKFLOW_EXECUTIONS.STATUS)),
          timeDifferenceSeconds,
          workflowRecord.get(WORKFLOW_EXECUTIONS.NAME),
          startingTime,
          lastUpdateTime,
          hasAccess
        )
      })
      .asScala
      .toList
  }
}
