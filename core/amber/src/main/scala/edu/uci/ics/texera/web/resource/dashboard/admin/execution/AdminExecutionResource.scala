package edu.uci.ics.texera.web.resource.dashboard.admin.execution

import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.jooq.generated.Tables._
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.resource.dashboard.admin.execution.AdminExecutionResource._
import io.dropwizard.auth.Auth
import org.jooq.impl.DSL

import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.jdk.CollectionConverters._

/**
  * This file handles various request related to saved-executions.
  */

object AdminExecutionResource {
  final private lazy val context = SqlServer
    .getInstance()
    .createDSLContext()

  case class dashboardExecution(
      workflowName: String,
      workflowId: Integer,
      userName: String,
      userId: Integer,
      executionId: Integer,
      executionStatus: String,
      executionTime: Double,
      executionName: String,
      startTime: Long,
      endTime: Long,
      access: Boolean
  )

  def mapToName(code: Short): String = {
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

  def mapToStatus(status: String): Int = {
    status match {
      case "READY"     => 0
      case "RUNNING"   => 1
      case "PAUSED"    => 2
      case "COMPLETED" => 3
      case "FAILED"    => 4
      case "KILLED"    => 5
      case _           => -1 // or throw an exception, depends on your needs
    }
  }

  val sortFieldMapping = Map(
    "workflow_name" -> WORKFLOW.NAME,
    "execution_name" -> WORKFLOW_EXECUTIONS.NAME,
    "initiator" -> USER.NAME,
    "end_time" -> WORKFLOW_EXECUTIONS.LAST_UPDATE_TIME
  )

}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/admin/execution")
@RolesAllowed(Array("ADMIN"))
class AdminExecutionResource {

  @GET
  @Path("/totalWorkflow")
  @Produces()
  def getTotalWorkflows: Int = {
    context
      .select(
        DSL.countDistinct(WORKFLOW.WID)
      )
      .from(WORKFLOW_EXECUTIONS)
      .join(WORKFLOW_VERSION)
      .on(WORKFLOW_EXECUTIONS.VID.eq(WORKFLOW_VERSION.VID))
      .join(USER)
      .on(WORKFLOW_EXECUTIONS.UID.eq(USER.UID))
      .join(WORKFLOW)
      .on(WORKFLOW.WID.eq(WORKFLOW_VERSION.WID))
      .fetchOne(0, classOf[Int])
  }

  /**
    * This method retrieves latest execution of each workflow for specified page.
    * The returned executions are sorted and filtered according to the parameters.
    */
  @GET
  @Path("/executionList/{pageSize}/{pageIndex}/{sortField}/{sortDirection}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def listWorkflows(
      @Auth current_user: SessionUser,
      @PathParam("pageSize") page_size: Int = 20,
      @PathParam("pageIndex") page_index: Int = 0,
      @PathParam("sortField") sortField: String = "end_time",
      @PathParam("sortDirection") sortDirection: String = "desc",
      @QueryParam("filter") filter: java.util.List[String]
  ): List[dashboardExecution] = {
    val filter_status = filter.asScala.map(mapToStatus).toSeq.filter(_ != -1).asJava

    // Base query that retrieves latest execution info for each workflow without sorting and filtering.
    // Only retrieving executions in current page according to pageSize and pageIndex parameters.
    val executions_base_query = context
      .select(
        WORKFLOW_EXECUTIONS.UID,
        USER.NAME,
        WORKFLOW_VERSION.WID,
        WORKFLOW.NAME,
        WORKFLOW_EXECUTIONS.EID,
        WORKFLOW_EXECUTIONS.STARTING_TIME,
        WORKFLOW_EXECUTIONS.LAST_UPDATE_TIME,
        WORKFLOW_EXECUTIONS.STATUS,
        WORKFLOW_EXECUTIONS.NAME
      )
      .from(WORKFLOW_EXECUTIONS)
      .join(WORKFLOW_VERSION)
      .on(WORKFLOW_EXECUTIONS.VID.eq(WORKFLOW_VERSION.VID))
      .join(USER)
      .on(WORKFLOW_EXECUTIONS.UID.eq(USER.UID))
      .join(WORKFLOW)
      .on(WORKFLOW.WID.eq(WORKFLOW_VERSION.WID))
      .naturalJoin(
        context
          .select(
            DSL.max(WORKFLOW_EXECUTIONS.EID).as("eid")
          )
          .from(WORKFLOW_EXECUTIONS)
          .join(WORKFLOW_VERSION)
          .on(WORKFLOW_VERSION.VID.eq(WORKFLOW_EXECUTIONS.VID))
          .groupBy(WORKFLOW_VERSION.WID)
      )

    // Apply filter if the status are not empty.
    val executions_apply_filter = if (!filter_status.isEmpty) {
      executions_base_query.where(WORKFLOW_EXECUTIONS.STATUS.in(filter_status))
    } else {
      executions_base_query
    }

    // Apply sorting if user specified.
    var executions_apply_order =
      executions_apply_filter.limit(page_size).offset(page_index * page_size)
    if (sortField != "NO_SORTING") {
      executions_apply_order = executions_apply_filter
        .orderBy(
          if (sortDirection == "desc") sortFieldMapping.getOrElse(sortField, WORKFLOW.NAME).desc()
          else sortFieldMapping.getOrElse(sortField, WORKFLOW.NAME).asc()
        )
        .limit(page_size)
        .offset(page_index * page_size)
    }

    val executions = executions_apply_order.fetch()

    // Retrieve the id of each workflow that the user has access to.
    val availableWorkflowIds = context
      .select(WORKFLOW_USER_ACCESS.WID)
      .from(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.UID.eq(current_user.getUid))
      .fetchInto(classOf[Integer])

    // Calculate the statistics needed for each execution.
    executions
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
