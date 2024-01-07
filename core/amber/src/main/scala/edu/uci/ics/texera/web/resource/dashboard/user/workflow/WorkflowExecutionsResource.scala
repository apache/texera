package edu.uci.ics.texera.web.resource.dashboard.user.workflow

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{
  USER,
  WORKFLOW_EXECUTIONS,
  WORKFLOW_VERSION
}
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.WorkflowExecutionsDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.WorkflowExecutions
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowExecutionsResource._
import io.dropwizard.auth.Auth
import org.jooq.impl.DSL._
import org.jooq.types.UInteger

import java.sql.Timestamp
import java.util.concurrent.TimeUnit
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object WorkflowExecutionsResource {
  final private lazy val context = SqlServer.createDSLContext()
  final private lazy val executionsDao = new WorkflowExecutionsDao(context.configuration)

  def getExecutionById(eId: UInteger): WorkflowExecutions = {
    executionsDao.fetchOneByEid(eId)
  }

  def getExpiredExecutionsWithResultOrLog(timeToLive: Int): List[WorkflowExecutions] = {
    context
      .selectFrom(WORKFLOW_EXECUTIONS)
      .where(
        WORKFLOW_EXECUTIONS.LAST_UPDATE_TIME
          .lt(new Timestamp(System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(timeToLive)))
          .and(WORKFLOW_EXECUTIONS.RESULT.ne("").or(WORKFLOW_EXECUTIONS.LOG_LOCATION.ne("")))
      )
      .fetchInto(classOf[WorkflowExecutions])
      .toList
  }

  /**
    * This function retrieves the latest execution id of a workflow
    * @param wid workflow id
    * @return UInteger
    */
  def getLatestExecutionID(wid: UInteger): Option[UInteger] = {
    val executions = context
      .select(WORKFLOW_EXECUTIONS.EID)
      .from(WORKFLOW_EXECUTIONS)
      .fetchInto(classOf[UInteger])
      .toList
    if (executions.isEmpty) {
      None
    } else {
      Some(executions.max)
    }
  }

  case class WorkflowExecutionEntry(
      eId: UInteger,
      vId: UInteger,
      userName: String,
      status: Byte,
      result: String,
      startingTime: Timestamp,
      completionTime: Timestamp,
      bookmarked: Boolean,
      name: String
  )
}

case class ExecutionGroupBookmarkRequest(
    wid: UInteger,
    eIds: Array[UInteger],
    isBookmarked: Boolean
)
case class ExecutionGroupDeleteRequest(wid: UInteger, eIds: Array[UInteger])
case class ExecutionRenameRequest(wid: UInteger, eId: UInteger, executionName: String)

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/executions")
class WorkflowExecutionsResource {

  /**
    * This method returns the executions of a workflow given by its ID
    *
    * @return executions[]
    */
  @GET
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/{wid}")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def retrieveExecutionsOfWorkflow(
      @PathParam("wid") wid: UInteger,
      @Auth sessionUser: SessionUser
  ): List[WorkflowExecutionEntry] = {
    val user = sessionUser.getUser
    if (!WorkflowAccessResource.hasReadAccess(wid, user.getUid)) {
      List()
    } else {
      context
        .select(
          WORKFLOW_EXECUTIONS.EID,
          WORKFLOW_EXECUTIONS.VID,
          field(
            context
              .select(USER.NAME)
              .from(USER)
              .where(WORKFLOW_EXECUTIONS.UID.eq(USER.UID))
          ),
          WORKFLOW_EXECUTIONS.STATUS,
          WORKFLOW_EXECUTIONS.RESULT,
          WORKFLOW_EXECUTIONS.STARTING_TIME,
          WORKFLOW_EXECUTIONS.LAST_UPDATE_TIME,
          WORKFLOW_EXECUTIONS.BOOKMARKED,
          WORKFLOW_EXECUTIONS.NAME
        )
        .from(WORKFLOW_EXECUTIONS)
        .join(WORKFLOW_VERSION)
        .on(WORKFLOW_VERSION.VID.eq(WORKFLOW_EXECUTIONS.VID))
        .where(WORKFLOW_VERSION.WID.eq(wid))
        .fetchInto(classOf[WorkflowExecutionEntry])
        .toList
        .reverse
    }
  }

  /** Sets a group of executions' bookmarks to the payload passed in the body. */
  @PUT
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Path("/set_execution_bookmarks")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def setExecutionAreBookmarked(
      request: ExecutionGroupBookmarkRequest,
      @Auth sessionUser: SessionUser
  ): Unit = {
    validateUserCanAccessWorkflow(sessionUser.getUser.getUid, request.wid)
    if (request.isBookmarked) {
      val eIdArray = request.eIds.mkString("(", ",", ")")
      val sqlString = "update texera_db.workflow_executions " +
        "set texera_db.workflow_executions.bookmarked = 0 " +
        s"where texera_db.workflow_executions.eid in $eIdArray"
      context
        .query(sqlString)
        .execute()
    } else {
      val eIdArray = request.eIds.mkString("(", ",", ")")
      val sqlString = "UPDATE texera_db.workflow_executions " +
        "SET texera_db.workflow_executions.bookmarked = 1 " +
        s"WHERE texera_db.workflow_executions.eid IN $eIdArray"
      context
        .query(sqlString)
        .execute()
    }
  }

  /** Determine if user is authorized to access the workflow, if not raise 401 */
  def validateUserCanAccessWorkflow(uid: UInteger, wid: UInteger): Unit = {
    if (!WorkflowAccessResource.hasReadAccess(wid, uid))
      throw new WebApplicationException(Response.Status.UNAUTHORIZED)
  }

  /** Delete a group of executions */
  @PUT
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Path("/delete_executions")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def groupDeleteExecutionsOfWorkflow(
      request: ExecutionGroupDeleteRequest,
      @Auth sessionUser: SessionUser
  ): Unit = {
    validateUserCanAccessWorkflow(sessionUser.getUser.getUid, request.wid)
    /* delete the execution in sql */
    val eIdArray = request.eIds.mkString("(", ",", ")")
    val sqlString: String = "DELETE FROM texera_db.workflow_executions " +
      s"WHERE texera_db.workflow_executions.eid IN $eIdArray"
    context
      .query(sqlString)
      .execute()
  }

  /** Name a single execution * */
  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Path("/update_execution_name")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def updateWorkflowExecutionsName(
      request: ExecutionRenameRequest,
      @Auth sessionUser: SessionUser
  ): Unit = {
    validateUserCanAccessWorkflow(sessionUser.getUser.getUid, request.wid)
    val execution = getExecutionById(request.eId)
    execution.setName(request.executionName)
    executionsDao.update(execution)
  }

}
