package edu.uci.ics.texera.web.resource.dashboard.user.workflow

import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.amber.engine.architecture.logreplay.{ReplayDestination, ReplayLogRecord}
import edu.uci.ics.amber.engine.common.storage.SequentialRecordStorage
import edu.uci.ics.amber.core.virtualidentity.{ChannelMarkerIdentity, ExecutionIdentity}
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.dao.jooq.generated.Tables.{
  USER,
  WORKFLOW_EXECUTIONS,
  WORKFLOW_RUNTIME_STATISTICS,
  WORKFLOW_VERSION
}
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.{
  WorkflowExecutionsDao,
  WorkflowRuntimeStatisticsDao
}
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.{
  WorkflowExecutions,
  WorkflowRuntimeStatistics
}
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowExecutionsResource._
import edu.uci.ics.texera.web.service.ExecutionsMetadataPersistService
import io.dropwizard.auth.Auth
import org.jooq.impl.DSL._
import org.jooq.types.{UInteger, ULong}

import java.net.URI
import java.sql.Timestamp
import java.util
import java.util.concurrent.TimeUnit
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import scala.collection.mutable
import scala.jdk.CollectionConverters.ListHasAsScala

object WorkflowExecutionsResource {
  final private lazy val context = SqlServer
    .getInstance(StorageConfig.jdbcUrl, StorageConfig.jdbcUsername, StorageConfig.jdbcPassword)
    .createDSLContext()
  final private lazy val executionsDao = new WorkflowExecutionsDao(context.configuration)
  final private lazy val workflowRuntimeStatisticsDao = new WorkflowRuntimeStatisticsDao(
    context.configuration
  )

  def getExecutionById(eId: UInteger): WorkflowExecutions = {
    executionsDao.fetchOneByEid(eId)
  }

  def getExpiredExecutionsWithResultOrLog(timeToLive: Int): List[WorkflowExecutions] = {
    val deadline = new Timestamp(
      System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(timeToLive)
    )
    context
      .selectFrom(WORKFLOW_EXECUTIONS)
      .where(
        WORKFLOW_EXECUTIONS.LAST_UPDATE_TIME.isNull
          .and(WORKFLOW_EXECUTIONS.STARTING_TIME.lt(deadline))
          .or(WORKFLOW_EXECUTIONS.LAST_UPDATE_TIME.lt(deadline))
      )
      .and(
        WORKFLOW_EXECUTIONS.RESULT.ne("").or(WORKFLOW_EXECUTIONS.LOG_LOCATION.ne(""))
      )
      .fetchInto(classOf[WorkflowExecutions])
      .asScala
      .toList
  }

  /**
    * This function retrieves the latest execution id of a workflow
    *
    * @param wid workflow id
    * @return UInteger
    */
  def getLatestExecutionID(wid: UInteger): Option[UInteger] = {
    val executions = context
      .select(WORKFLOW_EXECUTIONS.EID)
      .from(WORKFLOW_EXECUTIONS)
      .fetchInto(classOf[UInteger])
      .asScala
      .toList
    if (executions.isEmpty) {
      None
    } else {
      Some(executions.max)
    }
  }

  def insertWorkflowRuntimeStatistics(list: util.ArrayList[WorkflowRuntimeStatistics]): Unit = {
    workflowRuntimeStatisticsDao.insert(list);
  }

  case class WorkflowExecutionEntry(
      eId: UInteger,
      vId: UInteger,
      userName: String,
      googleAvatar: String,
      status: Byte,
      result: String,
      startingTime: Timestamp,
      completionTime: Timestamp,
      bookmarked: Boolean,
      name: String,
      logLocation: String
  )

  case class ExecutionResultEntry(
      eId: UInteger,
      result: String
  )

  case class OperatorRuntimeStatistics(
      operatorId: String,
      inputTupleCount: UInteger,
      outputTupleCount: UInteger,
      timestamp: Timestamp,
      dataProcessingTime: ULong,
      controlProcessingTime: ULong,
      idleTime: ULong,
      numWorkers: UInteger
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

  @GET
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/{wid}/interactions/{eid}")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def retrieveInteractionHistory(
      @PathParam("wid") wid: UInteger,
      @PathParam("eid") eid: UInteger,
      @Auth sessionUser: SessionUser
  ): List[String] = {
    val user = sessionUser.getUser
    if (!WorkflowAccessResource.hasReadAccess(wid, user.getUid)) {
      List()
    } else {
      ExecutionsMetadataPersistService.tryGetExistingExecution(
        ExecutionIdentity(eid.longValue())
      ) match {
        case Some(value) =>
          val logLocation = value.getLogLocation
          if (logLocation != null && logLocation.nonEmpty) {
            val storage =
              SequentialRecordStorage.getStorage[ReplayLogRecord](Some(new URI(logLocation)))
            val result = new mutable.ArrayBuffer[ChannelMarkerIdentity]()
            storage.getReader("CONTROLLER").mkRecordIterator().foreach {
              case destination: ReplayDestination =>
                result.append(destination.id)
              case _ =>
            }
            result.map(_.id).toList
          } else {
            List()
          }
        case None => List()
      }
    }
  }

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
          USER.NAME,
          USER.GOOGLE_AVATAR,
          WORKFLOW_EXECUTIONS.STATUS,
          WORKFLOW_EXECUTIONS.RESULT,
          WORKFLOW_EXECUTIONS.STARTING_TIME,
          WORKFLOW_EXECUTIONS.LAST_UPDATE_TIME,
          WORKFLOW_EXECUTIONS.BOOKMARKED,
          WORKFLOW_EXECUTIONS.NAME,
          WORKFLOW_EXECUTIONS.LOG_LOCATION
        )
        .from(WORKFLOW_EXECUTIONS)
        .join(WORKFLOW_VERSION)
        .on(WORKFLOW_VERSION.VID.eq(WORKFLOW_EXECUTIONS.VID))
        .join(USER)
        .on(WORKFLOW_EXECUTIONS.UID.eq(USER.UID))
        .where(WORKFLOW_VERSION.WID.eq(wid))
        .fetchInto(classOf[WorkflowExecutionEntry])
        .asScala
        .toList
        .reverse
    }
  }

  @GET
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/{wid}/{eid}")
  def retrieveWorkflowRuntimeStatistics(
      @PathParam("wid") wid: UInteger,
      @PathParam("eid") eid: UInteger
  ): List[OperatorRuntimeStatistics] = {
    context
      .select(
        WORKFLOW_RUNTIME_STATISTICS.OPERATOR_ID,
        WORKFLOW_RUNTIME_STATISTICS.INPUT_TUPLE_CNT,
        WORKFLOW_RUNTIME_STATISTICS.OUTPUT_TUPLE_CNT,
        WORKFLOW_RUNTIME_STATISTICS.TIME,
        WORKFLOW_RUNTIME_STATISTICS.DATA_PROCESSING_TIME,
        WORKFLOW_RUNTIME_STATISTICS.CONTROL_PROCESSING_TIME,
        WORKFLOW_RUNTIME_STATISTICS.IDLE_TIME,
        WORKFLOW_RUNTIME_STATISTICS.NUM_WORKERS
      )
      .from(WORKFLOW_RUNTIME_STATISTICS)
      .where(
        WORKFLOW_RUNTIME_STATISTICS.WORKFLOW_ID
          .eq(wid)
          .and(WORKFLOW_RUNTIME_STATISTICS.EXECUTION_ID.eq(eid))
      )
      .orderBy(WORKFLOW_RUNTIME_STATISTICS.TIME, WORKFLOW_RUNTIME_STATISTICS.OPERATOR_ID)
      .fetchInto(classOf[OperatorRuntimeStatistics])
      .asScala
      .toList
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
