package edu.uci.ics.texera.web.resource.dashboard.user.workflow

import edu.uci.ics.amber.core.storage.result.ExecutionResourcesMapping
import edu.uci.ics.amber.core.storage.{DocumentFactory, VFSURIFactory}
import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.core.virtualidentity.{
  ChannelMarkerIdentity,
  ExecutionIdentity,
  OperatorIdentity,
  WorkflowIdentity
}
import edu.uci.ics.amber.core.workflow.PortIdentity
import edu.uci.ics.amber.engine.architecture.logreplay.{ReplayDestination, ReplayLogRecord}
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.storage.SequentialRecordStorage
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.jooq.generated.Tables._
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.WorkflowExecutionsDao
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.WorkflowExecutions
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowExecutionsResource._
import edu.uci.ics.texera.web.service.ExecutionsMetadataPersistService
import io.dropwizard.auth.Auth

import java.net.URI
import java.sql.Timestamp
import java.util.concurrent.TimeUnit
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

object WorkflowExecutionsResource {
  final private lazy val context = SqlServer
    .getInstance()
    .createDSLContext()
  final private lazy val executionsDao = new WorkflowExecutionsDao(context.configuration)

  def getExecutionById(eId: Integer): WorkflowExecutions = {
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
    * @return Integer
    */
  def getLatestExecutionID(wid: Integer): Option[Integer] = {
    val executions = context
      .select(WORKFLOW_EXECUTIONS.EID)
      .from(WORKFLOW_EXECUTIONS)
      .join(WORKFLOW_VERSION)
      .on(WORKFLOW_EXECUTIONS.VID.eq(WORKFLOW_VERSION.VID))
      .where(WORKFLOW_VERSION.WID.eq(wid))
      .fetchInto(classOf[Integer])
      .asScala
      .toList
    if (executions.isEmpty) {
      None
    } else {
      Some(executions.max)
    }
  }

  def insertOperatorPortResultUri(
      eid: ExecutionIdentity,
      opId: OperatorIdentity,
      portId: PortIdentity,
      uri: URI
  ): Unit = {
    if (AmberConfig.isUserSystemEnabled) {
      context
        .insertInto(OPERATOR_PORT_EXECUTIONS)
        .values(eid.id, opId.id, portId.id, uri.toString)
        .execute()
    } else {
      ExecutionResourcesMapping.addResourceUri(eid, uri)
    }
  }

  def insertOperatorExecutions(
      eid: Long,
      opId: String,
      uri: URI
  ): Unit = {
    context
      .insertInto(OPERATOR_EXECUTIONS)
      .values(eid, opId, uri.toString)
      .execute()
  }

  def updateRuntimeStatsUri(wid: Long, eid: Long, uri: URI): Unit = {
    context
      .update(WORKFLOW_EXECUTIONS)
      .set(WORKFLOW_EXECUTIONS.RUNTIME_STATS_URI, uri.toString)
      .where(
        WORKFLOW_EXECUTIONS.EID
          .eq(eid.toInt)
          .and(
            WORKFLOW_EXECUTIONS.VID.in(
              context
                .select(WORKFLOW_VERSION.VID)
                .from(WORKFLOW_VERSION)
                .where(WORKFLOW_VERSION.WID.eq(wid.toInt))
            )
          )
      )
      .execute()
  }

  def getResultUrisByExecutionId(eid: ExecutionIdentity): List[URI] = {
    if (AmberConfig.isUserSystemEnabled) {
      context
        .select(OPERATOR_PORT_EXECUTIONS.RESULT_URI)
        .from(OPERATOR_PORT_EXECUTIONS)
        .where(OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(eid.id.toInt))
        .fetchInto(classOf[String])
        .asScala
        .toList
        .map(URI.create)
    } else {
      ExecutionResourcesMapping.getResourceURIs(eid)
    }
  }

  def clearUris(eid: ExecutionIdentity): Unit = {
    if (AmberConfig.isUserSystemEnabled) {
      context
        .delete(OPERATOR_PORT_EXECUTIONS)
        .where(OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(eid.id.toInt))
        .execute()
    } else {
      ExecutionResourcesMapping.removeExecutionResources(eid)
    }
  }

  def getResultUriByExecutionAndPort(
      wid: WorkflowIdentity,
      eid: ExecutionIdentity,
      opId: OperatorIdentity,
      portId: PortIdentity
  ): Option[URI] = {
    if (AmberConfig.isUserSystemEnabled) {
      Option(
        context
          .select(OPERATOR_PORT_EXECUTIONS.RESULT_URI)
          .from(OPERATOR_PORT_EXECUTIONS)
          .where(
            OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID
              .eq(eid.id.toInt)
              .and(OPERATOR_PORT_EXECUTIONS.OPERATOR_ID.eq(opId.id))
              .and(OPERATOR_PORT_EXECUTIONS.PORT_ID.eq(portId.id))
          )
          .fetchOneInto(classOf[String])
      ).map(URI.create)
    } else {
      Option(
        VFSURIFactory.createResultURI(
          wid,
          eid,
          opId,
          portId
        )
      )
    }
  }

  case class WorkflowExecutionEntry(
      eId: Integer,
      vId: Integer,
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

  case class WorkflowRuntimeStatistics(
      operatorId: String,
      timestamp: Timestamp,
      inputTupleCount: Long,
      inputTupleSize: Long,
      outputTupleCount: Long,
      outputTupleSize: Long,
      dataProcessingTime: Long,
      controlProcessingTime: Long,
      idleTime: Long,
      numWorkers: Int,
      status: Int
  )
}

case class ExecutionGroupBookmarkRequest(
    wid: Integer,
    eIds: Array[Integer],
    isBookmarked: Boolean
)

case class ExecutionGroupDeleteRequest(wid: Integer, eIds: Array[Integer])

case class ExecutionRenameRequest(wid: Integer, eId: Integer, executionName: String)

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/executions")
class WorkflowExecutionsResource {

  @GET
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/{wid}/interactions/{eid}")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def retrieveInteractionHistory(
      @PathParam("wid") wid: Integer,
      @PathParam("eid") eid: Integer,
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
      @PathParam("wid") wid: Integer,
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
      @PathParam("wid") wid: Integer,
      @PathParam("eid") eid: Integer
  ): List[WorkflowRuntimeStatistics] = {
    // Create URI for runtime statistics
    val uriString: String = context
      .select(WORKFLOW_EXECUTIONS.RUNTIME_STATS_URI)
      .from(WORKFLOW_EXECUTIONS)
      .where(
        WORKFLOW_EXECUTIONS.EID
          .eq(eid)
          .and(
            WORKFLOW_EXECUTIONS.VID.in(
              context
                .select(WORKFLOW_VERSION.VID)
                .from(WORKFLOW_VERSION)
                .where(WORKFLOW_VERSION.WID.eq(wid))
            )
          )
      )
      .fetchOneInto(classOf[String])

    if (uriString == null || uriString.isEmpty) {
      throw new NoSuchElementException(
        "No runtime statistics URI found for the given execution ID."
      )
    }

    val uri: URI = new URI(uriString)
    val document = DocumentFactory.openDocument(uri)._1

    // Read all records from Iceberg and convert to WorkflowRuntimeStatistics
    document
      .get()
      .map(tuple => {
        val record = tuple.asInstanceOf[Tuple]
        WorkflowRuntimeStatistics(
          operatorId = record.getField(0).asInstanceOf[String],
          timestamp = record.getField(1).asInstanceOf[Timestamp],
          inputTupleCount = record.getField(2).asInstanceOf[Long],
          inputTupleSize = record.getField(3).asInstanceOf[Long],
          outputTupleCount = record.getField(4).asInstanceOf[Long],
          outputTupleSize = record.getField(5).asInstanceOf[Long],
          dataProcessingTime = record.getField(6).asInstanceOf[Long],
          controlProcessingTime = record.getField(7).asInstanceOf[Long],
          idleTime = record.getField(8).asInstanceOf[Long],
          numWorkers = record.getField(9).asInstanceOf[Int],
          status = record.getField(10).asInstanceOf[Int]
        )
      })
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
    val eIdsList = request.eIds.toSeq.asJava
    if (request.isBookmarked) {
      // If currently bookmarked, un-bookmark (set bookmarked = false)
      context
        .update(WORKFLOW_EXECUTIONS)
        .set(WORKFLOW_EXECUTIONS.BOOKMARKED, java.lang.Boolean.valueOf(false))
        .where(WORKFLOW_EXECUTIONS.EID.in(eIdsList))
        .execute()
    } else {
      // If currently not bookmarked, bookmark (set bookmarked = true)
      context
        .update(WORKFLOW_EXECUTIONS)
        .set(WORKFLOW_EXECUTIONS.BOOKMARKED, java.lang.Boolean.valueOf(true))
        .where(WORKFLOW_EXECUTIONS.EID.in(eIdsList))
        .execute()
    }

  }

  /** Determine if user is authorized to access the workflow, if not raise 401 */
  def validateUserCanAccessWorkflow(uid: Integer, wid: Integer): Unit = {
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
    val eIdsList = request.eIds.toSeq.asJava

    context
      .deleteFrom(WORKFLOW_EXECUTIONS)
      .where(WORKFLOW_EXECUTIONS.EID.in(eIdsList))
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
