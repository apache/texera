package edu.uci.ics.texera.web.resource.dashboard.hub.workflow

import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.jooq.generated.Tables._
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.WorkflowDao
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.{User, Workflow}
import edu.uci.ics.texera.web.resource.dashboard.hub.workflow.HubWorkflowResource.{
  fetchDashboardWorkflowsByWids,
  recordUserActivity
}
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource.{
  DashboardWorkflow,
  WorkflowWithPrivilege
}
import org.jooq.impl.DSL
import org.jooq.types.UInteger

import java.util
import java.util.Collections
import java.util.regex.Pattern
import javax.servlet.http.HttpServletRequest
import javax.ws.rs._
import javax.ws.rs.core.{Context, MediaType}
import scala.jdk.CollectionConverters._

object HubWorkflowResource {
  final private lazy val context = SqlServer
    .getInstance(StorageConfig.jdbcUrl, StorageConfig.jdbcUsername, StorageConfig.jdbcPassword)
    .createDSLContext()

  final private val ipv4Pattern: Pattern = Pattern.compile(
    "^([0-9]{1,3}\\.){3}[0-9]{1,3}$"
  )

  def fetchDashboardWorkflowsByWids(wids: Seq[UInteger]): util.List[DashboardWorkflow] = {
    if (wids.nonEmpty) {
      context
        .select(
          WORKFLOW.NAME,
          WORKFLOW.DESCRIPTION,
          WORKFLOW.WID,
          WORKFLOW.CREATION_TIME,
          WORKFLOW.LAST_MODIFIED_TIME,
          USER.NAME.as("ownerName"),
          WORKFLOW_OF_USER.UID.as("ownerId")
        )
        .from(WORKFLOW)
        .join(WORKFLOW_OF_USER)
        .on(WORKFLOW.WID.eq(WORKFLOW_OF_USER.WID))
        .join(USER)
        .on(WORKFLOW_OF_USER.UID.eq(USER.UID))
        .where(WORKFLOW.WID.in(wids: _*))
        .fetch()
        .asScala
        .map(record => {
          val workflow = new Workflow(
            record.get(WORKFLOW.NAME),
            record.get(WORKFLOW.DESCRIPTION),
            record.get(WORKFLOW.WID),
            null,
            record.get(WORKFLOW.CREATION_TIME),
            record.get(WORKFLOW.LAST_MODIFIED_TIME),
            null
          )

          DashboardWorkflow(
            isOwner = false,
            accessLevel = "",
            ownerName = record.get("ownerName", classOf[String]),
            workflow = workflow,
            projectIDs = List(),
            ownerId = record.get("ownerId", classOf[UInteger])
          )
        })
        .toList
        .asJava
    } else {
      Collections.emptyList[DashboardWorkflow]()
    }
  }

  def recordUserActivity(
      request: HttpServletRequest,
      userId: UInteger = UInteger.valueOf(0),
      workflowId: UInteger,
      action: String
  ): Unit = {
    val userIp = request.getRemoteAddr()
    //    println(s"User IP from getRemoteAddr: $userIp")

    if (ipv4Pattern.matcher(userIp).matches()) {
      context
        .insertInto(WORKFLOW_USER_ACTIVITY)
        .set(WORKFLOW_USER_ACTIVITY.UID, userId)
        .set(WORKFLOW_USER_ACTIVITY.WID, workflowId)
        .set(WORKFLOW_USER_ACTIVITY.IP, userIp)
        .set(WORKFLOW_USER_ACTIVITY.ACTIVATE, action)
        .execute()
    } else {
      context
        .insertInto(WORKFLOW_USER_ACTIVITY)
        .set(WORKFLOW_USER_ACTIVITY.UID, userId)
        .set(WORKFLOW_USER_ACTIVITY.WID, workflowId)
        .set(WORKFLOW_USER_ACTIVITY.ACTIVATE, action)
        .execute()
    }
  }
}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/hub/workflow")
class HubWorkflowResource {
  final private lazy val context = SqlServer
    .getInstance(StorageConfig.jdbcUrl, StorageConfig.jdbcUsername, StorageConfig.jdbcPassword)
    .createDSLContext()
  final private lazy val workflowDao = new WorkflowDao(context.configuration)

  @GET
  @Path("/list")
  def getWorkflowList: util.List[Workflow] = {
    context
      .select()
      .from(WORKFLOW)
      .fetchInto(classOf[Workflow])
  }

  @GET
  @Path("/count")
  def getPublishedWorkflowCount: Integer = {
    context.selectCount
      .from(WORKFLOW)
      .where(WORKFLOW.IS_PUBLIC.eq(1.toByte))
      .fetchOne(0, classOf[Integer])
  }

  @GET
  @Path("/owner_user")
  def getOwnerUser(@QueryParam("wid") wid: UInteger): User = {
    context
      .select(
        USER.UID,
        USER.NAME,
        USER.EMAIL,
        USER.PASSWORD,
        USER.GOOGLE_ID,
        USER.ROLE,
        USER.GOOGLE_AVATAR
      )
      .from(WORKFLOW_OF_USER)
      .join(USER)
      .on(WORKFLOW_OF_USER.UID.eq(USER.UID))
      .where(WORKFLOW_OF_USER.WID.eq(wid))
      .fetchOneInto(classOf[User])
  }

  @GET
  @Path("/workflow_name")
  def getWorkflowName(@QueryParam("wid") wid: UInteger): String = {
    context
      .select(
        WORKFLOW.NAME
      )
      .from(WORKFLOW)
      .where(WORKFLOW.WID.eq(wid))
      .fetchOneInto(classOf[String])
  }

  @GET
  @Path("/public/{wid}")
  def retrievePublicWorkflow(
      @PathParam("wid") wid: UInteger
  ): WorkflowWithPrivilege = {
    val workflow = workflowDao.ctx
      .selectFrom(WORKFLOW)
      .where(WORKFLOW.WID.eq(wid))
      .and(WORKFLOW.IS_PUBLIC.isTrue)
      .fetchOne()
    WorkflowWithPrivilege(
      workflow.getName,
      workflow.getDescription,
      workflow.getWid,
      workflow.getContent,
      workflow.getCreationTime,
      workflow.getLastModifiedTime,
      workflow.getIsPublic,
      readonly = true
    )
  }

  @GET
  @Path("/workflow_description")
  def getWorkflowDescription(@QueryParam("wid") wid: UInteger): String = {
    context
      .select(
        WORKFLOW.DESCRIPTION
      )
      .from(WORKFLOW)
      .where(WORKFLOW.WID.eq(wid))
      .fetchOneInto(classOf[String])
  }

  @GET
  @Path("/isLiked")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def isLiked(
      @QueryParam("workflowId") workflowId: UInteger,
      @QueryParam("userId") userId: UInteger
  ): Boolean = {
    val existingLike = context
      .selectFrom(WORKFLOW_USER_LIKES)
      .where(
        WORKFLOW_USER_LIKES.UID
          .eq(userId)
          .and(WORKFLOW_USER_LIKES.WID.eq(workflowId))
      )
      .fetchOne()

    existingLike != null
  }

  @POST
  @Path("/like")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def likeWorkflow(@Context request: HttpServletRequest, likeRequest: Array[UInteger]): Boolean = {
    if (likeRequest.length != 2) {
      return false
    }

    val workflowId = likeRequest(0)
    val userId = likeRequest(1)

    val existingLike = context
      .selectFrom(WORKFLOW_USER_LIKES)
      .where(
        WORKFLOW_USER_LIKES.UID
          .eq(userId)
          .and(WORKFLOW_USER_LIKES.WID.eq(workflowId))
      )
      .fetchOne()

    if (existingLike == null) {
      context
        .insertInto(WORKFLOW_USER_LIKES)
        .set(WORKFLOW_USER_LIKES.UID, userId)
        .set(WORKFLOW_USER_LIKES.WID, workflowId)
        .execute()

      recordUserActivity(request, userId, workflowId, "like")
      true
    } else {
      false
    }
  }

  @POST
  @Path("/unlike")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def unlikeWorkflow(
      @Context request: HttpServletRequest,
      likeRequest: Array[UInteger]
  ): Boolean = {
    if (likeRequest.length != 2) {
      return false
    }

    val workflowId = likeRequest(0)
    val userId = likeRequest(1)

    val existingLike = context
      .selectFrom(WORKFLOW_USER_LIKES)
      .where(
        WORKFLOW_USER_LIKES.UID
          .eq(userId)
          .and(WORKFLOW_USER_LIKES.WID.eq(workflowId))
      )
      .fetchOne()

    if (existingLike != null) {
      context
        .deleteFrom(WORKFLOW_USER_LIKES)
        .where(
          WORKFLOW_USER_LIKES.UID
            .eq(userId)
            .and(WORKFLOW_USER_LIKES.WID.eq(workflowId))
        )
        .execute()

      recordUserActivity(request, userId, workflowId, "unlike")
      true
    } else {
      false
    }
  }

  @GET
  @Path("/likeCount/{wid}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getLikeCount(@PathParam("wid") wid: UInteger): Int = {
    val likeCount = context
      .selectCount()
      .from(WORKFLOW_USER_LIKES)
      .where(WORKFLOW_USER_LIKES.WID.eq(wid))
      .fetchOne(0, classOf[Int])

    likeCount
  }

  @GET
  @Path("/cloneCount/{wid}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getCloneCount(@PathParam("wid") wid: UInteger): Int = {
    val cloneCount = context
      .selectCount()
      .from(WORKFLOW_USER_CLONES)
      .where(WORKFLOW_USER_CLONES.WID.eq(wid))
      .fetchOne(0, classOf[Int])

    cloneCount
  }

  @GET
  @Path("/topLovedWorkflows")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getTopLovedWorkflows: util.List[DashboardWorkflow] = {
    val topLovedWorkflowsWids = context
      .select(WORKFLOW_USER_LIKES.WID)
      .from(WORKFLOW_USER_LIKES)
      .join(WORKFLOW)
      .on(WORKFLOW_USER_LIKES.WID.eq(WORKFLOW.WID))
      .where(WORKFLOW.IS_PUBLIC.eq(1.toByte))
      .groupBy(WORKFLOW_USER_LIKES.WID)
      .orderBy(DSL.count(WORKFLOW_USER_LIKES.WID).desc())
      .limit(8)
      .fetchInto(classOf[UInteger])
      .asScala
      .toSeq

    println(fetchDashboardWorkflowsByWids(topLovedWorkflowsWids))

    fetchDashboardWorkflowsByWids(topLovedWorkflowsWids)
  }

  @GET
  @Path("/topClonedWorkflows")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getTopClonedWorkflows: util.List[DashboardWorkflow] = {
    val topClonedWorkflowsWids = context
      .select(WORKFLOW_USER_CLONES.WID)
      .from(WORKFLOW_USER_CLONES)
      .join(WORKFLOW)
      .on(WORKFLOW_USER_CLONES.WID.eq(WORKFLOW.WID))
      .where(WORKFLOW.IS_PUBLIC.eq(1.toByte))
      .groupBy(WORKFLOW_USER_CLONES.WID)
      .orderBy(DSL.count(WORKFLOW_USER_CLONES.WID).desc())
      .limit(8)
      .fetchInto(classOf[UInteger])
      .asScala
      .toSeq

    println(fetchDashboardWorkflowsByWids(topClonedWorkflowsWids))

    fetchDashboardWorkflowsByWids(topClonedWorkflowsWids)
  }

  @POST
  @Path("/view")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def viewWorkflow(@Context request: HttpServletRequest, viewRequest: Array[UInteger]): Int = {

    val workflowId = viewRequest(0)
    val userId = viewRequest(1)

    context
      .insertInto(WORKFLOW_VIEW_COUNT)
      .set(WORKFLOW_VIEW_COUNT.WID, workflowId)
      .set(WORKFLOW_VIEW_COUNT.VIEW_COUNT, UInteger.valueOf(1))
      .onDuplicateKeyUpdate()
      .set(WORKFLOW_VIEW_COUNT.VIEW_COUNT, WORKFLOW_VIEW_COUNT.VIEW_COUNT.add(1))
      .execute()
    recordUserActivity(request, userId, workflowId, "view")
    context
      .select(WORKFLOW_VIEW_COUNT.VIEW_COUNT)
      .from(WORKFLOW_VIEW_COUNT)
      .where(WORKFLOW_VIEW_COUNT.WID.eq(workflowId))
      .fetchOneInto(classOf[Int])
  }

  @GET
  @Path("/viewCount/{wid}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getViewCount(@PathParam("wid") wid: UInteger): Int = {

    context
      .insertInto(WORKFLOW_VIEW_COUNT)
      .set(WORKFLOW_VIEW_COUNT.WID, wid)
      .set(WORKFLOW_VIEW_COUNT.VIEW_COUNT, UInteger.valueOf(0))
      .onDuplicateKeyIgnore()
      .execute()

    context
      .select(WORKFLOW_VIEW_COUNT.VIEW_COUNT)
      .from(WORKFLOW_VIEW_COUNT)
      .where(WORKFLOW_VIEW_COUNT.WID.eq(wid))
      .fetchOneInto(classOf[Int])
  }
}
