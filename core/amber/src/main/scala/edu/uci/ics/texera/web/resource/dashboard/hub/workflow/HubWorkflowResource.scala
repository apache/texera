package edu.uci.ics.texera.web.resource.dashboard.hub.workflow

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables._
import edu.uci.ics.texera.web.model.jooq.generated.enums.UserRole
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.WorkflowDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{User, Workflow}
import edu.uci.ics.texera.web.resource.dashboard.hub.workflow.HubWorkflowResource.recordUserActivity
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowAccessResource
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource.WorkflowWithPrivilege

import java.util
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import org.jooq.types.UInteger

import java.util.regex.Pattern
import javax.servlet.http.HttpServletRequest
import javax.ws.rs.core.Context

object HubWorkflowResource {
  final private lazy val context = SqlServer.createDSLContext()

  final private val ipv4Pattern: Pattern = Pattern.compile(
    "^([0-9]{1,3}\\.){3}[0-9]{1,3}$"
  )

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
  final private lazy val context = SqlServer.createDSLContext()
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
  def getWorkflowCount: Integer = {
    context.selectCount
      .from(WORKFLOW)
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
    val dummyUser = new User()
    dummyUser.setRole(UserRole.REGULAR)
    if (WorkflowAccessResource.hasReadAccess(wid, new SessionUser(dummyUser).getUid)) {
      val workflow = workflowDao.fetchOneByWid(wid)
      WorkflowWithPrivilege(
        workflow.getName,
        workflow.getDescription,
        workflow.getWid,
        workflow.getContent,
        workflow.getCreationTime,
        workflow.getLastModifiedTime,
        workflow.getIsPublished,
        true
      )
    } else {
      throw new ForbiddenException("No sufficient access privilege.")
    }
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
}
