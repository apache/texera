package edu.uci.ics.texera.web.resource.dashboard.hub.workflow

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables._
import edu.uci.ics.texera.web.model.jooq.generated.enums.UserRole
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.WorkflowDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{User, Workflow}
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowAccessResource
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource.WorkflowWithPrivilege

import java.util
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import org.jooq.types.UInteger

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
  def likeWorkflow(likeRequest: Array[UInteger]): Boolean = {
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
      true
    } else {
      false
    }
  }

  @POST
  @Path("/unlike")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def unlikeWorkflow(likeRequest: Array[UInteger]): Boolean = {
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
      true
    } else {
      false
    }
  }
}
