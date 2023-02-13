package edu.uci.ics.texera.web.resource.dashboard.workflow

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.common.AccessEntry2
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{
  USER,
  WORKFLOW_OF_USER,
  WORKFLOW_USER_ACCESS
}
import edu.uci.ics.texera.web.model.jooq.generated.enums.WorkflowUserAccessPrivilege
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{
  UserDao,
  WorkflowOfUserDao,
  WorkflowUserAccessDao
}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.WorkflowUserAccess
import edu.uci.ics.texera.web.resource.dashboard.workflow.WorkflowAccessResource.{context, userDao}
import io.dropwizard.auth.Auth
import org.jooq.{DSLContext, Record3}
import org.jooq.types.UInteger

import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object WorkflowAccessResource {
  private var context: DSLContext = SqlServer.createDSLContext
  final private val userDao = new UserDao(context.configuration())

  def getPrivilege(wid: UInteger, uid: UInteger): WorkflowUserAccessPrivilege = {
    val access = context
      .select()
      .from(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.WID.eq(wid).and(WORKFLOW_USER_ACCESS.UID.eq(uid)))
      .fetchOneInto(classOf[WorkflowUserAccess])
    if (access == null) {
      WorkflowUserAccessPrivilege.NONE
    } else {
      access.getPrivilege
    }
  }

  def hasReadAccess(wid: UInteger, uid: UInteger): Boolean = {
    getPrivilege(wid, uid).eq(WorkflowUserAccessPrivilege.READ)
  }

  def hasWriteAccess(wid: UInteger, uid: UInteger): Boolean = {
    getPrivilege(wid, uid).eq(WorkflowUserAccessPrivilege.WRITE)
  }

  def hasAccess(wid: UInteger, uid: UInteger): Boolean = {
    hasReadAccess(wid, uid) || hasWriteAccess(wid, uid)
  }
}

@Produces(Array(MediaType.APPLICATION_JSON))
@RolesAllowed(Array("REGULAR", "ADMIN"))
@Path("/workflow/access")
class WorkflowAccessResource() {

  private val workflowOfUserDao = new WorkflowOfUserDao(
    context.configuration
  )
  private val workflowUserAccessDao = new WorkflowUserAccessDao(
    context.configuration
  )

  def this(dslContext: DSLContext) {
    this()
    context = dslContext
  }

  @GET
  @Path("/owner/{wid}")
  def getOwner(@PathParam("wid") wid: UInteger): String = {
    val uid = workflowOfUserDao.fetchByWid(wid).get(0).getUid
    userDao.fetchOneByUid(uid).getEmail
  }

  @GET
  @Path("/list/{wid}")
  def getList(
      @PathParam("wid") wid: UInteger,
      @Auth sessionUser: SessionUser
  ): List[AccessEntry2] = {
    val user = sessionUser.getUser
    if (
      workflowOfUserDao.existsById(
        context
          .newRecord(WORKFLOW_OF_USER.UID, WORKFLOW_OF_USER.WID)
          .values(user.getUid, wid)
      )
    ) {
      context
        .select(
          USER.EMAIL,
          USER.NAME,
          WORKFLOW_USER_ACCESS.PRIVILEGE
        )
        .from(WORKFLOW_USER_ACCESS)
        .join(USER)
        .on(USER.UID.eq(WORKFLOW_USER_ACCESS.UID))
        .where(WORKFLOW_USER_ACCESS.WID.eq(wid).and(WORKFLOW_USER_ACCESS.UID.notEqual(user.getUid)))
        .fetch()
        .map(access => { access.into(classOf[AccessEntry2]) })
        .toList
    } else {
      throw new ForbiddenException("You are not the owner of the workflow.")
    }
  }

  @DELETE
  @Path("/revoke/{wid}/{email}")
  def revokeAccess(
      @PathParam("wid") wid: UInteger,
      @PathParam("email") email: String,
      @Auth sessionUser: SessionUser
  ): Unit = {
    if (
      !workflowOfUserDao.existsById(
        context
          .newRecord(WORKFLOW_OF_USER.UID, WORKFLOW_OF_USER.WID)
          .values(sessionUser.getUser.getUid, wid)
      )
    ) {
      throw new ForbiddenException("No sufficient access privilege.")
    } else {
      context
        .delete(WORKFLOW_USER_ACCESS)
        .where(
          WORKFLOW_USER_ACCESS.UID
            .eq(userDao.fetchOneByEmail(email).getUid)
            .and(WORKFLOW_USER_ACCESS.WID.eq(wid))
        )
        .execute()
    }
  }

  @PUT
  @Path("/grant/{wid}/{email}/{accessLevel}")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def grantAccess(
      @PathParam("wid") wid: UInteger,
      @PathParam("email") email: String,
      @PathParam("accessLevel") accessLevel: String,
      @Auth sessionUser: SessionUser
  ): Unit = {
    val uid: UInteger =
      try {
        userDao.fetchOneByEmail(email).getUid
      } catch {
        case _: IndexOutOfBoundsException =>
          throw new BadRequestException("Target user does not exist.")
      }
    if (
      !workflowOfUserDao.existsById(
        context
          .newRecord(WORKFLOW_OF_USER.UID, WORKFLOW_OF_USER.WID)
          .values(sessionUser.getUser.getUid, wid)
      )
    ) {
      throw new ForbiddenException("No sufficient access privilege.")
    } else {
      accessLevel match {
        case "read" =>
          workflowUserAccessDao.merge(
            new WorkflowUserAccess(
              uid,
              wid,
              WorkflowUserAccessPrivilege.READ
            )
          )
        case "write" =>
          workflowUserAccessDao.merge(
            new WorkflowUserAccess(
              uid,
              wid,
              WorkflowUserAccessPrivilege.WRITE
            )
          )
      }
    }
  }
}
