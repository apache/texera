package edu.uci.ics.texera.web.resource.dashboard.user.file

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{
  FILE,
  FILE_OF_WORKFLOW,
  USER,
  USER_FILE_ACCESS
}
import edu.uci.ics.texera.web.model.jooq.generated.enums.UserFileAccessPrivilege
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{UserDao, UserFileAccessDao}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.UserFileAccess
import edu.uci.ics.texera.web.resource.dashboard.user.file.UserFileAccessResource.{context, userDao}
import io.dropwizard.auth.Auth
import org.jooq.DSLContext
import org.jooq.types.UInteger

import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.MediaType

/**
  * A Utility Class used to for operations related to database
  */
object UserFileAccessResource {
  final private lazy val context: DSLContext = SqlServer.createDSLContext
  final private lazy val userDao = new UserDao(context.configuration)

  def getFileId(ownerName: String, fileName: String): UInteger = {
    val uid = userDao.fetchByName(ownerName).get(0).getUid
    val file = context
      .select(FILE.FID)
      .from(FILE)
      .where(FILE.OWNER_UID.eq(uid).and(FILE.NAME.eq(fileName)))
      .fetch()
    file.getValue(0, 0).asInstanceOf[UInteger]
  }

  def hasAccessTo(uid: UInteger, fid: UInteger): Boolean = {
    context
      .fetchExists(
        context
          .selectFrom(USER_FILE_ACCESS)
          .where(USER_FILE_ACCESS.UID.eq(uid).and(USER_FILE_ACCESS.FID.eq(fid)))
      )
  }

  def workflowHasFile(wid: UInteger, fid: UInteger): Boolean = {
    context
      .fetchExists(
        context
          .selectFrom(FILE_OF_WORKFLOW)
          .where(FILE_OF_WORKFLOW.WID.eq(wid).and(FILE_OF_WORKFLOW.FID.eq(fid)))
      )
  }
}
@Produces(Array(MediaType.APPLICATION_JSON))
@RolesAllowed(Array("REGULAR", "ADMIN"))
@Path("/access/file")
class UserFileAccessResource {
  final private val userFileAccessDao = new UserFileAccessDao(context.configuration)

  /**
    * Retrieves the list of all shared accesses of the target file
    * @param fid the id of the file
    * @return a List of email/name/permission pair
    */
  @GET
  @Path("list/{fid}")
  def getAccessList(
      @PathParam("fid") fid: UInteger,
      @Auth sessionUser: SessionUser
  ) = {
    context
      .select(
        USER.EMAIL,
        USER.NAME,
        USER_FILE_ACCESS.PRIVILEGE
      )
      .from(USER_FILE_ACCESS)
      .join(USER)
      .on(USER.UID.eq(USER_FILE_ACCESS.UID))
      .where(
        USER_FILE_ACCESS.FID
          .eq(fid)
          .and(USER_FILE_ACCESS.UID.notEqual(sessionUser.getUser.getUid))
      )
      .fetch()
  }

  /**
    * This method shares a file to a user with a specific access type
    *
    * @param fid       the id of target file to be shared to
    * @param email     the email of target user to be shared
    * @param privilege the type of access to be shared
    * @return rejection if user not permitted to share the workflow or Success Message
    */
  @PUT
  @Path("/grant/{fid}/{email}/{privilege}")
  def grantAccess(
      @PathParam("fid") fid: UInteger,
      @PathParam("email") email: String,
      @PathParam("privilege") privilege: String
  ): Unit = {
    userFileAccessDao.merge(
      new UserFileAccess(
        userDao.fetchOneByEmail(email).getUid,
        fid,
        UserFileAccessPrivilege.valueOf(privilege)
      )
    )
  }

  /**
    * Revoke a user's access to a file
    *
    * @param fid   the id of the file
    * @param email the email of target user whose access is about to be revoked
    * @return A successful resp if granted, failed resp otherwise
    */
  @DELETE
  @Path("/revoke/{fid}/{email}")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def revokeAccess(
      @PathParam("fid") fid: UInteger,
      @PathParam("email") email: String
  ): Unit = {
    context
      .delete(USER_FILE_ACCESS)
      .where(
        USER_FILE_ACCESS.UID
          .eq(userDao.fetchOneByEmail(email).getUid)
          .and(USER_FILE_ACCESS.FID.eq(fid))
      )
      .execute()
  }
}
