package edu.uci.ics.texera.web.resource.dashboard.user.file

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.common.{AccessEntry, AccessEntry2}
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{FILE, USER, USER_FILE_ACCESS}
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
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

/**
  * A Utility Class used to for operations related to database
  */
object UserFileAccessResource {
  private lazy val userFileAccessDao = new UserFileAccessDao(
    context.configuration
  )
  private lazy val userDao = new UserDao(context.configuration)
  private lazy val context: DSLContext = SqlServer.createDSLContext

  def getFileId(ownerName: String, fileName: String): UInteger = {
    val uid = userDao.fetchByName(ownerName).get(0).getUid
    val file = context
      .select(FILE.FID)
      .from(FILE)
      .where(FILE.OWNER_UID.eq(uid).and(FILE.NAME.eq(fileName)))
      .fetch()
    file.getValue(0, 0).asInstanceOf[UInteger]
  }

  def grantAccess(uid: UInteger, fid: UInteger, accessLevel: String): Unit = {
    if (UserFileAccessResource.hasAccessTo(uid, fid)) {
      if (accessLevel == "read") {
        userFileAccessDao.update(new UserFileAccess(uid, fid, true, false))
      } else {
        userFileAccessDao.update(new UserFileAccess(uid, fid, true, true))
      }

    } else {
      if (accessLevel == "read") {
        userFileAccessDao.insert(new UserFileAccess(uid, fid, true, false))
      } else {
        userFileAccessDao.insert(new UserFileAccess(uid, fid, true, true))
      }
    }
  }

  def hasAccessTo(uid: UInteger, fid: UInteger): Boolean = {
    context
      .fetchExists(
        context
          .selectFrom(USER_FILE_ACCESS)
          .where(USER_FILE_ACCESS.UID.eq(uid).and(USER_FILE_ACCESS.FID.eq(fid)))
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
    * @param fileName the file name of target file to be shared
    * @param ownerName the name of the file's owner
    * @return a List of email/permission pair
    */
  @GET
  @Path("list/{fileName}/{ownerName}")
  def getAccessList(
      @PathParam("fileName") fileName: String,
      @PathParam("ownerName") ownerName: String,
      @Auth sessionUser: SessionUser
  ): List[AccessEntry2] = {
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
          .eq(UserFileAccessResource.getFileId(ownerName, fileName))
          .and(USER_FILE_ACCESS.UID.notEqual(sessionUser.getUser.getUid))
      )
      .fetch()
      .map(access => {
        access.into(classOf[AccessEntry2])
      })
      .toList
  }

  /**
   * This method shares a file to a user with a specific access type
   * @param fileName     the filename of target file to be shared to
   * @param ownerName the name of the file's owner
   * @param email       the email of target user to be shared
   * @param privilege the type of access to be shared
   * @return rejection if user not permitted to share the workflow or Success Message
   */
  @PUT
  @Path("/grant/{ownerName}/{filename}/{email}/{privilege}")
  def grantAccess(
       @PathParam("ownerName") ownerName: String,
       @PathParam("filename") fileName: String,
       @PathParam("email") email: String,
       @PathParam("privilege") privilege: String
  ): Unit = {
    userFileAccessDao.merge(
      new UserFileAccess(
        userDao.fetchOneByEmail(email).getUid,
        UserFileAccessResource.getFileId(ownerName, fileName),
        UserFileAccessPrivilege.valueOf(privilege)
      )
    )
  }

  /**
    * Revoke a user's access to a file
    * @param ownerName the name of the file's owner
    * @param fileName    the file name of target file to be shared
    * @param email the email of target user whose access is about to be revoked
    * @return A successful resp if granted, failed resp otherwise
    */
  @DELETE
  @Path("/revoke/{ownerName}/{fileName}/{email}")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def revokeAccess(
      @PathParam("fileName") fileName: String,
      @PathParam("ownerName") ownerName: String,
      @PathParam("email") email: String
  ): Unit = {
    context
      .delete(USER_FILE_ACCESS)
      .where(
        USER_FILE_ACCESS.UID
          .eq(userDao.fetchOneByEmail(email).getUid)
          .and(USER_FILE_ACCESS.FID.eq(UserFileAccessResource.getFileId(ownerName, fileName)))
      )
      .execute()
  }
}
