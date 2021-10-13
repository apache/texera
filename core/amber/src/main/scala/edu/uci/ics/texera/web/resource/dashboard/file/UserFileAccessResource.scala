package edu.uci.ics.texera.web.resource.dashboard.file

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.common.AccessEntry
import edu.uci.ics.texera.web.model.http.request.auth.GrantAccessRequest
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{FILE, USER_FILE_ACCESS}
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{UserDao, UserFileAccessDao}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.UserFileAccess
import edu.uci.ics.texera.web.resource.dashboard.file.UserFileAccessResource.{
  context,
  grantAccess,
  userDao
}
import org.jooq.DSLContext
import org.jooq.types.UInteger

import javax.annotation.security.PermitAll
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.collection.JavaConverters._

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
      .where(FILE.UID.eq(uid).and(FILE.NAME.eq(fileName)))
      .fetch()
    file.getValue(0, 0).asInstanceOf[UInteger]
  }

  def getUidOfUser(username: String): UInteger = {
    userDao.fetchByName(username).get(0).getUid
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
@PermitAll
@Path("/user/file/access")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class UserFileAccessResource {

  /**
    * Retrieves the list of all shared accesses of the target file
    * @param fileName the file name of target file to be shared
    * @param ownerName the name of the file's owner
    * @return A JSON array of File Accesses, Ex: [{username: TestUser, fileAccess: read}]
    */
  @GET
  @Path("list/{fileName}/{ownerName}")
  def getAllSharedFileAccess(
      @PathParam("fileName") fileName: String,
      @PathParam("ownerName") ownerName: String
  ): List[AccessEntry] = {
    val fid = UserFileAccessResource.getFileId(ownerName, fileName)
    val fileAccess = context
      .select(USER_FILE_ACCESS.UID, USER_FILE_ACCESS.READ_ACCESS, USER_FILE_ACCESS.WRITE_ACCESS)
      .from(USER_FILE_ACCESS)
      .where(USER_FILE_ACCESS.FID.eq(fid))
      .fetch()
    fileAccess
      .getValues(0)
      .asScala
      .toList
      .zipWithIndex
      .map({
        case (uid, index) =>
          val userName = userDao.fetchOneByUid(uid.asInstanceOf[UInteger]).getName
          if (userName == ownerName) {
            AccessEntry(userName, "Owner")
          } else if (fileAccess.getValue(index, 2) == true) {
            AccessEntry(userName, "Write")
          } else {
            AccessEntry(userName, "Read")
          }
      })
  }

  /**
    * Grants a specific type of access of a file to a user
    *
    * @return A successful resp if granted, failed resp otherwise
    */
  @POST
  @Path("grant")
  def grantAccessTo(
      request: GrantAccessRequest
  ): Unit = {
    val fid = UserFileAccessResource.getFileId(request.ownerName, request.fileName)
    val uid: UInteger =
      try {
        userDao.fetchByName(request.username).get(0).getUid
      } catch {
        case _: NullPointerException =>
          throw new BadRequestException("Target User does not exist.")
      }
    grantAccess(uid, fid, request.accessLevel)
  }

  /**
    * Revoke a user's access to a file
    *
    * @param fileName    the file name of target file to be shared
    * @param ownerName the name of the file's owner
    * @param username the username of target user whose access is about to be revoked
    * @return A successful resp if granted, failed resp otherwise
    */
  @DELETE
  @Path("/revoke/{fileName}/{ownerName}/{username}")
  def revokeFileAccess(
      @PathParam("fileName") fileName: String,
      @PathParam("ownerName") ownerName: String,
      @PathParam("username") username: String
  ): Unit = {
    val fid = UserFileAccessResource.getFileId(ownerName, fileName)
    val uid: UInteger =
      try {
        userDao.fetchByName(username).get(0).getUid
      } catch {
        case _: NullPointerException =>
          throw new BadRequestException("Target User does not exist.")
      }
    context
      .deleteFrom(USER_FILE_ACCESS)
      .where(USER_FILE_ACCESS.UID.eq(uid).and(USER_FILE_ACCESS.FID.eq(fid)))
      .execute()
  }
}
