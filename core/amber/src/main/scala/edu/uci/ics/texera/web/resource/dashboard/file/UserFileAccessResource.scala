package edu.uci.ics.texera.web.resource.dashboard.file

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.basicauth.SessionUser
import edu.uci.ics.texera.web.model.common.AccessEntry
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{FILE, USER_FILE_ACCESS}
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{UserDao, UserFileAccessDao}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.UserFileAccess
import edu.uci.ics.texera.web.resource.dashboard.file.UserFileAccessResource.{context, grantAccess}
import io.dropwizard.auth.Auth
import org.jooq.DSLContext
import org.jooq.types.UInteger

import javax.annotation.security.PermitAll
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import scala.collection.JavaConverters._

/**
  * A Utility Class used to for operations related to database
  */
object UserFileAccessResource {
  final private val context: DSLContext = SqlServer.createDSLContext
  final private val userFileAccessDao = new UserFileAccessDao(
    context.configuration
  )

  def getFileId(ownerName: String, fileName: String): UInteger = {
    val userDao = new UserDao(context.configuration)
    val uid = userDao.fetchByName(ownerName).get(0).getUid
    val file = context
      .select(FILE.FID)
      .from(FILE)
      .where(FILE.UID.eq(uid).and(FILE.NAME.eq(fileName)))
      .fetch()
    file.getValue(0, 0).asInstanceOf[UInteger]
  }

  def getUidOfUser(username: String): UInteger = {
    val userDao = new UserDao(context.configuration)
    userDao.fetchByName(username).get(0).getUid
  }

  def grantAccess(uid: UInteger, fid: UInteger, accessType: String): Unit = {
    if (UserFileAccessResource.hasAccessTo(uid, fid)) {
      if (accessType == "read") {
        userFileAccessDao.update(new UserFileAccess(uid, fid, true, false))
      } else {
        userFileAccessDao.update(new UserFileAccess(uid, fid, true, true))
      }

    } else {
      if (accessType == "read") {
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

  final private val userDao = new UserDao(context.configuration)

  /**
    * Retrieves the list of all shared accesses of the target file
    * @param fileName    the file name of target file to be shared
    * @param ownerName the name of the file's owner
    * @param session the session identifying the current user
    * @return A JSON array of File Accesses, Ex: [{username: TestUser, fileAccess: read}]
    */
  @GET
  @Path("list/{fileName}/{ownerName}")
  def getAllSharedFileAccess(
      @PathParam("fileName") fileName: String,
      @PathParam("ownerName") ownerName: String,
      @Auth sessionUser: SessionUser
  ): Response = {
    val user = sessionUser.getUser
    val fid = UserFileAccessResource.getFileId(ownerName, fileName)
    val fileAccess = context
      .select(USER_FILE_ACCESS.UID, USER_FILE_ACCESS.READ_ACCESS, USER_FILE_ACCESS.WRITE_ACCESS)
      .from(USER_FILE_ACCESS)
      .where(USER_FILE_ACCESS.FID.eq(fid))
      .fetch()
    Response
      .ok(
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
      )
      .build()
  }

  /**
    * Checks whether a user has access to a file
    * @param fid     the fileId of target file to be checked
    * @param uid     the userId of target user to be checked
    * @return success resp if has access, failed resp otherwise
    */
  @GET
  @Path("hasAccess/{uid}/{fid}")
  def hasAccessTo(
      @PathParam("uid") uid: UInteger,
      @PathParam("fid") fid: UInteger,
      @Auth sessionUser: SessionUser // TODO: check this unused sessionUser
  ): Response = {
    val exist = context
      .fetchExists(
        context
          .selectFrom(USER_FILE_ACCESS)
          .where(USER_FILE_ACCESS.UID.eq(uid).and(USER_FILE_ACCESS.FID.eq(fid)))
      )
    if (exist) {
      Response.ok().build()
    } else {
      Response.status(Response.Status.BAD_REQUEST).entity("user has no access to file").build()
    }

  }

  /**
    * Grants a specific type of access of a file to a user
    * @param fileName    the file name of target file to be shared
    * @param ownerName the name of the file's owner
    * @param username the username of target user to be shared to
    * @param accessType the type of access to be shared
    * @param session the session identifying the current user
    * @return A successful resp if granted, failed resp otherwise
    */
  @POST
  @Path("grant/{fileName}/{ownerName}/{username}/{accessType}")
  def shareFileTo(
      @PathParam("username") username: String,
      @PathParam("fileName") fileName: String,
      @PathParam("ownerName") ownerName: String,
      @PathParam("accessType") accessType: String,
      @Auth sessionUser: SessionUser // TODO: check this unused sessionUser
  ): Response = {
    val fid = UserFileAccessResource.getFileId(ownerName, fileName)
    val uid: UInteger =
      try {
        userDao.fetchByName(username).get(0).getUid
      } catch {
        case _: NullPointerException =>
          return Response
            .status(Response.Status.BAD_REQUEST)
            .entity("Target User Does Not Exist")
            .build()
      }

    grantAccess(uid, fid, accessType)
    Response.ok().build()
  }

  /**
    * Revoke a user's access to a file
    *
    * @param fileName    the file name of target file to be shared
    * @param ownerName the name of the file's owner
    * @param username the username of target user whose access is about to be revoked
    * @param session the session identifying the current user
    * @return A successful resp if granted, failed resp otherwise
    */
  @POST
  @Path("/revoke/{fileName}/{ownerName}/{username}")
  def revokeFileAccess(
      @PathParam("fileName") fileName: String,
      @PathParam("ownerName") ownerName: String,
      @PathParam("username") username: String,
      @Auth sessionUser: SessionUser // TODO: check this unused sessionUser
  ): Response = {

    val fid = UserFileAccessResource.getFileId(ownerName, fileName)
    val uid: UInteger =
      try {
        userDao.fetchByName(username).get(0).getUid
      } catch {
        case _: NullPointerException =>
          return Response
            .status(Response.Status.BAD_REQUEST)
            .entity("Target User Does Not Exist")
            .build()
      }
    context
      .deleteFrom(USER_FILE_ACCESS)
      .where(USER_FILE_ACCESS.UID.eq(uid).and(USER_FILE_ACCESS.FID.eq(fid)))
      .execute()
    Response.ok().build()

  }
}
