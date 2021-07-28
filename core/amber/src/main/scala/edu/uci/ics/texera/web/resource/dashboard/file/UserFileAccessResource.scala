package edu.uci.ics.texera.web.resource.dashboard.file

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{FILE, USER_FILE_ACCESS}
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{UserDao, UserFileAccessDao}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.UserFileAccess
import edu.uci.ics.texera.web.resource.auth.UserResource
import edu.uci.ics.texera.web.resource.dashboard.file.UserFileAccessResource.context
import io.dropwizard.jersey.sessions.Session
import org.jooq.DSLContext
import org.jooq.types.UInteger

import javax.servlet.http.HttpSession
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import scala.collection.JavaConverters._

/**
  * A Utility Class used to for operations related to database
  */
object UserFileAccessResource {
  private var context: DSLContext = SqlServer.createDSLContext

  def hasAccessTo(uid: UInteger, fid: UInteger): Boolean = {
    context
      .fetchExists(
        context
          .selectFrom(USER_FILE_ACCESS)
          .where(USER_FILE_ACCESS.UID.eq(uid).and(USER_FILE_ACCESS.FID.eq(fid)))
      )
  }

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
}

case class FileAccess(username: String, fileAccess: String)

@Path("/user-file-access")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class UserFileAccessResource {
  final private val userFileAccessDao = new UserFileAccessDao(
    context.configuration
  )
  final private val userDao = new UserDao(context.configuration)

  /**
    * Retrieves the list of all shared accesses of the target file
    * @param fid     the fileId of target file to be shared
    * @param session the session identifying the current user
    * @return A JSON array of File Accesses, Ex: [{username: TestUser, fileAccess: read}]
    */
  @GET
  @Path("list/{fileName}/{ownerName}")
  def getAllSharedFileAccess(
      @PathParam("fileName") fileName: String,
      @PathParam("ownerName") ownerName: String,
      @Session session: HttpSession
  ): Response = {
    UserResource.getUser(session) match {
      case Some(_) =>
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
                  val uname = userDao.fetchOneByUid(uid.asInstanceOf[UInteger]).getName
                  if (uname == ownerName) {
                    FileAccess(uname, "Owner")
                  } else if (fileAccess.getValue(index, 2) == true) {
                    FileAccess(uname, "Write")
                  } else {
                    FileAccess(uname, "Read")
                  }
              })
          )
          .build()
      case None =>
        Response.status(Response.Status.UNAUTHORIZED).entity("Please Login").build()
    }
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
      @Session session: HttpSession
  ): Response = {
    UserResource.getUser(session) match {
      case Some(_) =>
        val existence = context
          .fetchExists(
            context
              .selectFrom(USER_FILE_ACCESS)
              .where(USER_FILE_ACCESS.UID.eq(uid).and(USER_FILE_ACCESS.FID.eq(fid)))
          )
        if (existence) {
          Response.ok().entity().build()
        } else {
          Response.status(Response.Status.BAD_REQUEST).entity("user has no access to file").build()
        }
      case None => Response.status(Response.Status.UNAUTHORIZED).entity("please login").build()
    }
  }

  /**
    * Grants a specific type of access of a file to a user
    * @param fid     the fileId of target file to be shared
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
      @Session session: HttpSession
  ): Response = {
    UserResource.getUser(session) match {
      case Some(_) =>
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

        if (UserFileAccessResource.hasAccessTo(uid, fid)) {
          if (accessType == "read") {
            userFileAccessDao.update(new UserFileAccess(uid, fid, true, false))
          } else {
            userFileAccessDao.update(new UserFileAccess(uid, fid, true, true))
          }
          Response.ok().build()
        } else {
          if (accessType == "read") {
            userFileAccessDao.insert(new UserFileAccess(uid, fid, true, false))
          } else {
            userFileAccessDao.insert(new UserFileAccess(uid, fid, true, true))
          }
          Response.ok().build()
        }
    }
  }

  /**
    * Revoke a user's access to a file
    * @param fid     the fileId of target file
    * @param username the username of target user whose access is about to be revoked
    * @param session the session identifying the current user
    * @return A successful resp if granted, failed resp otherwise
    */
  @POST
  @Path("/revoke/{fileName}/{ownerName}/{username}")
  def revokeAccess(
      @PathParam("fileName") fileName: String,
      @PathParam("ownerName") ownerName: String,
      @PathParam("username") username: String,
      @Session session: HttpSession
  ): Response = {
    UserResource.getUser(session) match {
      case Some(_) =>
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
      case None =>
        Response.status(Response.Status.UNAUTHORIZED).entity("please login").build()
    }
  }
}
