package edu.uci.ics.texera.web.resource.dashboard.file

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{FILE, USER_FILE_ACCESS}
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{FileDao, UserDao, UserFileAccessDao}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{File, UserFileAccess}
import edu.uci.ics.texera.web.resource.auth.UserResource
import io.dropwizard.jersey.sessions.Session
import org.apache.commons.lang3.tuple.Pair
import org.glassfish.jersey.media.multipart.{FormDataContentDisposition, FormDataParam}
import org.jooq.DSLContext
import org.jooq.types.UInteger

import java.io.InputStream
import java.nio.file.Paths
import java.util
import javax.servlet.http.HttpSession
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import scala.collection.JavaConverters._

/**
  * Model `File` corresponds to `core/new-gui/src/app/common/type/user-file.ts` (frontend).
  */

object FileAccessUtils {

  def hasAccessTo(uid: UInteger, fid: UInteger): Boolean = {
    return SqlServer
      .createDSLContext()
      .fetchExists(
        SqlServer.createDSLContext
          .selectFrom(USER_FILE_ACCESS)
          .where(USER_FILE_ACCESS.UID.eq(uid).and(USER_FILE_ACCESS.FID.eq(fid)))
      )
  }
}

case class FileAccess(username: String, fileAccess: String)

@Path("/user/file")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class UserFileResource {

  final private val fileDao = new FileDao(SqlServer.createDSLContext.configuration)
  final private val userFileAccessDao = new UserFileAccessDao(
    SqlServer.createDSLContext.configuration
  )
  final private val userDao = new UserDao(SqlServer.createDSLContext().configuration)
  private var context: DSLContext = SqlServer.createDSLContext

  /**
    * This method will handle the request to upload a single file.
    * @return
    */
  @POST @Path("/upload")
  @Consumes(Array(MediaType.MULTIPART_FORM_DATA))
  def uploadFile(
      @FormDataParam("file") uploadedInputStream: InputStream,
      @FormDataParam("file") fileDetail: FormDataContentDisposition,
      @FormDataParam("size") size: UInteger,
      @FormDataParam("description") description: String,
      @Session session: HttpSession
  ): Response = {
    UserResource.getUser(session) match {
      case Some(user) =>
        val userID = user.getUid
        val fileName = fileDetail.getFileName
        val validationResult = validateFileName(fileName, userID)
        if (!validationResult.getLeft)
          return Response
            .status(Response.Status.BAD_REQUEST)
            .entity(validationResult.getRight)
            .build()

        UserFileUtils.storeFile(uploadedInputStream, fileName, userID.toString)

        // insert record after completely storing the file on the file system.
        fileDao.insert(
          new File(
            userID,
            null,
            size,
            fileName,
            UserFileUtils.getFilePath(userID.toString, fileName).toString,
            description
          )
        )
        Response.ok().build()
      case None =>
        Response.status(Response.Status.UNAUTHORIZED).build()
    }
  }

  @GET
  @Path("all-access-of/{fid}")
  def getAllSharedFileAccess(
      @PathParam("fid") fid: UInteger,
      @Session session: HttpSession
  ): Response = {
    UserResource.getUser(session) match {
      case Some(user) =>
        val fileAccess = SqlServer
          .createDSLContext()
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
                  if (fileAccess.getValue(index, 2) == true) {
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

  @GET
  @Path("/{uid}/has-access-to/{fid}")
  def hasAccessTo(
      @PathParam("uid") uid: UInteger,
      @PathParam("fid") fid: UInteger,
      @Session session: HttpSession
  ): Response = {
    UserResource.getUser(session) match {
      case Some(user) =>
        val existence = SqlServer
          .createDSLContext()
          .fetchExists(
            SqlServer.createDSLContext
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

  @POST
  @Path("share/{fid}/to/{username}/{accessType}")
  def shareFileTo(
      @PathParam("username") username: String,
      @PathParam("fid") fid: UInteger,
      @PathParam("accessType") accessType: String,
      @Session session: HttpSession
  ): Response = {
    UserResource.getUser(session) match {
      case Some(user) =>
        val uid: UInteger =
          try { userDao.fetchByName(username).get(0).getUid }
          catch {
            case _: NullPointerException =>
              return Response
                .status(Response.Status.BAD_REQUEST)
                .entity("Target User Does Not Exist")
                .build()
          }

        if (FileAccessUtils.hasAccessTo(uid, fid)) {
          if (accessType == "read") {
            userFileAccessDao.update(new UserFileAccess(uid, fid, true, false))
          } else {
            userFileAccessDao.update(new UserFileAccess(uid, fid, true, false))
          }
          Response.ok().build()
        } else {
          if (accessType == "read") {
            userFileAccessDao.insert(new UserFileAccess(uid, fid, true, false))
          } else {
            userFileAccessDao.insert(new UserFileAccess(uid, fid, true, false))
          }
          Response.ok().build()
        }
    }
  }

  @POST
  @Path("/revoke/{fid}/{username}")
  def revokeAccess(
      @PathParam("fid") fid: UInteger,
      @PathParam("username") username: String,
      @Session session: HttpSession
  ): Response = {
    UserResource.getUser(session) match {
      case Some(user) =>
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
        SqlServer
          .createDSLContext()
          .deleteFrom(USER_FILE_ACCESS)
          .where(USER_FILE_ACCESS.UID.eq(uid).and(USER_FILE_ACCESS.FID.eq(fid)))
          .execute()
        Response.ok().build()
      case None =>
        Response.status(Response.Status.UNAUTHORIZED).entity("please login").build()
    }
  }
  @GET
  @Path("/list")
  def listUserFiles(@Session session: HttpSession): util.List[File] = {
    UserResource.getUser(session) match {
      case Some(user) => getUserFileRecord(user.getUid)
      case None       => new util.ArrayList[File]()
    }
  }

  private def getUserFileRecord(userID: UInteger): util.List[File] = {

    // TODO: verify user in session?
    fileDao.fetchByUid(userID)
  }

  @DELETE
  @Path("/delete/{fileID}")
  def deleteUserFile(
      @PathParam("fileID") fileID: UInteger,
      @Session session: HttpSession
  ): Response = {

    UserResource.getUser(session) match {
      case Some(user) =>
        val userID = user.getUid
        // TODO: add user check
        val filePath = fileDao.fetchOneByFid(fileID).getPath
        UserFileUtils.deleteFile(Paths.get(filePath))
        fileDao.deleteById(fileID)
        Response.ok().build()
      case None =>
        Response.status(Response.Status.UNAUTHORIZED).build()
    }
  }

  @POST
  @Path("/validate")
  @Consumes(Array(MediaType.MULTIPART_FORM_DATA))
  def validateUserFile(
      @Session session: HttpSession,
      @FormDataParam("name") fileName: String
  ): Response = {
    UserResource.getUser(session) match {
      case Some(user) =>
        val validationResult = validateFileName(fileName, user.getUid)
        if (validationResult.getLeft)
          Response.ok().build()
        else {
          Response.status(Response.Status.BAD_REQUEST).entity(validationResult.getRight).build()
        }
      case None =>
        Response.status(Response.Status.UNAUTHORIZED).build()
    }

  }

  private def validateFileName(fileName: String, userID: UInteger): Pair[Boolean, String] = {
    if (fileName == null) Pair.of(false, "file name cannot be null")
    else if (fileName.trim.isEmpty) Pair.of(false, "file name cannot be empty")
    else if (isFileNameExisted(fileName, userID)) Pair.of(false, "file name already exists")
    else Pair.of(true, "filename validation success")
  }

  private def isFileNameExisted(fileName: String, userID: UInteger): Boolean =
    SqlServer.createDSLContext.fetchExists(
      SqlServer.createDSLContext
        .selectFrom(FILE)
        .where(FILE.UID.equal(userID).and(FILE.NAME.equal(fileName)))
    )
}
