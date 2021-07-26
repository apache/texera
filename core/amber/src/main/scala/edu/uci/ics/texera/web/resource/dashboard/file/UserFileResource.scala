package edu.uci.ics.texera.web.resource.dashboard.file

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.Tables.FILE
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{FileDao, UserDao, UserFileAccessDao}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.File
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
import scala.collection.mutable

/**
  * Model `File` corresponds to `core/new-gui/src/app/common/type/user-file.ts` (frontend).
  */
case class fileRecord(ownerName: String, fileName: String, size: UInteger, description: String)
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
  @Path("/list")
  def listUserFiles(@Session session: HttpSession): util.List[fileRecord] = {
    UserResource.getUser(session) match {
      case Some(user) => getUserFileRecord(user.getUid)
      case None       => new util.ArrayList[fileRecord]()
    }
  }

  private def getUserFileRecord(userID: UInteger): util.List[fileRecord] = {
    // TODO: verify user in session?
    val accesses = userFileAccessDao.fetchByUid(userID)
    var files: mutable.ArrayBuffer[fileRecord] = mutable.ArrayBuffer()
    accesses.asScala.toList.map((access) => {
      val fid = access.getFid
      val file = fileDao.fetchOneByFid(fid)
      files += fileRecord(
        userDao.fetchOneByUid(file.getUid).getName,
        file.getName,
        file.getSize,
        file.getDescription
      )
    })
    val ownFiles = fileDao.fetchByUid(userID)
    ownFiles.asScala.toList.map(file => {
      files += fileRecord(
        userDao.fetchOneByUid(file.getUid).getName,
        file.getName,
        file.getSize,
        file.getDescription
      )
    })
    files.toList.asJava
  }

  @DELETE
  @Path("/delete/{fileName}/{ownerName}")
  def deleteUserFile(
      @PathParam("fileName") fileName: String,
      @PathParam("ownerName") ownerName: String,
      @Session session: HttpSession
  ): Response = {

    UserResource.getUser(session) match {
      case Some(user) =>
        val fileID = FileAccessUtils.getFileId(ownerName, fileName)
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
