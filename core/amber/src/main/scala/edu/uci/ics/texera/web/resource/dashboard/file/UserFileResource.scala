package edu.uci.ics.texera.web.resource.dashboard.file

import com.google.common.io.Files
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{FILE, USER_FILE_ACCESS}
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{FileDao, UserDao, UserFileAccessDao}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{File, User}
import edu.uci.ics.texera.web.resource.dashboard.file.UserFileResource.{
  DashboardFileEntry,
  context,
  saveUserFileSafe
}
import io.dropwizard.auth.Auth
import org.apache.commons.lang3.tuple.Pair
import org.glassfish.jersey.media.multipart.{FormDataContentDisposition, FormDataParam}
import org.jooq.DSLContext
import org.jooq.types.UInteger

import java.io.{FileInputStream, IOException, InputStream, OutputStream}
import java.nio.file.Paths
import java.util
import javax.annotation.security.PermitAll
import javax.ws.rs.core.{MediaType, Response, StreamingOutput}
import javax.ws.rs.{WebApplicationException, _}
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Model `File` corresponds to `core/new-gui/src/app/common/type/user-file.ts` (frontend).
  */

object UserFileResource {
  private val context: DSLContext = SqlServer.createDSLContext
  private val fileDao = new FileDao(context.configuration)

  def saveUserFileSafe(
      uid: UInteger,
      fileName: String,
      uploadedInputStream: InputStream,
      size: UInteger,
      description: String
  ): String = {

    val fileNameStored = UserFileUtils.storeFileSafe(uploadedInputStream, fileName, uid)

    // insert record after completely storing the file on the file system.
    fileDao.insert(
      new File(
        uid,
        null,
        size,
        fileNameStored,
        UserFileUtils.getFilePath(uid, fileNameStored).toString,
        description
      )
    )

    // insert UserFileAccess record to grant write access
    val fid = context
      .select(FILE.FID)
      .from(FILE)
      .where(FILE.UID.eq(uid).and(FILE.NAME.eq(fileNameStored)))
      .fetchOneInto(FILE)
      .getFid
    UserFileAccessResource.grantAccess(uid, fid, "write")
    fileNameStored
  }
  case class DashboardFileEntry(
      ownerName: String,
      accessLevel: String,
      isOwner: Boolean,
      file: File
  )
}

@PermitAll
@Path("/user/file")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class UserFileResource {
  final private val fileDao = new FileDao(context.configuration)
  final private val userFileAccessDao = new UserFileAccessDao(
    context.configuration
  )
  final private val userDao = new UserDao(context.configuration)

  /**
    * This method will handle the request to upload a single file.
    * @return
    */
  @POST
  @Path("/upload")
  @Consumes(Array(MediaType.MULTIPART_FORM_DATA))
  def uploadFile(
      @FormDataParam("file") uploadedInputStream: InputStream,
      @FormDataParam("file") fileDetail: FormDataContentDisposition,
      @FormDataParam("size") size: UInteger,
      @FormDataParam("description") description: String,
      @Auth sessionUser: SessionUser
  ): Response = {
    val user = sessionUser.getUser
    val uid = user.getUid
    val fileName = fileDetail.getFileName
    val validationResult = validateFileName(fileName, uid)
    if (!validationResult.getLeft) {
      return Response
        .status(Response.Status.BAD_REQUEST)
        .entity(validationResult.getRight)
        .build()
    }
    saveUserFileSafe(uid, fileName, uploadedInputStream, size, description)
    Response.ok().build()
  }

  /**
    * This method returns a list fo all files accessible by the current user
    *
    * @return
    */
  @GET
  @Path("/list")
  def listUserFiles(@Auth sessionUser: SessionUser): util.List[DashboardFileEntry] = {
    val user = sessionUser.getUser
    getUserFileRecord(user)

  }

  private def getUserFileRecord(user: User): util.List[DashboardFileEntry] = {
    val accesses = userFileAccessDao.fetchByUid(user.getUid)
    val fileEntries: mutable.ArrayBuffer[DashboardFileEntry] = mutable.ArrayBuffer()
    accesses.asScala.toList.map(access => {
      val fid = access.getFid
      val file = fileDao.fetchOneByFid(fid)
      var accessLevel = "None"
      if (access.getWriteAccess) {
        accessLevel = "Write"
      } else if (access.getReadAccess) {
        accessLevel = "Read"
      } else {
        accessLevel = "None"
      }
      val ownerName = userDao.fetchOneByUid(file.getUid).getName
      fileEntries += DashboardFileEntry(
        ownerName,
        accessLevel,
        ownerName == user.getName,
        file
      )
    })
    fileEntries.toList.asJava
  }

  /**
    * This method deletes a file from a user's repository
    * @param fileName the name of file being deleted
    * @param ownerName the name of the file's owner
    * @return
    */
  @DELETE
  @Path("/delete/{fileName}/{ownerName}")
  def deleteUserFile(
      @PathParam("fileName") fileName: String,
      @PathParam("ownerName") ownerName: String,
      @Auth sessionUser: SessionUser
  ): Response = {

    val user = sessionUser.getUser
    val fileID = UserFileAccessResource.getFileId(ownerName, fileName)
    val userID = user.getUid
    val hasWriteAccess = context
      .select(USER_FILE_ACCESS.WRITE_ACCESS)
      .from(USER_FILE_ACCESS)
      .where(USER_FILE_ACCESS.UID.eq(userID).and(USER_FILE_ACCESS.FID.eq(fileID)))
      .fetch()
      .getValue(0, 0)
    if (hasWriteAccess == false) {
      Response
        .status(Response.Status.UNAUTHORIZED)
        .entity("You do not have the access to deleting the file")
        .build()
    } else {
      val filePath = fileDao.fetchOneByFid(fileID).getPath
      UserFileUtils.deleteFile(Paths.get(filePath))
      fileDao.deleteById(fileID)
      Response.ok().build()
    }
  }

  @POST
  @Path("/validate")
  @Consumes(Array(MediaType.MULTIPART_FORM_DATA))
  def validateUserFile(
      @FormDataParam("name") fileName: String,
      @Auth sessionUser: SessionUser
  ): Response = {
    val user = sessionUser.getUser
    val validationResult = validateFileName(fileName, user.getUid)
    if (validationResult.getLeft)
      Response.ok().build()
    else {
      Response.status(Response.Status.BAD_REQUEST).entity(validationResult.getRight).build()
    }

  }

  private def validateFileName(fileName: String, userID: UInteger): Pair[Boolean, String] = {
    if (fileName == null) Pair.of(false, "file name cannot be null")
    else if (fileName.trim.isEmpty) Pair.of(false, "file name cannot be empty")
    else if (isFileNameExisted(fileName, userID)) Pair.of(false, "file name already exists")
    else Pair.of(true, "filename validation success")
  }

  private def isFileNameExisted(fileName: String, userID: UInteger): Boolean =
    context.fetchExists(
      context
        .selectFrom(FILE)
        .where(FILE.UID.equal(userID).and(FILE.NAME.equal(fileName)))
    )

  @GET
  @Path("/download/{fileId}")
  def downloadFile(
      @PathParam("fileId") fileId: UInteger,
      @Auth sessionUser: SessionUser
  ): Response = {
    val user = sessionUser.getUser
    val filePath: Option[java.nio.file.Path] =
      UserFileUtils.getFilePathByIds(user.getUid, fileId)
    if (filePath.isDefined) {
      val fileObject = filePath.get.toFile

      // sending a FileOutputStream/ByteArrayOutputStream directly will cause MessageBodyWriter
      // not found issue for jersey
      // so we create our own stream.
      val fileStream = new StreamingOutput() {
        @throws[IOException]
        @throws[WebApplicationException]
        def write(output: OutputStream): Unit = {
          val data = Files.toByteArray(fileObject)
          output.write(data)
          output.flush()
        }
      }
      Response
        .ok(fileStream, MediaType.APPLICATION_OCTET_STREAM)
        .header(
          "content-disposition",
          String.format("attachment; filename=%s", fileObject.getName)
        )
        .build
    } else {

      Response
        .status(Response.Status.BAD_REQUEST)
        .`type`(MediaType.TEXT_PLAIN)
        .entity(s"Could not find file $fileId of ${user.getName}")
        .build()
    }

  }

  /**
    * This method updates the name of a given userFile
    *
    * @param file the to be updated file
    * @return the updated userFile
    */
  @POST
  @Path("/update/name")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  def changeUserFileName(file: File, @Auth sessionUser: SessionUser): Unit = {
    val userId = sessionUser.getUser.getUid
    val fid = file.getFid
    val newFileName = file.getName

    val validationRes = this.validateFileName(newFileName, userId)
    val hasWriteAccess = context
      .select(USER_FILE_ACCESS.WRITE_ACCESS)
      .from(USER_FILE_ACCESS)
      .where(USER_FILE_ACCESS.UID.eq(userId).and(USER_FILE_ACCESS.FID.eq(fid)))
      .fetch()
      .getValue(0, 0)
    if (hasWriteAccess == false) {
      throw new ForbiddenException("No sufficient access privilege.")
    }
    if (validationRes.getLeft == false) {
      throw new BadRequestException(validationRes.getRight)
    } else {
      val userFile = fileDao.fetchOneByFid(fid)
      val filePath = userFile.getPath

      val uploadedInputStream = new FileInputStream(filePath)
      // delete the original file
      UserFileUtils.deleteFile(Paths.get(filePath))
      // store the file with the new file name
      val fileNameStored = UserFileUtils.storeFileSafe(uploadedInputStream, newFileName, userId)

      userFile.setName(newFileName)
      userFile.setPath(UserFileUtils.getFilePath(userId, fileNameStored).toString)
      fileDao.update(userFile)
    }
  }

}
