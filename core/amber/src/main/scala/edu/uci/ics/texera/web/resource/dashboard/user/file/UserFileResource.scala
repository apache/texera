package edu.uci.ics.texera.web.resource.dashboard.user.file

import com.google.common.io.Files
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{
  FILE,
  FILE_OF_WORKFLOW,
  USER,
  USER_FILE_ACCESS,
  WORKFLOW_USER_ACCESS
}
import edu.uci.ics.texera.web.model.jooq.generated.enums.UserFileAccessPrivilege
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{
  FileDao,
  FileOfProjectDao
}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{File, User}
import edu.uci.ics.texera.web.resource.dashboard.user.file.UserFileResource.{
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
import java.net.URLDecoder
import java.nio.file.Paths
import java.sql.Timestamp
import java.time.Instant
import java.util
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response, StreamingOutput}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Model `File` corresponds to `core/new-gui/src/app/common/type/user-file.ts` (frontend).
  */

object UserFileResource {
  private lazy val context: DSLContext = SqlServer.createDSLContext
  private lazy val fileDao = new FileDao(context.configuration)

  def saveUserFileSafe(
      uid: UInteger,
      fileName: String,
      uploadedInputStream: InputStream,
      description: String
  ): String = {

    val fileNameStored = UserFileUtils.storeFileSafe(uploadedInputStream, fileName, uid)
    // insert record after completely storing the file on the file system.
    fileDao.insert(
      new File(
        uid,
        null,
        UInteger.valueOf(UserFileUtils.getFilePath(uid, fileNameStored).toFile.length()),
        fileNameStored,
        UserFileUtils.getFilePath(uid, fileNameStored).toString,
        description,
        Timestamp.from(Instant.now())
      )
    )
    fileNameStored
  }

  case class DashboardFileEntry(
      ownerName: String,
      accessLevel: String,
      isOwner: Boolean,
      file: File,
      projectIDs: List[UInteger]
  )
}
@Produces(Array(MediaType.APPLICATION_JSON))
@RolesAllowed(Array("REGULAR", "ADMIN"))
@Path("/user/file")
class UserFileResource {
  final private val fileDao = new FileDao(context.configuration)
  final private val fileOfProjectDao = new FileOfProjectDao(context.configuration)

  /**
    * This method will handle the request to upload a single file.
    * @return
    */
  @POST
  @Consumes(Array(MediaType.MULTIPART_FORM_DATA))
  @Path("/upload")
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
    saveUserFileSafe(uid, fileName, uploadedInputStream, description)
    Response.ok().build()
  }

  /**
    * This method returns a list of all files accessible by the current user
    *
    * @return
    */
  @GET
  @Path("/list")
  def listUserFiles(@Auth sessionUser: SessionUser): util.List[DashboardFileEntry] = {
    getUserFileRecord(sessionUser.getUser)
  }

  private def getUserFileRecord(user: User): util.List[DashboardFileEntry] = {
    // fetch the user files:
    // user_file_access JOIN file on FID (to get all files this user can access)
    // then JOIN USER on UID (to get the name of the file owner)
    val sharedFileRecords = context
      .select()
      .from(USER_FILE_ACCESS)
      .join(FILE)
      .on(USER_FILE_ACCESS.FID.eq(FILE.FID))
      .join(USER)
      .on(FILE.OWNER_UID.eq(USER.UID))
      .where(USER_FILE_ACCESS.UID.eq(user.getUid))
      .fetch()

    // fetch the entire table of fileOfProject in memory, assuming this table is small
    val fileOfProjectMap = fileOfProjectDao
      .findAll()
      .asScala
      .groupBy(record => record.getFid)
      .mapValues(values => values.map(v => v.getPid).toList)

    val fileEntries: mutable.ArrayBuffer[DashboardFileEntry] = mutable.ArrayBuffer()
    sharedFileRecords.forEach(fileRecord => {
      val file = fileRecord.into(FILE)
      val owner = fileRecord.into(USER)
      val access = fileRecord.into(USER_FILE_ACCESS)

      var accessLevel = "None"
      if (access.getPrivilege == UserFileAccessPrivilege.WRITE) {
        accessLevel = "Write"
      } else if (access.getPrivilege == UserFileAccessPrivilege.READ) {
        accessLevel = "Read"
      }
      fileEntries += DashboardFileEntry(
        owner.getName,
        accessLevel,
        owner.getName == user.getName,
        new File(file),
        fileOfProjectMap.getOrElse(file.getFid, List())
      )
    })

    val workflowFileRecords = context
      .select()
      .from(FILE_OF_WORKFLOW)
      .join(FILE)
      .on(FILE_OF_WORKFLOW.FID.eq(FILE.FID))
      .join(USER)
      .on(FILE.OWNER_UID.eq(USER.UID))
      .join(WORKFLOW_USER_ACCESS)
      .on(FILE_OF_WORKFLOW.WID.eq(WORKFLOW_USER_ACCESS.WID))
      .where(WORKFLOW_USER_ACCESS.UID.eq(user.getUid))
      .fetch()

    workflowFileRecords.forEach(fileRecord => {
      val file = fileRecord.into(FILE)
      val owner = fileRecord.into(USER)

      fileEntries += DashboardFileEntry(
        owner.getName,
        "Read",
        isOwner = false,
        new File(file),
        List()
      )
    })


    fileDao.fetchByOwnerUid(user.getUid)
      .forEach(fileRecord => {
      fileEntries += DashboardFileEntry(
        user.getName,
        "Write",
        isOwner = false,
        new File(fileRecord),
        List()
      )
    })

    fileEntries.toList.asJava
  }

  @GET
  @Path("/autocomplete/{query:.*}")
  def autocompleteUserFiles(
      @Auth sessionUser: SessionUser,
      @PathParam("query") q: String
  ): util.List[String] = {
    // get the user files
    // select the filenames that applies the input
    val query = URLDecoder.decode(q, "UTF-8")
    val user = sessionUser.getUser
    val fileList: List[DashboardFileEntry] = getUserFileRecord(user).asScala.toList
    val filenames = ArrayBuffer[String]()
    val username = user.getName
    // get all the filename list
    for (i <- fileList) {
      filenames += i.file.getName
    }
    // select the filenames that apply
    val selectedByFile = ArrayBuffer[String]()
    val selectedByUsername = ArrayBuffer[String]()
    val selectedByFullPath = ArrayBuffer[String]()
    for (e <- filenames) {
      val fullPath = username + "/" + e
      if (e.contains(query) || query.isEmpty)
        selectedByFile += (username + "/" + e)
      else if (username.contains(query))
        selectedByUsername += (username + "/" + e)
      else if (fullPath.contains(query))
        selectedByFullPath += (username + "/" + e)
    }
    (selectedByFile ++ selectedByUsername ++ selectedByFullPath).toList.asJava
  }

  /**
    * This method deletes a file from a user's repository
    * @param fid id of the file
    * @return
    */
  @DELETE
  @Path("/delete/{fid}")
  def deleteUserFile(
      @PathParam("fid") fid: UInteger,
  ): Unit = {
    UserFileUtils.deleteFile(Paths.get(fileDao.fetchOneByFid(fid).getPath))
    fileDao.deleteById(fid)
  }

  @POST
  @Consumes(Array(MediaType.MULTIPART_FORM_DATA))
  @Path("/validate")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
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

  @GET
  @Path("/download/{fileId}")
  def downloadFile(
      @PathParam("fileId") fileId: UInteger,
      @Auth sessionUser: SessionUser
  ): Response = {
    val filePath: Option[java.nio.file.Path] = {
      if (UserFileAccessResource.hasAccessTo(sessionUser.getUser.getUid, fileId)) {
        Some(Paths.get(fileDao.fetchOneByFid(fileId).getPath))
      } else {
        None
      }
    }
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
  }

  /**
    * This method updates the name of a given userFile
    * @param fid the id of the file
    * @param name new file name
    * @return the updated userFile
    */
  @PUT
  @Path("/name/{fid}/{name}")
  def changeFileName(@PathParam("fid") fid: UInteger,
                     @PathParam("name") name: String,
                     @Auth sessionUser: SessionUser): Unit = {
    val userId = sessionUser.getUser.getUid
    val validationRes = this.validateFileName(name, userId)

    if (!validationRes.getLeft) {
      throw new BadRequestException(validationRes.getRight)
    } else {
      val userFile = fileDao.fetchOneByFid(fid)
      val filePath = userFile.getPath

      val uploadedInputStream = new FileInputStream(filePath)
      // delete the original file
      UserFileUtils.deleteFile(Paths.get(filePath))
      // store the file with the new file name
      val fileNameStored = UserFileUtils.storeFileSafe(uploadedInputStream, name, userId)

      userFile.setName(name)
      userFile.setPath(UserFileUtils.getFilePath(userId, fileNameStored).toString)
      fileDao.update(userFile)
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
        .where(FILE.OWNER_UID.equal(userID).and(FILE.NAME.equal(fileName)))
    )

  /**
    * This method updates the description of a given userFile
    * @param fid the id of the file
    * @param description the id of the file
    * @return the updated userFile
    */
  @PUT
  @Path("/description/{fid}/{description}")
  def changeFileDescription(@PathParam("fid") fid: UInteger,
                            @PathParam("description") description: String): Unit = {
    val userFile = fileDao.fetchOneByFid(fid)
    userFile.setDescription(description)
    fileDao.update(userFile)
  }
}
