package edu.uci.ics.texera.web.resource.dashboard.user.file

import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables._
import edu.uci.ics.texera.web.model.jooq.generated.enums.UserFileAccessPrivilege
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{FileDao, FileOfProjectDao}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{File, User}
import edu.uci.ics.texera.web.resource.dashboard.user.file.UserFileAccessResource.{
  checkReadAccess,
  checkWriteAccess
}
import edu.uci.ics.texera.web.resource.dashboard.user.file.UserFileResource.{
  DashboardFileEntry,
  context,
  fileDao,
  saveFile
}
import io.dropwizard.auth.Auth
import org.apache.commons.lang3.tuple.Pair
import org.glassfish.jersey.media.multipart.FormDataParam
import org.jooq.DSLContext
import org.jooq.types.UInteger

import java.io.{InputStream, OutputStream}
import java.net.URLDecoder
import java.nio.file.{Files, Paths}
import java.util
import java.util.UUID
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response, StreamingOutput}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object UserFileResource {
  final private lazy val context: DSLContext = SqlServer.createDSLContext
  final private lazy val fileDao = new FileDao(context.configuration)

  def saveFile(uid: UInteger, fileName: String, stream: InputStream, des: String = ""): Unit = {
    val path = Utils.amberHomePath.resolve("user-resources").resolve("files").resolve(uid.toString)
    Files.createDirectories(path)
    val filepath = path.resolve(UUID.randomUUID.toString)
    Files.copy(stream, filepath)
    fileDao.insert(
      new File(
        uid,
        null,
        UInteger.valueOf(filepath.toFile.length()),
        fileName,
        filepath.toString,
        des,
        null
      )
    )
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
  final private val fileOfProjectDao = new FileOfProjectDao(context.configuration)

  /**
    * This method will handle the request to upload a single file.
    * @return
    */
  @POST
  @Consumes(Array(MediaType.MULTIPART_FORM_DATA))
  @Path("/upload")
  def uploadFile(
      @FormDataParam("file") stream: InputStream,
      @FormDataParam("name") fileName: String,
      @Auth sessionUser: SessionUser
  ): Response = {
    val user = sessionUser.getUser
    val uid = user.getUid
    val validationResult = validateFileName(fileName, uid)
    if (!validationResult.getLeft) {
      return Response
        .status(Response.Status.BAD_REQUEST)
        .entity(validationResult.getRight)
        .build()
    }
    saveFile(uid: UInteger, fileName, stream)
    Response.ok().build()
  }

  /**
    * This method returns a list of all files accessible by the current user
    *
    * @return
    */
  @GET
  @Path("/list")
  def getFileList(@Auth sessionUser: SessionUser): util.List[DashboardFileEntry] = {
    getFileRecord(sessionUser.getUser)
  }

  private def getFileRecord(user: User): util.List[DashboardFileEntry] = {
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
        owner.getEmail,
        accessLevel,
        owner.getEmail == user.getEmail,
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
        owner.getEmail,
        "Read",
        isOwner = false,
        new File(file),
        List()
      )
    })

    fileDao
      .fetchByOwnerUid(user.getUid)
      .forEach(fileRecord => {
        fileEntries += DashboardFileEntry(
          user.getEmail,
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
    val fileList: List[DashboardFileEntry] = getFileRecord(user).asScala.toList
    val filenames = ArrayBuffer[String]()
    val username = user.getEmail
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
      @Auth user: SessionUser
  ): Unit = {
    checkWriteAccess(fid, user.getUid)
    Files.deleteIfExists(Paths.get(fileDao.fetchOneByFid(fid).getPath))
    fileDao.deleteById(fid)
  }

  @POST
  @Consumes(Array(MediaType.MULTIPART_FORM_DATA))
  @Path("/validate")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def validateUserFile(
      @FormDataParam("name") fileName: String,
      @Auth user: SessionUser
  ): Response = {
    val validationResult = validateFileName(fileName, user.getUid)
    if (validationResult.getLeft)
      Response.ok().build()
    else {
      Response.status(Response.Status.BAD_REQUEST).entity(validationResult.getRight).build()
    }
  }

  @GET
  @Path("/download/{fid}")
  def downloadFile(
      @PathParam("fid") fid: UInteger,
      @Auth user: SessionUser
  ): Response = {
    checkReadAccess(fid, user.getUid)
    val filePath = Paths.get(fileDao.fetchOneByFid(fid).getPath)
    val fileStream = new StreamingOutput() {
      @Override
      def write(output: OutputStream): Unit = {
        Files.copy(filePath, output)
        output.flush()
      }
    }
    Response
      .ok(fileStream, MediaType.APPLICATION_OCTET_STREAM)
      .header(
        "content-disposition",
        String.format("attachment; filename=%s", fileDao.fetchOneByFid(fid).getName)
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
  def changeFileName(
      @PathParam("fid") fid: UInteger,
      @PathParam("name") name: String,
      @Auth user: SessionUser
  ): Unit = {
    checkWriteAccess(fid, user.getUid)
    val validationRes = this.validateFileName(name, user.getUid)
    if (!validationRes.getLeft) {
      throw new BadRequestException(validationRes.getRight)
    } else {
      val userFile = fileDao.fetchOneByFid(fid)
      userFile.setName(name)
      fileDao.update(userFile)
    }
  }

  private def validateFileName(fileName: String, userID: UInteger): Pair[Boolean, String] = {
    if (
      context.fetchExists(
        context
          .selectFrom(FILE)
          .where(FILE.OWNER_UID.equal(userID).and(FILE.NAME.equal(fileName)))
      )
    ) Pair.of(false, "file name already exists")
    else Pair.of(true, "filename validation success")
  }

  /**
    * This method updates the description of a given userFile
    * @param fid the id of the file
    * @param description the id of the file
    * @return the updated userFile
    */
  @PUT
  @Path("/description/{fid}/{description}")
  def changeFileDescription(
      @PathParam("fid") fid: UInteger,
      @PathParam("description") description: String,
      @Auth user: SessionUser
  ): Unit = {
    checkWriteAccess(fid, user.getUid)
    val userFile = fileDao.fetchOneByFid(fid)
    userFile.setDescription(description)
    fileDao.update(userFile)
  }
}
