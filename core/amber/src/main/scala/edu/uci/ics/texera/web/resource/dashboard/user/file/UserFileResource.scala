package edu.uci.ics.texera.web.resource.dashboard.user.file

import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables._
import edu.uci.ics.texera.web.model.jooq.generated.enums.UserFileAccessPrivilege
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{FileDao, UserFileAccessDao}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{File, User, UserFileAccess}
import edu.uci.ics.texera.web.resource.dashboard.user.file.UserFileResource.{
  DashboardFile,
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
  final private lazy val userFileAccessDao = new UserFileAccessDao(context.configuration)

  def saveFile(uid: UInteger, fileName: String, stream: InputStream, des: String = ""): Unit = {
    val path = Utils.amberHomePath.resolve("user-resources").resolve("files").resolve(uid.toString)
    Files.createDirectories(path)
    val filepath = path.resolve(UUID.randomUUID.toString)
    Files.copy(stream, filepath)
    val file = new File(
      uid,
      null,
      UInteger.valueOf(filepath.toFile.length()),
      fileName,
      filepath.toString,
      des,
      null
    )
    fileDao.insert(file)
    userFileAccessDao.merge(
      new UserFileAccess(
        uid,
        file.getFid,
        UserFileAccessPrivilege.WRITE
      )
    )
  }

  case class DashboardFile(
      ownerEmail: String,
      accessLevel: String,
      file: File
  )
}
@Produces(Array(MediaType.APPLICATION_JSON))
@RolesAllowed(Array("REGULAR", "ADMIN"))
@Path("/user/file")
class UserFileResource {
  @POST
  @Consumes(Array(MediaType.MULTIPART_FORM_DATA))
  @Path("/upload")
  def uploadFile(
      @FormDataParam("file") stream: InputStream,
      @FormDataParam("name") fileName: String,
      @Auth user: SessionUser
  ): Response = {
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

  @GET
  @Path("/list")
  def getFileList(@Auth sessionUser: SessionUser): util.List[DashboardFile] = {
    getFileRecord(sessionUser.getUser)
  }

  private def getFileRecord(user: User): util.List[DashboardFile] = {
    val fids: mutable.ArrayBuffer[UInteger] = mutable.ArrayBuffer()
    val fileEntries: mutable.ArrayBuffer[DashboardFile] = mutable.ArrayBuffer()
    context
      .select()
      .from(USER_FILE_ACCESS)
      .join(FILE)
      .on(USER_FILE_ACCESS.FID.eq(FILE.FID))
      .join(USER)
      .on(FILE.OWNER_UID.eq(USER.UID))
      .where(USER_FILE_ACCESS.UID.eq(user.getUid))
      .fetch()
      .forEach(fileRecord => {
        fids += fileRecord.into(FILE).getFid
        fileEntries += DashboardFile(
          fileRecord.into(USER).getEmail,
          fileRecord.into(USER_FILE_ACCESS).getPrivilege.toString,
          fileRecord.into(FILE).into(classOf[File])
        )
      })

    context
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
      .forEach(fileRecord => {
        if (!fileEntries.exists(file => { file.file.getFid == fileRecord.into(FILE).getFid })) {
          fileEntries += DashboardFile(
            fileRecord.into(USER).getEmail,
            "READ",
            fileRecord.into(FILE).into(classOf[File])
          )
        }
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
    val fileList: List[DashboardFile] = getFileRecord(user).asScala.toList
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

  @DELETE
  @Path("/delete/{fid}")
  def deleteFile(
      @PathParam("fid") fid: UInteger,
      @Auth user: SessionUser
  ): Unit = {
    Files.deleteIfExists(Paths.get(fileDao.fetchOneByFid(fid).getPath))
    fileDao.deleteById(fid)
  }

  @GET
  @Path("/download/{fid}")
  def downloadFile(
      @PathParam("fid") fid: UInteger,
      @Auth user: SessionUser
  ): Response = {
    Response
      .ok(
        new StreamingOutput() {
          @Override
          def write(output: OutputStream): Unit = {
            Files.copy(Paths.get(fileDao.fetchOneByFid(fid).getPath), output)
            output.flush()
          }
        },
        MediaType.APPLICATION_OCTET_STREAM
      )
      .header(
        "content-disposition",
        String.format("attachment; filename=%s", fileDao.fetchOneByFid(fid).getName)
      )
      .build
  }

  @PUT
  @Path("/name/{fid}/{name}")
  def changeFileName(
      @PathParam("fid") fid: UInteger,
      @PathParam("name") name: String,
      @Auth user: SessionUser
  ): Unit = {
    val validationRes = this.validateFileName(name, user.getUid)
    if (!validationRes.getLeft) {
      throw new BadRequestException(validationRes.getRight)
    } else {
      val userFile = fileDao.fetchOneByFid(fid)
      userFile.setName(name)
      fileDao.update(userFile)
    }
  }

  @PUT
  @Path("/description/{fid}/{description}")
  def changeFileDescription(
      @PathParam("fid") fid: UInteger,
      @PathParam("description") description: String
  ): Unit = {
    val userFile = fileDao.fetchOneByFid(fid)
    userFile.setDescription(description)
    fileDao.update(userFile)
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

}
