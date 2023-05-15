package edu.uci.ics.texera.web.resource.dashboard.user.file

import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{FileDao, FileOfWorkflowDao}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.FileOfWorkflow
import org.apache.commons.io.IOUtils
import org.jooq.types.UInteger
import java.io._
import java.nio.file.{Files, Path, Paths}

object UserFileUtils {
  private lazy val fileDao = new FileDao(SqlServer.createDSLContext.configuration)
  private lazy val file_of_workflowDao = new FileOfWorkflowDao(
    SqlServer.createDSLContext.configuration
  )
  private val FILE_CONTAINER_PATH: Path = {
    Utils.amberHomePath.resolve("user-resources").resolve("files")
  }

  def storeFileSafe(fileStream: InputStream, fileName: String, userID: UInteger): String = {
    createFileDirectoryIfNotExist(UserFileUtils.FILE_CONTAINER_PATH.resolve(userID.toString))
    var fileNameToStore = fileName
    val fileNameComponents = fileName.split("\\.")
    val fileNameRaw = fileNameComponents.apply(0)
    val fileExtension = if (fileNameComponents.length == 2) fileNameComponents.apply(1) else ""
    var copyId = 0
    while (
      Files.exists(
        UserFileUtils.getFilePath(
          userID,
          fileNameToStore
        )
      )
    ) {
      copyId += 1
      fileNameToStore = s"$fileNameRaw-$copyId.$fileExtension"
    }
    writeToFile(UserFileUtils.getFilePath(userID, fileNameToStore), fileStream)
    fileNameToStore
  }

  def getFilePath(userID: UInteger, fileName: String): Path = {
    FILE_CONTAINER_PATH.resolve(userID.toString).resolve(fileName)
  }

  @throws[FileIOException]
  private def createFileDirectoryIfNotExist(directoryPath: Path): Unit = {
    if (!Files.exists(directoryPath))
      try Files.createDirectories(directoryPath)
      catch {
        case e: IOException =>
          throw FileIOException(e.getMessage)
      }
  }

  @throws[FileIOException]
  private def writeToFile(filePath: Path, fileStream: InputStream): Unit = {
    val charset: String = null
    val outputStream = new FileWriter(filePath.toString)
    IOUtils.copy(fileStream, outputStream, charset)
    IOUtils.closeQuietly(fileStream)
    IOUtils.closeQuietly(outputStream)
  }

  def getFilePathByInfo(
      ownerName: String,
      fileName: String,
      uid: UInteger,
      wid: UInteger
  ): Option[Path] = {
    val fid = UserFileAccessResource.getFileId(ownerName, fileName)
    if (
      UserFileAccessResource
        .hasAccessTo(uid, fid) || UserFileAccessResource.workflowHasFile(wid, fid)
    ) {
      file_of_workflowDao.merge(new FileOfWorkflow(fid, wid))
      Some(Paths.get(fileDao.fetchOneByFid(fid).getPath))
    } else {
      None
    }
  }

  @throws[FileIOException]
  def deleteFile(filePath: Path): Unit = {
    try Files.deleteIfExists(filePath)
    catch {
      case e: Exception =>
        throw FileIOException(
          "Error occur when deleting the file " + filePath.toString + ": " + e.getMessage
        )
    }
  }

  private case class FileIOException(message: String) extends WorkflowRuntimeException(message)
}
