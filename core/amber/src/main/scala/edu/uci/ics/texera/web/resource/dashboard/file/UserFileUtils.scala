package edu.uci.ics.texera.web.resource.dashboard.file

import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.FileDao
import org.apache.commons.io.IOUtils
import org.jooq.types.UInteger

import java.io._
import java.nio.file.{Files, Path, Paths}

object UserFileUtils {
  private val FILE_CONTAINER_PATH: Path = {
    Utils.amberHomePath.resolve("user-resources").resolve("files")
  }
  private val fileDao = new FileDao(SqlServer.createDSLContext.configuration)

  def storeFile(fileStream: InputStream, fileName: String, userID: UInteger): Unit = {
    createFileDirectoryIfNotExist(UserFileUtils.getFileDirectory(userID))
    checkFileDuplicate(UserFileUtils.getFilePath(userID, fileName))
    writeToFile(UserFileUtils.getFilePath(userID, fileName), fileStream)
  }

  @throws[FileIOException]
  private def checkFileDuplicate(filePath: Path): Unit = {
    if (Files.exists(filePath)) throw FileIOException("File already exists.")
  }

  def storeFileSafe(fileStream: InputStream, fileName: String, userID: UInteger): String = {
    createFileDirectoryIfNotExist(UserFileUtils.getFileDirectory(userID))
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
    getFileDirectory(userID).resolve(fileName)
  }

  def getFileDirectory(userID: UInteger): Path = FILE_CONTAINER_PATH.resolve(userID.toString)

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

  def getFilePathByInfo(ownerName: String, fileName: String, uid: UInteger): Option[Path] = {
    val fid = UserFileAccessResource.getFileId(ownerName, fileName)
    getFilePathByIds(uid, fid)
  }

  def getFilePathByIds(uid: UInteger, fid: UInteger): Option[Path] = {
    if (UserFileAccessResource.hasAccessTo(uid, fid)) {
      val path = fileDao.fetchByFid(fid).get(0).getPath
      Some(Paths.get(path))
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

  case class FileIOException(message: String) extends WorkflowRuntimeException(message)

}
