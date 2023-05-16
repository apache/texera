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
import java.util.UUID

object UserFileUtils {
  private lazy val fileDao = new FileDao(SqlServer.createDSLContext.configuration)
  private lazy val file_of_workflowDao = new FileOfWorkflowDao(
    SqlServer.createDSLContext.configuration
  )

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
