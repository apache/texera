package edu.uci.ics.amber.engine.common.storage

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.Dataset
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.DatasetVersion
import edu.uci.ics.texera.web.model.jooq.generated.tables.Dataset.DATASET
import edu.uci.ics.texera.web.model.jooq.generated.tables.User.USER
import edu.uci.ics.texera.web.model.jooq.generated.tables.DatasetVersion.DATASET_VERSION
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.service.GitVersionControlLocalFileStorage
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.utils.PathUtils
import org.jooq.types.UInteger

import java.io.{File, FileOutputStream, InputStream}
import java.nio.file.{Files, Path, Paths}
import javax.ws.rs.BadRequestException

class DatasetDirectoryDocument(fileFullPath: Path, shouldContainFile: Boolean = true) {

  private val context = SqlServer.createDSLContext()
  private val (dataset, datasetVersion, fileRelativePath) =
    resolvePath(fileFullPath, shouldContainFile)
  private var tempFile: Option[File] = None

  private def getDatasetByName(ownerEmail: String, datasetName: String): Dataset = {
    context
      .select(DATASET.fields: _*)
      .from(DATASET)
      .leftJoin(USER)
      .on(USER.UID.eq(DATASET.OWNER_UID))
      .where(USER.EMAIL.eq(ownerEmail))
      .and(DATASET.NAME.eq(datasetName))
      .fetchOneInto(classOf[Dataset])
  }

  private def getDatasetVersionByName(
      did: UInteger,
      versionName: String
  ): DatasetVersion = {
    context
      .selectFrom(DATASET_VERSION)
      .where(DATASET_VERSION.DID.eq(did))
      .and(DATASET_VERSION.NAME.eq(versionName))
      .fetchOneInto(classOf[DatasetVersion])
  }

  def resolvePath(
      path: java.nio.file.Path,
      shouldContainFile: Boolean
  ): (Dataset, DatasetVersion, Option[Path]) = {
    val pathSegments = (0 until path.getNameCount).map(path.getName(_).toString).toArray
    val expectedLength = if (shouldContainFile) 4 else 3

    if (pathSegments.length < expectedLength) {
      throw new BadRequestException(
        s"Invalid path format. Expected format: /ownerEmail/datasetName/versionName" +
          (if (shouldContainFile) "/fileRelativePath" else "")
      )
    }

    val ownerEmail = pathSegments(0)
    val datasetName = pathSegments(1)
    val versionName = pathSegments(2)

    val fileRelativePath =
      if (shouldContainFile) Some(Paths.get(pathSegments.drop(3).mkString("/"))) else None

    val dataset = getDatasetByName(ownerEmail, datasetName)
    val datasetVersion = getDatasetVersionByName(dataset.getDid, versionName)
    (dataset, datasetVersion, fileRelativePath)
  }

  def asFile(): File = {
    tempFile match {
      case Some(file) => file
      case None =>
        val tempFilePath = Files.createTempFile("versionedFile", ".tmp")
        val tempFileStream = new FileOutputStream(tempFilePath.toFile)
        val inputStream = asInputStream()

        val buffer = new Array[Byte](1024)

        // Create an iterator to repeatedly call inputStream.read, and direct buffered data to file
        Iterator
          .continually(inputStream.read(buffer))
          .takeWhile(_ != -1)
          .foreach(tempFileStream.write(buffer, 0, _))

        inputStream.close()
        tempFileStream.close()

        val file = tempFilePath.toFile
        tempFile = Some(file)
        file
    }
  }

  def asInputStream(): InputStream = {
    val datasetAbsolutePath = PathUtils.getDatasetPath(dataset.getDid)
    GitVersionControlLocalFileStorage
      .retrieveFileContentOfVersionAsInputStream(
        datasetAbsolutePath,
        datasetVersion.getVersionHash,
        datasetAbsolutePath.resolve(fileRelativePath.get)
      )
  }

  def asDirectory(): String = {
    PathUtils.getDatasetPath(dataset.getDid).toString
  }
}
