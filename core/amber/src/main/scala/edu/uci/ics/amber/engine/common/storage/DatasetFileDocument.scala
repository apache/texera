package edu.uci.ics.amber.engine.common.storage

import edu.uci.ics.texera.web.resource.dashboard.user.dataset.DatasetResource
import org.jooq.types.UInteger

import java.io.{File, InputStream, FileOutputStream}
import java.net.URI
import java.nio.file.{Files, Path}

class DatasetFileDocument(uid: UInteger, fileFullPath: Path) extends VirtualDocument[Nothing] {

  private val (_, dataset, datasetVersion, fileRelativePath) =
    DatasetResource.resolveFilePath(fileFullPath)

  private var tempFile: Option[File] = None

  override def getURI: URI =
    throw new UnsupportedOperationException(
      "The URI cannot be acquired because the file is not physically located"
    )

  override def asInputStream(): InputStream =
    DatasetResource.getDatasetFile(dataset.getDid, datasetVersion.getDvid, uid, fileRelativePath)

  override def asFile(): File = {
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

  override def remove(): Unit = {
    tempFile match {
      case Some(file) => Files.delete(file.toPath)
      case None       => // Do nothing
    }
  }
}
