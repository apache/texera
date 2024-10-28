package edu.uci.ics.amber.engine.common.storage

import edu.uci.ics.texera.web.resource.dashboard.user.dataset.service.GitVersionControlLocalFileStorage
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.utils.PathUtils
import org.jooq.types.UInteger

import java.io.{File, FileOutputStream, InputStream}
import java.net.{URI, URLDecoder}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

class DatasetFileDocument(uri: URI) extends VirtualDocument[Nothing] {
  // Extract path components and decode them
  private val pathParts = uri.getPath
    .stripPrefix("/")
    .split("/")
    .map(part => URLDecoder.decode(part, StandardCharsets.UTF_8))

  private val did = pathParts(0).toInt
  private val datasetVersionHash = pathParts(1)
  private val fileRelativePath = Paths.get(pathParts.drop(2).head, pathParts.drop(2).tail: _*)

  private var tempFile: Option[File] = None

  override def getURI: URI =
    throw new UnsupportedOperationException(
      "The URI cannot be acquired because the file is not physically located"
    )

  override def asInputStream(): InputStream = {
    val datasetAbsolutePath = PathUtils.getDatasetPath(UInteger.valueOf(did))
    GitVersionControlLocalFileStorage
      .retrieveFileContentOfVersionAsInputStream(
        datasetAbsolutePath,
        datasetVersionHash,
        datasetAbsolutePath.resolve(fileRelativePath)
      )
  }

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
    // first remove the temporary file
    tempFile match {
      case Some(file) => Files.delete(file.toPath)
      case None       => // Do nothing
    }
    // then remove the dataset file
    GitVersionControlLocalFileStorage.removeFileFromRepo(
      PathUtils.getDatasetPath(UInteger.valueOf(did)),
      PathUtils.getDatasetPath(UInteger.valueOf(did)).resolve(fileRelativePath)
    )
  }
}
