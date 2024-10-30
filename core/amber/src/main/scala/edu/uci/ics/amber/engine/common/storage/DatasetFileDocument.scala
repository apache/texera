package edu.uci.ics.amber.engine.common.storage

import edu.uci.ics.texera.web.resource.dashboard.user.dataset.service.GitVersionControlLocalFileStorage
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.utils.PathUtils
import org.jooq.types.UInteger

import java.io.{File, FileOutputStream, InputStream}
import java.net.{URI, URLDecoder}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters.IteratorHasAsScala

class DatasetFileDocument(uri: URI) extends VirtualDocument[Nothing] {
  // Utility function to parse and decode URI segments into individual components
  private def parseUri(uri: URI): (Int, String, Path) = {
    val segments = Paths.get(uri.getPath).iterator().asScala.map(_.toString).toArray
    if (segments.length < 3)
      throw new IllegalArgumentException("URI format is incorrect")

    val did = segments(0).toInt
    val datasetVersionHash = URLDecoder.decode(segments(1), StandardCharsets.UTF_8)
    val decodedRelativeSegments =
      segments.drop(2).map(part => URLDecoder.decode(part, StandardCharsets.UTF_8))
    val fileRelativePath = Paths.get(decodedRelativeSegments.head, decodedRelativeSegments.tail: _*)

    (did, datasetVersionHash, fileRelativePath)
  }

  // Extract components from URI using the utility function
  private val (did, datasetVersionHash, fileRelativePath) = parseUri(uri)

  private var tempFile: Option[File] = None

  override def getURI: URI = uri

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
