package edu.uci.ics.amber.core.storage.model

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.storage.EnvironmentalVariable
import edu.uci.ics.amber.core.storage.model.DatasetFileDocument.{
  fileServiceGetPresignURLEndpoint,
  userJwtToken
}
import edu.uci.ics.amber.core.storage.util.LakeFSStorageClient
import edu.uci.ics.amber.core.storage.util.dataset.GitVersionControlLocalFileStorage
import edu.uci.ics.amber.util.PathUtils

import java.io.{File, FileOutputStream, InputStream}
import java.net.{HttpURLConnection, URI, URL, URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters.IteratorHasAsScala

object DatasetFileDocument {
  // Since requests need to be sent to the FileService in order to read the file, we store USER_JWT_TOKEN in the environment vars
  // This variable should be NON-EMPTY in the dynamic-computing-unit architecture, i.e. each user-created computing unit should store user's jwt token.
  // In the local development or other architectures, this token can be empty.
  lazy val userJwtToken: String =
    sys.env.getOrElse(EnvironmentalVariable.ENV_USER_JWT_TOKEN, "").trim

  // The endpoint of getting presigned url from the file service, also stored in the environment vars.
  lazy val fileServiceGetPresignURLEndpoint: String =
    sys.env
      .getOrElse(
        EnvironmentalVariable.ENV_FILE_SERVICE_GET_PRESIGNED_URL_ENDPOINT,
        "http://localhost:9092/api/dataset/presign-download"
      )
      .trim
}

private[storage] class DatasetFileDocument(uri: URI)
    extends VirtualDocument[Nothing]
    with OnDataset
    with LazyLogging {
  // Utility function to parse and decode URI segments into individual components
  private def parseUri(uri: URI): (String, String, Path) = {
    val segments = Paths.get(uri.getPath).iterator().asScala.map(_.toString).toArray
    if (segments.length < 3)
      throw new IllegalArgumentException("URI format is incorrect")

    // TODO: consider whether use dataset name or did
    val datasetName = segments(0)
    val datasetVersionHash = URLDecoder.decode(segments(1), StandardCharsets.UTF_8)
    val decodedRelativeSegments =
      segments.drop(2).map(part => URLDecoder.decode(part, StandardCharsets.UTF_8))
    val fileRelativePath = Paths.get(decodedRelativeSegments.head, decodedRelativeSegments.tail: _*)

    (datasetName, datasetVersionHash, fileRelativePath)
  }

  // Extract components from URI using the utility function
  private val (datasetName, datasetVersionHash, fileRelativePath) = parseUri(uri)

  private var tempFile: Option[File] = None

  override def getURI: URI = uri

  override def asInputStream(): InputStream = {

    def fallbackToLakeFS(exception: Throwable): InputStream = {
      logger.warn(s"${exception.getMessage}. Falling back to LakeFS direct file fetch.", exception)
      val file = LakeFSStorageClient.getFileFromRepo(
        getDatasetName(),
        getVersionHash(),
        getFileRelativePath()
      )
      Files.newInputStream(file.toPath)
    }

    if (userJwtToken.isEmpty) {
      try {
        val presignUrl = LakeFSStorageClient.getFilePresignedUrl(
          getDatasetName(),
          getVersionHash(),
          getFileRelativePath()
        )
        new URL(presignUrl).openStream()
      } catch {
        case e: Exception =>
          fallbackToLakeFS(e)
      }
    } else {
      val presignRequestUrl =
        s"$fileServiceGetPresignURLEndpoint?datasetName=${getDatasetName()}&commitHash=${getVersionHash()}&filePath=${URLEncoder
          .encode(getFileRelativePath(), StandardCharsets.UTF_8.name())}"

      val connection = new URL(presignRequestUrl).openConnection().asInstanceOf[HttpURLConnection]
      connection.setRequestMethod("GET")
      connection.setRequestProperty("Authorization", s"Bearer $userJwtToken")

      try {
        if (connection.getResponseCode != HttpURLConnection.HTTP_OK) {
          throw new RuntimeException(
            s"Failed to retrieve presigned URL: HTTP ${connection.getResponseCode}"
          )
        }

        // Read response body as a string
        val responseBody =
          new String(connection.getInputStream.readAllBytes(), StandardCharsets.UTF_8)

        // Extract presigned URL from JSON response
        val presignedUrl = responseBody
          .split("\"presignedUrl\"\\s*:\\s*\"")(1)
          .split("\"")(0)

        new URL(presignedUrl).openStream()
      } catch {
        case e: Exception =>
          fallbackToLakeFS(e)
      } finally {
        connection.disconnect()
      }
    }
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

  override def clear(): Unit = {
    // first remove the temporary file
    tempFile match {
      case Some(file) => Files.delete(file.toPath)
      case None       => // Do nothing
    }
    // then remove the dataset file
    GitVersionControlLocalFileStorage.removeFileFromRepo(
      PathUtils.getDatasetPath(0),
      PathUtils.getDatasetPath(0).resolve(fileRelativePath)
    )
  }

  override def getVersionHash(): String = datasetVersionHash

  override def getDatasetName(): String = datasetName

  override def getFileRelativePath(): String = fileRelativePath.toString
}
