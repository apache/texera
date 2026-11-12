/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.core.storage.model

import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.common.config.EnvironmentalVariable
import org.apache.texera.amber.core.storage.ResourceType
import org.apache.texera.amber.core.storage.model.LakeFSFileDocument.userJwtToken
import org.apache.texera.amber.core.storage.util.LakeFSStorageClient

import java.io.{File, IOException, InputStream}
import java.net._
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.Using

object LakeFSFileDocument {
  // Since requests need to be sent to the FileService in order to read the file, we store USER_JWT_TOKEN in the environment vars
  // This variable should be NON-EMPTY in the dynamic-computing-unit architecture, i.e. each user-created computing unit should store user's jwt token.
  // In the local development or other architectures, this token can be empty.
  lazy val userJwtToken: String =
    sys.env.getOrElse(EnvironmentalVariable.ENV_USER_JWT_TOKEN, "").trim

  private lazy val datasetPresignEndpoint: String =
    sys.env
      .getOrElse(
        EnvironmentalVariable.ENV_FILE_SERVICE_GET_DATASET_PRESIGNED_URL_ENDPOINT,
        "http://localhost:9092/api/dataset/presign-download"
      )
      .trim

  private lazy val modelPresignEndpoint: String =
    sys.env
      .getOrElse(
        EnvironmentalVariable.ENV_FILE_SERVICE_GET_MODEL_PRESIGNED_URL_ENDPOINT,
        "http://localhost:9092/api/model/presign-download"
      )
      .trim

  /**
    * The file-service presign-download endpoint serving this resource type. Each resource type
    * owns an endpoint because they enforce different access control
    */
  def presignEndpointOf(resourceType: ResourceType.Value): String =
    resourceType match {
      case ResourceType.Dataset => datasetPresignEndpoint
      case ResourceType.Model   => modelPresignEndpoint
    }
}

/**
  * A read-only document over a single file stored in a LakeFS repository, addressed by the URI
  * {scheme}:///{repositoryName}/{versionHash}/{fileRelativePath}.
  *
  * Every versioned-file resource (datasets, models, …) reads its files the same way — fetch the
  * bytes through a presigned URL, falling back to a direct LakeFS fetch
  *
  * @param uri          the resolved {scheme}:///{repositoryName}/{versionHash}/{file} URI
  * @param resourceType which resource this file belongs to, selecting the presign endpoint
  */
private[storage] class LakeFSFileDocument(uri: URI, val resourceType: ResourceType.Value)
    extends VirtualDocument[Nothing]
    with OnVersionedFileResource
    with LazyLogging {

  private val presignEndpoint: String = LakeFSFileDocument.presignEndpointOf(resourceType)
  // Utility function to parse and decode URI segments into individual components
  private def parseUri(uri: URI): (String, String, Path) = {
    val segments = Paths.get(uri.getPath).iterator().asScala.map(_.toString).toArray
    if (segments.length < 3)
      throw new IllegalArgumentException("URI format is incorrect")

    // parse uri to (repositoryName, versionHash, fileRelativePath)
    val repositoryName = segments(0)
    val versionHash = URLDecoder.decode(segments(1), StandardCharsets.UTF_8)
    val decodedRelativeSegments =
      segments.drop(2).map(part => URLDecoder.decode(part, StandardCharsets.UTF_8))
    val fileRelativePath = Paths.get(decodedRelativeSegments.head, decodedRelativeSegments.tail: _*)

    (repositoryName, versionHash, fileRelativePath)
  }

  // Extract components from URI using the utility function
  protected val (repositoryName, versionHash, fileRelativePath) = parseUri(uri)

  protected var tempFile: Option[File] = None

  override def getURI: URI = uri

  override def asInputStream(): InputStream = {

    def fallbackToLakeFS(exception: Throwable): InputStream = {
      logger.warn(s"${exception.getMessage}. Falling back to LakeFS direct file fetch.", exception)
      val file = LakeFSStorageClient.getFileFromRepo(
        getRepositoryName(),
        getVersionHash(),
        getFileRelativePath()
      )
      // The SDK saves the object to a temp file; delete it once the caller closes the stream.
      Files.newInputStream(file.toPath, StandardOpenOption.DELETE_ON_CLOSE)
    }

    if (userJwtToken.isEmpty) {
      try {
        val presignUrl = LakeFSStorageClient.getFilePresignedUrl(
          getRepositoryName(),
          getVersionHash(),
          getFileRelativePath()
        )
        openPresigned(presignUrl)
      } catch {
        case e: Exception =>
          fallbackToLakeFS(e)
      }
    } else {
      val presignRequestUrl =
        s"$presignEndpoint?repositoryName=${getRepositoryName()}&commitHash=${getVersionHash()}&filePath=${URLEncoder
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

        openPresigned(presignedUrl)
      } catch {
        case e: Exception =>
          fallbackToLakeFS(e)
      } finally {
        connection.disconnect()
      }
    }
  }

  private def openPresigned(presignedUrl: String): InputStream = {
    val connection = new URL(presignedUrl).openConnection()
    val stream = connection.getInputStream
    new LengthCheckedInputStream(stream, connection.getContentLengthLong, getFileRelativePath())
  }

  override def asFile(): File = {
    tempFile match {
      case Some(file) => file
      case None =>
        val tempFilePath = Files.createTempFile("versionedFile", ".tmp")
        try Using.resources(asInputStream(), Files.newOutputStream(tempFilePath))(_.transferTo(_))
        catch {
          // A copy that did not finish is not the file; leaving it would also leak it on disk.
          case e: Throwable =>
            Files.deleteIfExists(tempFilePath)
            throw e
        }

        val file = tempFilePath.toFile
        tempFile = Some(file)
        file
    }
  }

  override def clear(): Unit = {
    // remove the temporary file, if one was materialized
    tempFile match {
      case Some(file) => Files.delete(file.toPath)
      case None       => // Do nothing
    }
    tempFile = None
  }

  override def getRepositoryName(): String = repositoryName

  override def getVersionHash(): String = versionHash

  override def getFileRelativePath(): String = fileRelativePath.toString
}

/**
  * Fails a read that ends before the length the server declared. HttpURLConnection reports a
  * connection closed mid-body as an ordinary end of stream, so without this a proxy that times out
  * a large presigned download hands the caller a silently truncated file, which a format reader
  * then rejects as corrupt.
  *
  * Extends InputStream rather than FilterInputStream so that skip reads (and counts) through
  * `read`, and mark/reset stay unsupported instead of rewinding the wrapped stream behind the count.
  */
private class LengthCheckedInputStream(in: InputStream, expectedLength: Long, what: String)
    extends InputStream {
  private var received = 0L

  override def read(): Int = {
    val byte = in.read()
    if (byte == -1) checkComplete() else received += 1
    byte
  }

  override def read(buffer: Array[Byte], offset: Int, length: Int): Int = {
    val count = in.read(buffer, offset, length)
    if (count == -1) checkComplete() else received += count
    count
  }

  override def available(): Int = in.available()

  override def close(): Unit = in.close()

  // An undeclared length is -1, which `received` never falls below, so such streams pass through.
  private def checkComplete(): Unit =
    if (received < expectedLength)
      throw new IOException(
        s"Download of $what ended after $received of $expectedLength bytes: the connection was " +
          "closed before the whole file arrived."
      )
}
