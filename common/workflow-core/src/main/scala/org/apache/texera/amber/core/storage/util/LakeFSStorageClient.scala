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

package org.apache.texera.amber.core.storage.util

import com.typesafe.scalalogging.LazyLogging
import io.lakefs.clients.sdk._
import io.lakefs.clients.sdk.model.ResetCreation.TypeEnum
import io.lakefs.clients.sdk.model._
import org.apache.texera.common.config.StorageConfig
import org.apache.texera.common.util.RetryUtil

import java.io.{File, FileOutputStream, InputStream}
import java.net.{HttpURLConnection, URI, URL}
import java.nio.file.Files
import scala.jdk.CollectionConverters._

/**
  * LakeFSFileStorage provides high-level file storage operations using LakeFS,
  * similar to Git operations for version control and file management.
  */
object LakeFSStorageClient extends LazyLogging {

  // Maximum number of results per LakeFS API request (pagination page size)
  private val PageSize = 1000

  // Health-check retry settings: retry with exponential backoff before giving up.
  // 5 attempts starting at 200ms (200, 400, 800, 1600ms) caps total wait at ~3s.
  private val HealthCheckMaxAttempts = 5
  private val HealthCheckInitialDelayMillis = 200L

  private lazy val apiClient: ApiClient = {
    val client = new ApiClient()
    client.setApiKey(StorageConfig.lakefsPassword)
    client.setUsername(StorageConfig.lakefsUsername)
    client.setPassword(StorageConfig.lakefsPassword)
    client.setServers(
      List(
        new ServerConfiguration(
          StorageConfig.lakefsEndpoint,
          "LakeFS API server endpoint",
          new java.util.HashMap[String, ServerVariable]()
        )
      ).asJava
    )
    client
  }
  private lazy val repoApi: RepositoriesApi = new RepositoriesApi(apiClient)
  private lazy val objectsApi: ObjectsApi = new ObjectsApi(apiClient)
  private lazy val branchesApi: BranchesApi = new BranchesApi(apiClient)
  private lazy val commitsApi: CommitsApi = new CommitsApi(apiClient)
  private lazy val refsApi: RefsApi = new RefsApi(apiClient)
  private lazy val experimentalApi: ExperimentalApi = new ExperimentalApi(apiClient)
  private lazy val healthCheckApi: HealthCheckApi = new HealthCheckApi(apiClient)

  private val storageNamespaceURI: String =
    s"${StorageConfig.lakefsBlockStorageType}://${StorageConfig.lakefsBucketName}"

  private val branchName: String = "main"

  def healthCheck(): Unit = {
    RetryUtil.withBackoff(
      description = "connect to lake fs server",
      maxAttempts = HealthCheckMaxAttempts,
      initialDelayMillis = HealthCheckInitialDelayMillis,
      onRetry = attempt => logger.warn(attempt.message)
    ) {
      this.healthCheckApi.healthCheck().execute()
    }
  }

  /**
    * Initializes a new repository in LakeFS.
    *
    * @param repoName         Name of the repository.
    */
  def initRepo(
      repoName: String
  ): Repository = {
    // validate repoName, see https://docs.lakefs.io/latest/understand/model/#repository
    val repoNamePattern = "^[a-z0-9][a-z0-9-]{2,62}$".r
    if (!repoNamePattern.matches(repoName)) {
      throw new IllegalArgumentException(
        s"Invalid repository name: '$repoName'. " +
          "Repository names must be 3-63 characters long, " +
          "contain only lowercase letters, numbers, and hyphens, " +
          "and cannot start with a hyphen."
      )
    }

    // create repository
    val storageNamespace = s"$storageNamespaceURI/$repoName"
    val repo = new RepositoryCreation()
      .name(repoName)
      .storageNamespace(storageNamespace)
      .defaultBranch(branchName)
      .sampleData(false)

    repoApi.createRepository(repo).execute()
  }

  /**
    * Writes a file to the repository (similar to Git add).
    * Converts the InputStream to a temporary file for upload.
    *
    * @param repoName    Repository name.
    * @param filePath    Path in the repository.
    * @param inputStream File content stream.
    */
  def writeFileToRepo(
      repoName: String,
      filePath: String,
      inputStream: InputStream
  ): ObjectStats = {
    val tempFilePath = Files.createTempFile("lakefs-upload-", ".tmp")
    val tempFileStream = new FileOutputStream(tempFilePath.toFile)
    val buffer = new Array[Byte](8192)

    // Create an iterator to repeatedly call inputStream.read, and direct buffered data to file
    Iterator
      .continually(inputStream.read(buffer))
      .takeWhile(_ != -1)
      .foreach(tempFileStream.write(buffer, 0, _))

    inputStream.close()
    tempFileStream.close()

    // Upload the temporary file to LakeFS
    objectsApi.uploadObject(repoName, branchName, filePath).content(tempFilePath.toFile).execute()
  }

  /**
    * Retrieves a file from a specific repository and commit.
    *
    * @param repoName     Repository name.
    * @param versionHash  Commit hash of the version.
    * @param filePath     Path to the file in the repository.
    * @return             The file retrieved from LakeFS.
    */
  def getFileFromRepo(repoName: String, versionHash: String, filePath: String): File = {
    objectsApi.getObject(repoName, versionHash, filePath).execute()
  }

  /**
    * Generates a presigned URL for downloading a file directly from the underlying object store,
    * bypassing the LakeFS server.
    *
    * @param repoName     Repository name.
    * @param commitHash   Commit hash of the version.
    * @param filePath     Path to the file in the repository.
    * @return             A time-limited presigned URL pointing at the object's physical address.
    */
  def getFilePresignedUrl(repoName: String, commitHash: String, filePath: String): String = {
    objectsApi.statObject(repoName, commitHash, filePath).presign(true).execute().getPhysicalAddress
  }

  /**
    * Initiates a presigned multipart upload for a file in LakeFS.
    *
    * @param repoName     Repository name.
    * @param filePath     File path within the repository.
    * @param numberOfParts Number of parts to upload.
    * @return              Multipart upload information.
    */
  def initiatePresignedMultipartUploads(
      repoName: String,
      filePath: String,
      numberOfParts: Int
  ): PresignMultipartUpload = {
    experimentalApi
      .createPresignMultipartUpload(repoName, branchName, filePath)
      .parts(numberOfParts)
      .execute()

  }

  /**
    * Uploads one part of a presigned multipart upload: PUTs exactly `len` bytes from `buf` to
    * the presigned URL and returns the part's ETag.
    *
    * This is the middle step of the presigned lifecycle — [[initiatePresignedMultipartUploads]]
    * hands out the URLs, this uploads each part, and the returned ETag is the second half of the
    * `(partNumber, eTag)` pairs [[completePresignedMultipartUploads]] consumes.
    *
    * @param buf     Buffer holding the part's bytes.
    * @param len     Number of bytes from `buf` to send.
    * @param url     Presigned URL for this part.
    * @param partNum Part number, used only for the error message.
    * @return        The ETag returned by the object store, quotes stripped.
    */
  def put(buf: Array[Byte], len: Int, url: String, partNum: Int): String = {
    val conn = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    conn.setDoOutput(true)
    conn.setRequestMethod("PUT")
    conn.setFixedLengthStreamingMode(len)
    val out = conn.getOutputStream
    out.write(buf, 0, len)
    out.close()

    val code = conn.getResponseCode
    if (code != HttpURLConnection.HTTP_OK && code != HttpURLConnection.HTTP_CREATED)
      throw new RuntimeException(s"Part $partNum upload failed (HTTP $code)")

    val etag = conn.getHeaderField("ETag").replace("\"", "")
    conn.disconnect()
    etag
  }

  /**
    * Completes a previously initiated multipart upload.
    *
    * @param repoName        Repository name.
    * @param filePath        File path within the repository.
    * @param uploadId        Multipart upload ID.
    * @param partsList       List of (part number, ETag) pairs.
    * @param physicalAddress Physical location of the file in storage.
    * @return                Object metadata after completion.
    */
  def completePresignedMultipartUploads(
      repoName: String,
      filePath: String,
      uploadId: String,
      partsList: List[(Int, String)],
      physicalAddress: String
  ): ObjectStats = {
    val completePresignMultipartUpload: CompletePresignMultipartUpload =
      new CompletePresignMultipartUpload()

    // Sort parts by part number in ascending order
    val sortedParts = partsList.sortBy(_._1)

    completePresignMultipartUpload.setParts(
      sortedParts
        .map(part => {
          val newUploadPart = new UploadPart
          newUploadPart.setPartNumber(part._1)
          newUploadPart.setEtag(part._2)
          newUploadPart
        })
        .asJava
    )

    completePresignMultipartUpload.setPhysicalAddress(physicalAddress)

    experimentalApi
      .completePresignMultipartUpload(repoName, branchName, uploadId, filePath)
      .completePresignMultipartUpload(completePresignMultipartUpload)
      .execute()
  }

  /**
    * Aborts a multipart upload operation for a given file.
    *
    * @param repoName        Repository name.
    * @param filePath        File path within the repository.
    * @param uploadId        Multipart upload ID.
    * @param physicalAddress Physical address of the file.
    */
  def abortPresignedMultipartUploads(
      repoName: String,
      filePath: String,
      uploadId: String,
      physicalAddress: String
  ): Unit = {
    val abortPresignMultipartUpload: AbortPresignMultipartUpload = new AbortPresignMultipartUpload
    abortPresignMultipartUpload.setPhysicalAddress(physicalAddress)

    experimentalApi
      .abortPresignMultipartUpload(repoName, branchName, uploadId, filePath)
      .abortPresignMultipartUpload(abortPresignMultipartUpload)
      .execute()
  }

  /**
    * Deletes an entire repository.
    *
    * @param repoName Name of the repository to delete.
    */
  def deleteRepo(repoName: String): Unit = {
    repoApi.deleteRepository(repoName).execute()
  }

  private def retrieveVersionsOfRepository(repoName: String): List[Commit] = {
    refsApi
      .logCommits(repoName, branchName)
      .execute()
      .getResults
      .asScala
      .toList
      .sortBy(_.getCreationDate)(Ordering[java.lang.Long].reverse) // Sort in descending order
  }

  /**
    * Fetches all pages from a paginated LakeFS API call.
    *
    * @param fetch A function that takes a pagination cursor and returns (results, pagination).
    * @return All results across all pages.
    */
  private def fetchAllPages[T](
      fetch: String => (java.util.List[T], Pagination)
  ): List[T] = {
    val allResults = scala.collection.mutable.ListBuffer[T]()
    var hasMore = true
    var after = "" // Pagination cursor returned by LakeFS

    while (hasMore) {
      val (results, pagination) = fetch(after)
      allResults ++= results.asScala
      hasMore = pagination.getHasMore
      if (hasMore) after = pagination.getNextOffset
    }

    allResults.toList
  }

  def retrieveObjectsOfVersion(repoName: String, commitHash: String): List[ObjectStats] = {
    fetchAllPages[ObjectStats] { after =>
      val request = objectsApi.listObjects(repoName, commitHash).amount(PageSize)
      if (after.nonEmpty) request.after(after)
      val response = request.execute()
      (response.getResults, response.getPagination)
    }
  }

  def retrieveRepositorySize(repoName: String, commitHash: String = ""): Long = {
    val versionHash: String =
      if (commitHash.isEmpty) {
        val versionList = retrieveVersionsOfRepository(repoName)
        if (versionList.isEmpty) {
          ""
        } else {
          versionList.head.getId
        }
      } else {
        commitHash
      }

    if (versionHash.isEmpty) {
      0
    } else {
      LakeFSStorageClient
        .retrieveObjectsOfVersion(repoName, versionHash)
        .map(_.getSizeBytes.longValue())
        .sum
    }
  }

  /**
    * Retrieves a list of uncommitted (staged) objects in a repository branch.
    *
    * @param repoName Repository name.
    * @return List of uncommitted object stats.
    */
  def retrieveUncommittedObjects(repoName: String): List[Diff] = {
    fetchAllPages[Diff] { after =>
      val request = branchesApi.diffBranch(repoName, branchName).amount(PageSize)
      if (after.nonEmpty) request.after(after)
      val response = request.execute()
      (response.getResults, response.getPagination)
    }
  }

  def createCommit(repoName: String, branch: String, commitMessage: String): Commit = {
    val commit = new CommitCreation()
      .message(commitMessage)
    commitsApi.commit(repoName, branch, commit).execute()
  }

  def deleteObject(repoName: String, filePath: String): Unit = {
    objectsApi.deleteObject(repoName, branchName, filePath).execute()
  }

  def resetObjectUploadOrDeletion(repoName: String, filePath: String): Unit = {
    val resetCreation: ResetCreation = new ResetCreation
    resetCreation.setType(TypeEnum.OBJECT)
    resetCreation.setPath(filePath)

    branchesApi.resetBranch(repoName, branchName, resetCreation).execute()
  }

  /**
    * Parse a physical address URI of the form "<scheme>://<bucket>/<key...>" into (bucket, key).
    *
    * Expected examples:
    *   - "s3://my-bucket/path/to/file.csv"
    *   - "gs://my-bucket/some/prefix/data.json"
    *
    * @param address URI string in the form "<scheme>://<bucket>/<key...>"
    * @return (bucket, key) where key does not start with "/"
    * @throws java.lang.IllegalArgumentException
    *   if the address is empty, not a valid URI, missing bucket/host, or missing key/path
    */
  def parsePhysicalAddress(address: String): (String, String) = {
    val raw = Option(address).getOrElse("").trim
    if (raw.isEmpty)
      throw new IllegalArgumentException("Address is empty (expected '<scheme>://<bucket>/<key>')")

    val uri =
      try new URI(raw)
      catch {
        case e: Exception =>
          throw new IllegalArgumentException(
            s"Invalid address URI: '$raw' (expected '<scheme>://<bucket>/<key>')",
            e
          )
      }

    val bucket = Option(uri.getHost).getOrElse("").trim
    if (bucket.isEmpty)
      throw new IllegalArgumentException(
        s"Invalid address: missing host/bucket in '$raw' (expected '<scheme>://<bucket>/<key>')"
      )

    val key = Option(uri.getPath).getOrElse("").stripPrefix("/").trim
    if (key.isEmpty)
      throw new IllegalArgumentException(
        s"Invalid address: missing key/path in '$raw' (expected '<scheme>://<bucket>/<key>')"
      )

    (bucket, key)
  }

  /**
    * Get file size.
    *
    * @param repoName     Repository name.
    * @param commitHash   Commit hash of the version.
    * @param filePath     Path to the file in the repository.
    * @return File size in bytes
    */
  def getFileSize(
      repoName: String,
      commitHash: String,
      filePath: String
  ): Long = {
    objectsApi
      .statObject(repoName, commitHash, filePath)
      .execute()
      .getSizeBytes
      .longValue()
  }

  /**
    * Gets the last-modified time of a staged (uncommitted) object on the main branch.
    *
    * @param repoName Repository name.
    * @param filePath Path to the staged object in the repository.
    * @return Last-modified time as Unix epoch seconds.
    */
  def getStagedObjectMtime(repoName: String, filePath: String): Long = {
    objectsApi
      .statObject(repoName, branchName, filePath)
      .execute()
      .getMtime
      .longValue()
  }
}
