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

package edu.uci.ics.amber.core.storage.util

import edu.uci.ics.amber.config.StorageConfig
import io.lakefs.clients.sdk._
import io.lakefs.clients.sdk.model._

import java.io.{File, FileOutputStream, InputStream}
import java.nio.file.Files
import scala.jdk.CollectionConverters._
import io.lakefs.clients.sdk.model.ResetCreation.TypeEnum

/**
  * LakeFSFileStorage provides high-level file storage operations using LakeFS,
  * similar to Git operations for version control and file management.
  */
object LakeFSStorageClient {

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
  private lazy val stagingApi: StagingApi = new StagingApi(apiClient)
  private lazy val experimentalApi: ExperimentalApi = new ExperimentalApi(apiClient)
  private lazy val healthCheckApi: HealthCheckApi = new HealthCheckApi(apiClient)

  private val storageNamespaceURI: String =
    s"${StorageConfig.lakefsBlockStorageType}://${StorageConfig.lakefsBucketName}"

  private val branchName: String = "main"

  def healthCheck(): Unit = {
    try {
      this.healthCheckApi.healthCheck().execute()
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Failed to connect to lake fs server: ${e.getMessage}")
    }
  }

  /**
    * Initializes a new repository in LakeFS.
    *
    * @param repoName         Name of the repository.
    * @param defaultBranch    Default branch name, usually "main".
    */
  def initRepo(
      repoName: String
  ): Repository = {
    val repoNamePattern = "^[a-z0-9][a-z0-9-]{2,62}$".r

    // Validate repoName
    if (!repoNamePattern.matches(repoName)) {
      throw new IllegalArgumentException(
        s"Invalid dataset name: '$repoName'. " +
          "Dataset names must be 3-63 characters long, " +
          "contain only lowercase letters, numbers, and hyphens, " +
          "and cannot start or end with a hyphen."
      )
    }
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
    * @param branch      Branch name.
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
    * Removes a file from the repository (similar to Git rm).
    *
    * @param repoName Repository name.
    * @param branch   Branch name.
    * @param filePath Path in the repository to delete.
    */
  def removeFileFromRepo(repoName: String, branch: String, filePath: String): Unit = {
    objectsApi.deleteObject(repoName, branch, filePath).execute()
  }

  /**
    * Executes operations and creates a commit (similar to a transactional commit).
    *
    * @param repoName      Repository name.
    * @param branch        Branch name.
    * @param commitMessage Commit message.
    * @param operations    File operations to perform before committing.
    */
  def withCreateVersion(repoName: String, commitMessage: String)(
      operations: => Unit
  ): Commit = {
    operations
    val commit = new CommitCreation()
      .message(commitMessage)

    commitsApi.commit(repoName, branchName, commit).execute()
  }

  /**
    * Retrieves file content from a specific commit and path.
    *
    * @param repoName     Repository name.
    * @param commitHash   Commit hash of the version.
    * @param filePath     Path to the file in the repository.
    */
  def retrieveFileContent(repoName: String, commitHash: String, filePath: String): File = {
    objectsApi.getObject(repoName, commitHash, filePath).execute()
  }

  /**
    * Retrieves file content from a specific commit and path.
    *
    * @param repoName     Repository name.
    * @param commitHash   Commit hash of the version.
    * @param filePath     Path to the file in the repository.
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

  def retrieveVersionsOfRepository(repoName: String): List[Commit] = {
    refsApi
      .logCommits(repoName, branchName)
      .execute()
      .getResults
      .asScala
      .toList
      .sortBy(_.getCreationDate)(Ordering[java.lang.Long].reverse) // Sort in descending order
  }

  def retrieveObjectsOfVersion(repoName: String, commitHash: String): List[ObjectStats] = {
    objectsApi.listObjects(repoName, commitHash).execute().getResults.asScala.toList
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
    branchesApi
      .diffBranch(repoName, branchName)
      .execute()
      .getResults
      .asScala
      .toList
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
}
