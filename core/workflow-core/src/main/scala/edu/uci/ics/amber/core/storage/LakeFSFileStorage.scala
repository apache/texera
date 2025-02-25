package edu.uci.ics.amber.core.storage

import io.lakefs.clients.sdk._
import io.lakefs.clients.sdk.model._

import java.io.{File, FileOutputStream, InputStream}
import java.nio.file.Files
import scala.jdk.CollectionConverters._
import edu.uci.ics.amber.core.storage.StorageConfig

/**
  * LakeFSFileStorage provides high-level file storage operations using LakeFS,
  * similar to Git operations for version control and file management.
  */
object LakeFSFileStorage {

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

  private val storageNamespaceURI: String = s"${StorageConfig.lakefsBlockStorageType}://${StorageConfig.lakefsBlockStorageBucketName}"

  private val branchName: String = "main"
  /**
    * Initializes a new repository in LakeFS.
    *
    * @param repoName         Name of the repository.
    * @param defaultBranch    Default branch name, usually "main".
    */
  def initRepo(
      repoName: String,
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
      branch: String,
      filePath: String,
      inputStream: InputStream
  ): ObjectStats = {
    val tempFilePath = Files.createTempFile("lakefs-upload-", ".tmp")
    val tempFileStream = new FileOutputStream(tempFilePath.toFile)
    val buffer = new Array[Byte](1024)

    // Create an iterator to repeatedly call inputStream.read, and direct buffered data to file
    Iterator
      .continually(inputStream.read(buffer))
      .takeWhile(_ != -1)
      .foreach(tempFileStream.write(buffer, 0, _))

    inputStream.close()
    tempFileStream.close()

    // Upload the temporary file to LakeFS
    objectsApi.uploadObject(repoName, branch, filePath).content(tempFilePath.toFile).execute()
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
  def withCreateVersion(repoName: String, branch: String, commitMessage: String)(
      operations: => Unit
  ): Commit = {
    operations
    val commit = new CommitCreation()
      .message(commitMessage)

    commitsApi.commit(repoName, branch, commit).execute()
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
   *
   */
  def initiatePresignedMultipartUploads(repoName: String, filePath: String, numberOfParts: Int): PresignMultipartUpload = {
    experimentalApi.createPresignMultipartUpload(repoName, branchName, filePath).parts(numberOfParts).execute()

  }

  def completePresignedMultipartUploads(repoName: String, filePath: String, uploadId: String): ObjectStats = {
    val completePresignMultipartUpload: CompletePresignMultipartUpload = new CompletePresignMultipartUpload()
    experimentalApi.completePresignMultipartUpload(repoName, branchName, uploadId, filePath).execute()
  }

  def abortPresignedMultipartUploads(repoName: String, filePath: String, uploadId: String): Unit  = {
    experimentalApi.abortPresignMultipartUpload(repoName, branchName, uploadId, filePath).execute()
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

}
