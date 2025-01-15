package edu.uci.ics.amber.storage

import io.lakefs.clients.sdk._
import io.lakefs.clients.sdk.model._

import java.io.{File, FileOutputStream, InputStream, OutputStream}
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

/**
 * LakeFSFileStorage provides high-level file storage operations using LakeFS,
 * similar to Git operations for version control and file management.
 */
object LakeFSFileStorage {

  // Lazy initialization of LakeFS API clients
  private lazy val apiClient: ApiClient = LakeFSApiClientInstance.getInstance()
  private lazy val repoApi: RepositoriesApi = new RepositoriesApi(apiClient)
  private lazy val objectsApi: ObjectsApi = new ObjectsApi(apiClient)
  private lazy val commitsApi: CommitsApi = new CommitsApi(apiClient)

  /**
   * Initializes a new repository in LakeFS.
   *
   * @param repoName         Name of the repository.
   * @param storageNamespace Storage path (e.g., "s3://bucket-name/").
   * @param defaultBranch    Default branch name, usually "main".
   */
  def initRepo(repoName: String, storageNamespace: String, defaultBranch: String = "main"): Repository = {
    val repo = new RepositoryCreation()
      .name(repoName)
      .storageNamespace(storageNamespace)
      .defaultBranch(defaultBranch)

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
  def writeFileToRepo(repoName: String, branch: String, filePath: String, inputStream: InputStream): ObjectStats = {
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
   * @param repoName Repository name.
   * @param branch   Branch name.
   * @param commitMessage Commit message.
   * @param operations File operations to perform before committing.
   */
  def withCreateVersion(repoName: String, branch: String, commitMessage: String)(operations: => Unit): Unit = {
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
   * @param outputStream OutputStream to write the content.
   */
  def retrieveFileContent(repoName: String, commitHash: String, filePath: String, outputStream: OutputStream): File = {
    objectsApi.getObject(repoName, commitHash, filePath).execute()
  }

  /**
   * Deletes an entire repository.
   *
   * @param repoName Name of the repository to delete.
   */
  def deleteRepo(repoName: String): Unit = {
    repoApi.deleteRepository(repoName).execute()
  }
}