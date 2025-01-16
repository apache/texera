package edu.uci.ics.amber.storage

import edu.uci.ics.amber.core.storage.LakeFSFileStorage
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayInputStream, File}
import java.nio.file.Files
import java.util.UUID

class LakeFSFileStorageSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  val repoName: String = UUID.randomUUID().toString
  val branchName = "main"

  val fileContent1 = "Content of file 1"
  val fileContent2 = "Content of file 2"
  val fileContent3 = "Content of file 3"
  val fileContent4 = "Content of file 4"

  val filePaths: Seq[String] = Seq(
    "dir1/file1.txt",
    "dir1/file2.txt",
    "dir1/subdir1/file3.txt",
    "dir2/file4.txt"
  )

  override def beforeAll(): Unit = {
    LakeFSFileStorage.initRepo(repoName, s"local://$repoName")
  }

  override def afterAll(): Unit = {
    LakeFSFileStorage.deleteRepo(repoName)
  }

  private def writeFile(filePath: String, content: String): Unit = {
    val inputStream = new ByteArrayInputStream(content.getBytes)
    LakeFSFileStorage.writeFileToRepo(repoName, branchName, filePath, inputStream)
  }

  private def readFileContent(file: File): String = {
    new String(Files.readAllBytes(file.toPath))
  }

  private def findCommitByMessage(message: String): Option[String] = {
    LakeFSFileStorage.retrieveVersionsOfRepository(repoName, branchName)
      .find(_.getMessage == message)
      .map(_.getId)
  }

  "LakeFSFileStorage" should "write multiple files and verify contents across versions" in {
    // Version 1: Add file1.txt and file2.txt
    LakeFSFileStorage.withCreateVersion(repoName, branchName, "Add file1 and file2") {
      writeFile(filePaths(0), fileContent1)
      writeFile(filePaths(1), fileContent2)
    }

    // Version 2: Add file3.txt
    LakeFSFileStorage.withCreateVersion(repoName, branchName, "Add file3") {
      writeFile(filePaths(2), fileContent3)
    }

    // Version 3: Add file4.txt
    LakeFSFileStorage.withCreateVersion(repoName, branchName, "Add file4") {
      writeFile(filePaths(3), fileContent4)
    }

    // Validate Version 1
    val commitV1 = findCommitByMessage("Add file1 and file2").get
    val objectsV1 = LakeFSFileStorage.retrieveObjectsOfVersion(repoName, commitV1).map(_.getPath)
    objectsV1 should contain allElementsOf Seq(filePaths(0), filePaths(1))
    objectsV1 should not contain filePaths(2)

    // Validate Version 2
    val commitV2 = findCommitByMessage("Add file3").get
    val objectsV2 = LakeFSFileStorage.retrieveObjectsOfVersion(repoName, commitV2).map(_.getPath)
    objectsV2 should contain allElementsOf Seq(filePaths(0), filePaths(1), filePaths(2))
    objectsV2 should not contain filePaths(3)

    // Validate Version 3
    val commitV3 = findCommitByMessage("Add file4").get
    val objects = LakeFSFileStorage.retrieveObjectsOfVersion(repoName, commitV3)
    val objectsV3 = LakeFSFileStorage.retrieveObjectsOfVersion(repoName, commitV3).map(_.getPath)
    objectsV3 should contain allElementsOf filePaths

    // Verify content of file4.txt in the latest commit
    val file4 = LakeFSFileStorage.retrieveFileContent(repoName, commitV3, filePaths(3))
    readFileContent(file4) should equal(fileContent4)
  }

  it should "remove a file and verify its absence in the next version" in {
    // Delete file2.txt and commit the change
    LakeFSFileStorage.withCreateVersion(repoName, branchName, "Remove file2.txt") {
      LakeFSFileStorage.removeFileFromRepo(repoName, branchName, filePaths(1))
    }

    // Locate the commit by message
    val deleteCommit = findCommitByMessage("Remove file2.txt").get

    // Verify file2.txt is absent in the latest commit
    val objectsAfterDeletion = LakeFSFileStorage.retrieveObjectsOfVersion(repoName, deleteCommit).map(_.getPath)
    objectsAfterDeletion should not contain filePaths(1)

    // Verify file1.txt is still present
    val file1 = LakeFSFileStorage.retrieveFileContent(repoName, deleteCommit, filePaths(0))
    readFileContent(file1) should equal(fileContent1)
  }

  it should "maintain hierarchical structure in file retrieval" in {
    // Get the latest commit
    val latestCommit = LakeFSFileStorage.retrieveVersionsOfRepository(repoName, branchName).head.getId

    // Retrieve all objects
    val objects = LakeFSFileStorage.retrieveObjectsOfVersion(repoName, latestCommit)
    val objectPaths = objects.map(_.getPath)

    // Verify nested directories are intact
    objectPaths should contain("dir1/subdir1/file3.txt")
    objectPaths should contain("dir2/file4.txt")
  }
}