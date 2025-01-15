package edu.uci.ics.amber.storage

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

class LakeFSFileStorageSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {

  val repoName = "test-repo"
  val branchName = "main"
  val filePath = "testFile.txt"

  var commitHashes: List[String] = List.empty

  val testFileContentV1 = "This is test file version 1"
  val testFileContentV2 = "This is test file version 2"

  before {
    // Initialize the repository
    LakeFSFileStorage.initRepo(repoName, "s3://test-bucket/")
  }

  after {
    // Delete the repository
    LakeFSFileStorage.deleteRepo(repoName)
  }

  private def writeFile(content: String): Unit = {
    val inputStream = new ByteArrayInputStream(content.getBytes)
    LakeFSFileStorage.writeFileToRepo(repoName, branchName, filePath, inputStream)
  }

  "LakeFSFileStorage" should "write and retrieve file content across versions" in {
    // Version 1
    LakeFSFileStorage.withCreateVersion(repoName, branchName, "Version 1") {
      writeFile(testFileContentV1)
    }

    // Version 2
    LakeFSFileStorage.withCreateVersion(repoName, branchName, "Version 2") {
      writeFile(testFileContentV2)
    }

    // Retrieve version 1
    val outputV1 = new ByteArrayOutputStream()
    LakeFSFileStorage.retrieveFileContent(repoName, branchName, filePath, outputV1)
    outputV1.toString should equal(testFileContentV2) // Latest content should be V2

    // (Optional) Verify version 1 content by using specific commit hash if available
  }

  it should "remove a file and verify its absence" in {
    // Write and commit version 1
    LakeFSFileStorage.withCreateVersion(repoName, branchName, "Add file") {
      writeFile(testFileContentV1)
    }

    // Remove the file and commit
    LakeFSFileStorage.withCreateVersion(repoName, branchName, "Remove file") {
      LakeFSFileStorage.removeFileFromRepo(repoName, branchName, filePath)
    }

    // Attempt to retrieve the deleted file (expect failure)
    val output = new ByteArrayOutputStream()
    intercept[Exception] {
      LakeFSFileStorage.retrieveFileContent(repoName, branchName, filePath, output)
    }
  }

  it should "handle multiple versions correctly" in {
    // Version 1
    LakeFSFileStorage.withCreateVersion(repoName, branchName, "Version 1") {
      writeFile(testFileContentV1)
    }

    // Version 2
    LakeFSFileStorage.withCreateVersion(repoName, branchName, "Version 2") {
      writeFile(testFileContentV2)
    }

    // Retrieve the latest version content (should be V2)
    val outputLatest = new ByteArrayOutputStream()
    LakeFSFileStorage.retrieveFileContent(repoName, branchName, filePath, outputLatest)
    outputLatest.toString should equal(testFileContentV2)
  }
}