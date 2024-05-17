package edu.uci.ics.texera.workflow.common.storage

import edu.uci.ics.amber.engine.common.storage.FileDocument

import java.net.URI
import java.nio.file.{Files, Paths}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.ScalaFutures.convertScalaFuture
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Using

class FileDocumentSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {

  var tempFileURI: URI = _
  var fileDocument: FileDocument = _

  val initialContent = "Initial Content"
  val newContent = "New Content"
  before {
    // Generate a path for a temporary file without actually creating the file
    val tempPath = Files.createTempFile("", "")
    tempFileURI = tempPath.toUri
    fileDocument = new FileDocument(tempFileURI)

    val contentStream = new ByteArrayInputStream(initialContent.getBytes)
    // Write initial content to file
    fileDocument.write(contentStream)
    contentStream.close()
  }

  after {
    // Delete the temporary file
    Files.deleteIfExists(Paths.get(tempFileURI))
  }

  "FileDocument" should "correctly report its URI" in {
    fileDocument.getURI should be(tempFileURI)
  }

  it should "allow reading from the file" in {
    val content = Using(fileDocument.asInputStream()) { inStream =>
      new String(inStream.readAllBytes())
    }.getOrElse(fail("Failed to read from the file"))

    content should equal(initialContent)
  }

  it should "allow writing to the file" in {
    fileDocument.setItem(newContent)

    // Read back the content
    val content = Using(fileDocument.asInputStream()) { inStream =>
      new String(inStream.readAllBytes())
    }.getOrElse(fail("Failed to read from the FileDocument"))

    content should be(initialContent + newContent)
  }

  it should "remove the file successfully" in {
    // Remove the file using FileDocument's remove method
    fileDocument.remove()
    Files.exists(Paths.get(tempFileURI)) should be(false)
  }

  "FileDocument" should "handle concurrent writes safely" in {
    val numberOfThreads = 10
    val futures = (1 to numberOfThreads).map { i =>
      Future {
        val contentStream = new ByteArrayInputStream(s"Content from thread".getBytes)
        // multiple document of the same URI try to do write here
        new FileDocument(tempFileURI).write(contentStream)
      }
    }
    Future
      .sequence(futures)
      .map { _ =>
        val content = Using(fileDocument.asInputStream()) { inStream =>
          new String(inStream.readAllBytes())
        }.getOrElse(fail("Failed to read from the FileDocument"))
        content should include("Content from thread".repeat(numberOfThreads))
      }
      .futureValue
  }

  it should "handle concurrent reads and writes safely" in {
    val writer = Future {
      val contentStream = new ByteArrayInputStream(newContent.getBytes)
      fileDocument.write(contentStream)
    }

    val readers = (1 to 5).map { _ =>
      Future {
        Using(fileDocument.asInputStream()) { inStream =>
          new String(inStream.readAllBytes())
        }.getOrElse(fail("Failed to read from the FileDocument"))
      }
    }

    Future
      .sequence(readers)
      .map { results =>
        results.foreach { result =>
          result should be(initialContent + newContent)
        }
      }
      .futureValue
  }

  it should "handle multiple remove calls gracefully" in {
    // Remove the file for the first time
    fileDocument.remove()
    Files.exists(Paths.get(tempFileURI)) should be(false)

    // Attempt to remove the file again and catch the exception
    val exception = intercept[RuntimeException] {
      fileDocument.remove()
    }

    exception.getMessage should include(s"File $tempFileURI doesn't exist")
  }
}
