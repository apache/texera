package edu.uci.ics.texera.workflow.common.storage

import edu.uci.ics.amber.engine.common.storage.FileDocument

import java.net.URI
import java.nio.file.{Files, Paths}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.ScalaFutures.convertScalaFuture
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Using

class FileDocumentSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {

  var tempFileURI: URI = _
  var fileDocument: FileDocument[String] = _

  val initialContent = "Initial Content"
  val newContent = "New Content"
  before {
    // Generate a path for a temporary file without actually creating the file
    val tempPath = Files.createTempFile("", "")
    tempFileURI = tempPath.toUri
    fileDocument = new FileDocument(tempFileURI)

    val contentStream = new ByteArrayInputStream(initialContent.getBytes)
    // Write initial content to file
    fileDocument.appendStream(contentStream)
    contentStream.close()
  }

  after {
    // Delete the temporary file
    Files.deleteIfExists(Paths.get(tempFileURI))
  }

  private def readAllBytes(inputStream: InputStream): Array[Byte] = {
    val buffer = new ByteArrayOutputStream()
    val data = new Array[Byte](1024)
    var nRead = 0
    while ({
      nRead = inputStream.read(data, 0, data.length)
      nRead != -1
    }) {
      buffer.write(data, 0, nRead)
    }
    buffer.flush()
    buffer.toByteArray
  }

  "FileDocument" should "correctly report its URI" in {
    fileDocument.getURI should be(tempFileURI)
  }

  it should "allow reading from the file" in {
    val content = Using(fileDocument.asInputStream()) { inStream =>
      new String(readAllBytes(inStream))
    }.getOrElse(fail("Failed to read from the file"))

    content should equal(initialContent)
  }

  it should "allow writing to the file" in {
    fileDocument.appendStream(new ByteArrayInputStream(newContent.getBytes))

    // Read back the content
    val content = Using(fileDocument.asInputStream()) { inStream =>
      new String(readAllBytes(inStream))
    }.getOrElse(fail("Failed to read from the FileDocument"))

    content should be(initialContent + newContent)
  }

  it should "remove the file successfully" in {
    // Remove the file using FileDocument's remove method
    fileDocument.remove()
    Files.exists(Paths.get(tempFileURI)) should be(false)
  }

  it should "handle concurrent writes safely" in {
    val numberOfThreads = 10
    val futures = (1 to numberOfThreads).map { _ =>
      Future {
        val contentStream = new ByteArrayInputStream(s"Content from thread".getBytes)
        // Use the same FileDocument instance for all threads
        fileDocument.appendStream(contentStream)
      }
    }
    Future
      .sequence(futures)
      .map { _ =>
        val content = Using(fileDocument.asInputStream()) { inStream =>
          new String(readAllBytes(inStream))
        }.getOrElse(fail("Failed to read from the FileDocument"))
        content should include("Content from thread")
      }
      .futureValue
  }

  it should "handle concurrent reads safely" in {
    val contentStream = new ByteArrayInputStream(newContent.getBytes)
    fileDocument.appendStream(contentStream)

    val readers: Seq[Future[String]] = (1 to 5).map { _ =>
      Future {
        Using(fileDocument.asInputStream()) { inStream =>
          new String(readAllBytes(inStream))
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

  it should "correctly write and read a large amount of data" in {
    // Generate a large string of 20,000 characters
    val largeContent = "A" * 20000
    fileDocument.appendStream(new ByteArrayInputStream(largeContent.getBytes))

    // Read back the content
    val content = Using(fileDocument.asInputStream()) { inStream =>
      new String(readAllBytes(inStream))
    }.getOrElse(fail("Failed to read from the FileDocument"))

    content should be(initialContent + largeContent)
  }
}
