package edu.uci.ics.texera.workflow.common.storage

import edu.uci.ics.amber.engine.common.storage.FileDocument

import java.net.URI
import java.nio.file.{Files, Paths}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream
import scala.util.Using

class FileDocumentSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {

  var tempFileURI: URI = _
  var fileDocument: FileDocument = _

  val initialContent = "Initial Content"
  val newContent = "New Content"
  before {
    // Generate a path for a temporary file without actually creating the file
    val tempPath = Files.createTempDirectory("testFileDocument").resolve("tempFile.txt")
    tempFileURI = tempPath.toUri
    fileDocument = new FileDocument(tempFileURI)

    val contentStream = new ByteArrayInputStream(initialContent.getBytes)
    // Write initial content to file
    fileDocument.writeWithStream(contentStream)
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
    val contentStream = new ByteArrayInputStream(newContent.getBytes)
    // overwrite with new content
    fileDocument.writeWithStream(contentStream)

    // Read back the content
    val content = Using(fileDocument.asInputStream()) { inStream =>
      new String(inStream.readAllBytes())
    }.getOrElse(fail("Failed to read from the FileDocument"))

    content should equal(initialContent + newContent)
  }

  it should "remove the file successfully" in {
    // Remove the file using FileDocument's remove method
    fileDocument.remove()

    // Verify the file does not exist
    Files.exists(Paths.get(tempFileURI)) should be(false)
  }
}
