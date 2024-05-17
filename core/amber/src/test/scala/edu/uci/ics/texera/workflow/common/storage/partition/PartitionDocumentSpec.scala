package edu.uci.ics.texera.workflow.common.storage.partition

import edu.uci.ics.amber.engine.common.storage.partition.PartitionDocument

import java.net.URI
import java.nio.file.{Files, Path, Paths}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayOutputStream, InputStream}
import scala.util.Using

class PartitionDocumentSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {

  var baseTempURI: Path = _
  var partitionDocument: PartitionDocument = _
  val numOfPartitions: Int = 5

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

  before {
    // Create a temporary directory for the base URI
    baseTempURI = Files.createTempDirectory("partitionTest").resolve("partitionDoc")
    partitionDocument = new PartitionDocument(baseTempURI.toUri, numOfPartitions)
  }

  after {
    // Clean up: remove all files and the directory
    partitionDocument.remove()
  }

  "PartitionDocument" should "write and read content correctly for each partition" in {
    // Write to each partition
    val iterator = partitionDocument.get()
    var i = 0
    while (iterator.hasNext) {
      iterator.next().setItem(s"Content for partition $i")
      i += 1
    }

    // Verify each partition's content
    for (i <- 0 until numOfPartitions) {
      val doc = partitionDocument.getItem(i)
      val content = Using(doc.asInputStream()) { inStream =>
        new String(readAllBytes(inStream))
      }.getOrElse(fail("Failed to read from the partition"))
      content should be(s"Content for partition $i")
    }
  }

  it should "remove all partitions successfully" in {
    partitionDocument.remove()
    for (i <- 0 until numOfPartitions) {
      val uri = new URI(s"${baseTempURI.toUri}_$i")
      Files.exists(Paths.get(uri)) should be(false)
    }
  }
}
