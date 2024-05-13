package edu.uci.ics.texera.workflow.common.storage.partition

import edu.uci.ics.amber.engine.common.storage.partition.PartitionDocument

import java.net.URI
import java.nio.file.{Files, Path, Paths}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.Using

class PartitionDocumentSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {

  var baseTempURI: Path = _
  var partitionDocument: PartitionDocument = _
  val numOfPartitions: Int = 5

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
    val iterator = partitionDocument.read()
    var i = 0
    while (iterator.hasNext) {
      val content = s"Content for partition $i"
      val stream = new ByteArrayInputStream(content.getBytes)
      iterator.next().writeWithStream(stream)
      i += 1
    }

    // Verify each partition's content
    for (i <- 0 until numOfPartitions) {
      val doc = partitionDocument.readItem(i)
      val content = Using(doc.asInputStream()) { inStream =>
        new String(inStream.readAllBytes())
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
