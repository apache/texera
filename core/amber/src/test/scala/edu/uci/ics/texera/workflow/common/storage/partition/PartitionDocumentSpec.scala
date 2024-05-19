package edu.uci.ics.texera.workflow.common.storage.partition

import edu.uci.ics.amber.engine.common.storage.partition.PartitionDocument
import java.net.URI
import java.nio.file.{Files, Path, Paths}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers

class PartitionDocumentSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {

  var baseTempURI: Path = _
  var partitionDocument: PartitionDocument[String] = _
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
    val iterator = partitionDocument.get()
    var i = 0
    while (iterator.hasNext) {
      iterator.next().append(s"Content for partition $i")
      i += 1
    }

    // Verify each partition's content
    for (i <- 0 until numOfPartitions) {
      val doc = partitionDocument.getItem(i)
      val content = doc.getItem(0)
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

  it should "append items and read using iterator" in {
    // Append items to each partition
    for (i <- 0 until numOfPartitions) {
      val doc = partitionDocument.getItem(i)
      doc.append(s"First content for partition $i")
      doc.append(s"Second content for partition $i")
    }

    // Verify the content using iterator
    for (i <- 0 until numOfPartitions) {
      val doc = partitionDocument.getItem(i)
      val iterator = doc.get()
      iterator.next() should be(s"First content for partition $i")
      iterator.next() should be(s"Second content for partition $i")
    }
  }

  it should "append items and get the first item correctly" in {
    // Append items to each partition
    for (i <- 0 until numOfPartitions) {
      val doc = partitionDocument.getItem(i)
      doc.append(s"First content for partition $i")
      doc.append(s"Second content for partition $i")
    }

    // Verify the first item in each partition
    for (i <- 0 until numOfPartitions) {
      val doc = partitionDocument.getItem(i)
      doc.getItem(0) should be(s"First content for partition $i")
    }
  }
}
