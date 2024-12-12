package edu.uci.ics.amber.core.storage.result

import edu.uci.ics.amber.core.storage.result.PartitionedFileDocument.getPartitionURI
import org.apache.commons.vfs2.{FileObject, VFS}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.ScalaFutures.convertScalaFuture
import org.scalatest.matchers.should.Matchers

class PartitionedFileDocumentSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {

  var partitionDocument: PartitionedFileDocument[ArrowFileDocument[String], String] = _
  val numOfPartitions = 3
  val partitionId: String = "partition_doc_test"

  before {
    // Initialize the PartitionDocument with a base ID and number of partitions
    partitionDocument = new PartitionedFileDocument[ArrowFileDocument[String], String](
      partitionId,
      numOfPartitions,
      uri => new ArrowFileDocument[String](uri)
    )
  }

  after {
    // Clean up all partitions after each test
    partitionDocument.clear()
  }

  "PartitionDocument" should "create and write to each partition directly" in {
    for (i <- 0 until numOfPartitions) {
      val partitionURI = getPartitionURI(partitionId, i)
      val fileDoc = new ArrowFileDocument[String](partitionURI)
      fileDoc.open()
      fileDoc.putOne(s"Data for partition $i")
      fileDoc.close()
    }

    for (i <- 0 until numOfPartitions) {
      val item = partitionDocument.getItem(i)
      item should be(s"Data for partition $i")
    }
  }

  it should "read from multiple partitions" in {
    // Write some data directly to each partition
    for (i <- 0 until numOfPartitions) {
      val partitionURI = getPartitionURI(partitionId, i)
      val fileDoc = new ArrowFileDocument[String](partitionURI)
      fileDoc.open()
      fileDoc.putOne(s"Content in partition $i")
      fileDoc.close()
    }

    // Read and verify data from each partition using PartitionDocument
    val items = partitionDocument.get().toList
    for (i <- 0 until numOfPartitions) {
      items should contain(s"Content in partition $i")
    }
  }

  it should "clear all partitions" in {
    // Write some data directly to each partition
    for (i <- 0 until numOfPartitions) {
      val partitionURI = getPartitionURI(partitionId, i)
      val fileDoc = new ArrowFileDocument[String](partitionURI)
      fileDoc.open()
      fileDoc.putOne(s"Some data in partition $i")
      fileDoc.close()
    }

    // Clear all partitions using PartitionDocument
    partitionDocument.clear()

    for (i <- 0 until numOfPartitions) {
      val partitionURI = getPartitionURI(partitionId, i)
      val file: FileObject = VFS.getManager.resolveFile(partitionURI.toString)
      file.exists() should be(false)
    }
  }

  it should "handle concurrent writes to different partitions" in {
    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent.Future

    val futures = (0 until numOfPartitions).map { i =>
      Future {
        val partitionURI = getPartitionURI(partitionId, i)
        val fileDoc = new ArrowFileDocument[String](partitionURI)
        fileDoc.open()
        fileDoc.putOne(s"Concurrent write to partition $i")
        fileDoc.close()
      }
    }

    Future.sequence(futures).futureValue

    // Verify data written concurrently using PartitionDocument
    val items = partitionDocument.get().toList
    for (i <- 0 until numOfPartitions) {
      items should contain(s"Concurrent write to partition $i")
    }
  }
}
