package edu.uci.ics.amber.core.storage.result

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.ScalaFutures.convertScalaFuture
import org.scalatest.matchers.should.Matchers

import java.net.URI
import java.nio.file.{Files, Paths}

class PartitionDocumentSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {

  var partitionDocument: PartitionDocument[String] = _
  val numOfPartitions = 3
  val partitionId: String =
    Files.createTempDirectory("partition_doc_test").resolve("test_partition").toUri.toString

  before {
    // Initialize the PartitionDocument with a base ID and number of partitions
    partitionDocument = new PartitionDocument[String](partitionId, numOfPartitions)
  }

  after {
    // Clean up all partitions after each test
    partitionDocument.clear()
    for (i <- 0 until numOfPartitions) {
      Files.deleteIfExists(Paths.get(new URI(s"${partitionId}_partition$i")))
    }
    Files.deleteIfExists(Paths.get(new URI(partitionId).getPath.stripSuffix("/test_partition")))
  }

  "PartitionDocument" should "create and write to each partition" in {
    for (i <- 0 until numOfPartitions) {
      val fileDoc = partitionDocument.getItem(i)
      fileDoc.open()
      fileDoc.putOne(s"Data for partition $i")
      fileDoc.close()
    }

    for (i <- 0 until numOfPartitions) {
      val fileDoc = partitionDocument.getItem(i)
      val items = fileDoc.get().toList
      items should contain(s"Data for partition $i")
    }
  }

  it should "read from multiple partitions" in {
    // Write some data to each partition
    for (i <- 0 until numOfPartitions) {
      val fileDoc = partitionDocument.getItem(i)
      fileDoc.open()
      fileDoc.putOne(s"Content in partition $i")
      fileDoc.close()
    }

    // Read and verify data from each partition
    val partitionIterator = partitionDocument.get()
    for (i <- 0 until numOfPartitions) {
      val fileDoc = partitionIterator.next()
      val items = fileDoc.get().toList
      items should contain(s"Content in partition $i")
    }
  }

  it should "clear all partitions" in {
    // Write some data to each partition
    for (i <- 0 until numOfPartitions) {
      val fileDoc = partitionDocument.getItem(i)
      fileDoc.open()
      fileDoc.putOne(s"Some data in partition $i")
      fileDoc.close()
    }

    // Clear all partitions
    partitionDocument.clear()

    // Verify that each partition is empty
    for (i <- 0 until numOfPartitions) {
      val fileDoc = partitionDocument.getItem(i)
      val items = fileDoc.get().toList
      items should be(empty)
    }
  }

  it should "handle concurrent writes to different partitions" in {
    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent.Future

    val futures = (0 until numOfPartitions).map { i =>
      Future {
        val fileDoc = partitionDocument.getItem(i)
        fileDoc.open()
        fileDoc.putOne(s"Concurrent write to partition $i")
        fileDoc.close()
      }
    }

    Future.sequence(futures).futureValue

    // Verify data written concurrently
    for (i <- 0 until numOfPartitions) {
      val fileDoc = partitionDocument.getItem(i)
      val items = fileDoc.get().toList
      items should contain(s"Concurrent write to partition $i")
    }
  }

  it should "throw an exception when accessing an invalid partition index" in {
    val invalidIndex = numOfPartitions

    val exception = intercept[RuntimeException] {
      partitionDocument.getItem(invalidIndex)
    }

    exception.getMessage should include(s"Index $invalidIndex out of bound")
  }
}
