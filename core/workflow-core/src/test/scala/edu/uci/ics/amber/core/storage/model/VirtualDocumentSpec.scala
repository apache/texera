package edu.uci.ics.amber.core.storage.model

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

/**
  * A trait for testing VirtualDocument implementations.
  * Provides common read/write test cases and hooks for subclasses to customize.
  * @tparam T the type of data that the VirtualDocument handles.
  */
trait VirtualDocumentSpec[T] extends AnyFlatSpec with BeforeAndAfterEach {

  /**
    * Constructs the VirtualDocument instance to be tested.
    * Subclasses should override this to provide their specific implementation.
    */
  def getDocument: VirtualDocument[T]

  /**
    * Checks if the document has been cleared.
    * Subclasses should override this to provide their specific check.
    * @return true if the document is cleared, false otherwise.
    */
  def isDocumentCleared: Boolean

  // VirtualDocument instance for each test
  var document: VirtualDocument[T] = _

  override def beforeEach(): Unit = {
    document = getDocument
  }

  "VirtualDocument" should "write and read items successfully" in {
    val items = generateSampleItems()

    // Get writer and write items
    val writer = document.writer()
    writer.open()
    items.foreach(writer.putOne)
    writer.close()

    // Read items back
    val retrievedItems = document.get().toList

    assert(retrievedItems.toSet == items.toSet)
  }

  "VirtualDocument" should "read items while writer is writing new data" in {
    val allItems = generateSampleItems()

    // Split the items into two batches
    val (batch1, batch2) = allItems.splitAt(allItems.length / 2)

    // Create a reader before any data is written
    val reader = document.get()
    assert(!reader.hasNext, "Reader should initially have no data.")

    // Write the first batch
    val writer = document.writer()
    writer.open()
    batch1.foreach(writer.putOne)
    writer.close()

    // The reader should detect and read the first batch
    val retrievedBatch1 = reader.take(batch1.length).toList
    assert(retrievedBatch1.toSet == batch1.toSet, "Reader should read the first batch correctly.")

    // Write the second batch
    val writer2 = document.writer()
    writer2.open()
    batch2.foreach(writer2.putOne)
    writer2.close()

    // The reader should detect and read the second batch
    val retrievedBatch2 = reader.toList
    assert(retrievedBatch2.toSet == batch2.toSet, "Reader should read the second batch correctly.")
  }
  it should "clear the document" in {
    val items = generateSampleItems()

    // Write items
    val writer = document.writer()
    writer.open()
    items.foreach(writer.putOne)
    writer.close()

    // Ensure items are written
    assert(document.get().nonEmpty, "The document should contain items before clearing.")

    // Clear the document
    document.clear()

    // Check if the document is cleared
    assert(isDocumentCleared, "The document should be cleared after calling clear.")
    assert(document.get().isEmpty, "The document should have no items after clearing.")
  }

  it should "handle empty reads gracefully" in {
    val retrievedItems = document.get().toList
    assert(retrievedItems.isEmpty, "Reading from an empty document should return an empty list.")
  }

  it should "handle concurrent writes and read all items correctly" in {
    val allItems = generateSampleItems()
    val numWriters = 10

    // Calculate the batch size and the remainder
    val batchSize = allItems.length / numWriters
    val remainder = allItems.length % numWriters

    // Create batches using a simple for loop
    val itemBatches = (0 until numWriters).map { i =>
      val start = i * batchSize + Math.min(i, remainder)
      val end = start + batchSize + (if (i < remainder) 1 else 0)
      allItems.slice(start, end)
    }.toList

    assert(
      itemBatches.length == numWriters,
      s"Expected $numWriters batches but got ${itemBatches.length}"
    )

    // Perform concurrent writes
    val writeFutures = itemBatches.map { batch =>
      Future {
        val writer = document.writer()
        writer.open()
        batch.foreach(writer.putOne)
        writer.close()
      }
    }

    // Wait for all writers to complete
    Await.result(Future.sequence(writeFutures), 30.seconds)

    // Read all items back
    val retrievedItems = document.get().toList

    // Verify that the retrieved items match the original items
    assert(
      retrievedItems.toSet == allItems.toSet,
      "All items should be read correctly after concurrent writes."
    )
  }

  it should "allow a reader to read data while a writer is writing items incrementally" in {
    val allItems = generateSampleItems()
    val batchSize = allItems.length / 5 // Divide items into 5 incremental batches

    // Split items into 5 batches
    val itemBatches = allItems.grouped(batchSize).toList

    // Flag to indicate when writing is done
    @volatile var writingComplete = false

    // Start the writer in a Future to write batches with delays
    val writerFuture = Future {
      val writer = document.writer()
      writer.open()
      try {
        itemBatches.foreach { batch =>
          batch.foreach(writer.putOne)
          Thread.sleep(500) // Simulate delay between batches
        }
      } finally {
        writer.close()
        writingComplete = true
      }
    }

    // Start the reader in another Future
    val readerFuture = Future {
      val reader = document.get()
      val retrievedItems = scala.collection.mutable.ListBuffer[T]()

      // Keep checking for new data until writing is complete and no more items are available
      while (!writingComplete || reader.hasNext) {
        if (reader.hasNext) {
          retrievedItems += reader.next()
        } else {
          Thread.sleep(200) // Wait before retrying to avoid busy-waiting
        }
      }

      retrievedItems.toList
    }

    // Wait for both writer and reader to complete
    val retrievedItems = Await.result(readerFuture, 30.seconds)
    Await.result(writerFuture, 30.seconds)

    // Verify that the retrieved items match the original items
    assert(
      retrievedItems.toSet == allItems.toSet,
      "All items should be read correctly while writing is happening concurrently."
    )
  }

  it should "read a specific range of items correctly" in {
    val allItems = generateSampleItems()

    // Write items
    val writer = document.writer()
    writer.open()
    allItems.foreach(writer.putOne)
    writer.close()

    // Read a specific range
    val from = 5
    val until = 15
    val retrievedItems = document.getRange(from, until).toList

    // Verify the retrieved range
    assert(
      retrievedItems.size == allItems.slice(from, until).size,
      s"Items in range ($from, $until) should match."
    )
  }

  it should "read items after a specific offset correctly" in {
    val allItems = generateSampleItems()

    // Write items
    val writer = document.writer()
    writer.open()
    allItems.foreach(writer.putOne)
    writer.close()

    // Read items after a specific offset
    val offset = 10
    val retrievedItems = document.getAfter(offset).toList

    // Verify the retrieved items
    assert(
      retrievedItems == allItems.drop(offset + 1),
      s"Items after offset $offset should match."
    )
  }

  /**
    * Generates a sample list of items for testing.
    * Subclasses should override this to provide their specific sample items.
    * @return a list of sample items of type T.
    */
  def generateSampleItems(): List[T]
}
