package edu.uci.ics.amber.core.storage.model

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

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

    assert(retrievedItems == items)
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
    assert(retrievedBatch1 == batch1, "Reader should read the first batch correctly.")

    // Write the second batch
    val writer2 = document.writer()
    writer2.open()
    batch2.foreach(writer2.putOne)
    writer2.close()

    // The reader should detect and read the second batch
    val retrievedBatch2 = reader.toList
    assert(retrievedBatch2 == batch2, "Reader should read the second batch correctly.")

    // Verify that the combined retrieved items match the original items
    val retrievedItems = retrievedBatch1 ++ retrievedBatch2
    assert(retrievedItems == allItems, "Reader should read all items correctly.")
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

  /**
    * Generates a sample list of items for testing.
    * Subclasses should override this to provide their specific sample items.
    * @return a list of sample items of type T.
    */
  def generateSampleItems(): List[T]
}
