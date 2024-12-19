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

    assert(retrievedItems == items, "The retrieved items should match the written items.")
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
