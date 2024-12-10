package edu.uci.ics.amber.storage.result

import edu.uci.ics.amber.core.storage.result.ItemizedFileDocument

import java.net.URI
import java.nio.file.{Files, Paths}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.ScalaFutures.convertScalaFuture
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class ItemizedFileDocumentSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {

  var tempFileURI: URI = _
  var fileDocument: ItemizedFileDocument[String] = _

  val initialContent = "Initial Content"
  val newContent = "New Content"

  before {
    // Generate a path for a temporary file
    val tempPath = Files.createTempFile("", "")
    tempFileURI = tempPath.toUri
    fileDocument = new ItemizedFileDocument(tempFileURI)

    // Write initial content to file
    fileDocument.putOne(initialContent)
    fileDocument.close()
  }

  after {
    // Delete the temporary file
    Files.deleteIfExists(Paths.get(tempFileURI))
  }

  "ItemizedFileDocument" should "allow writing and flushing buffered items" in {
    // Add items to the buffer
    fileDocument.open()
    fileDocument.putOne("Buffered Item 1")
    fileDocument.putOne("Buffered Item 2")

    // Force a flush
    fileDocument.close()

    // Verify the items using the itemized get method
    fileDocument.getItem(0) should equal(initialContent)
    fileDocument.getItem(1) should equal("Buffered Item 1")
    fileDocument.getItem(2) should equal("Buffered Item 2")
  }

  it should "correctly flush the buffer when it reaches the buffer size" in {
    val largeBuffer = (1 to fileDocument.bufferSize).map(i => s"Item $i")

    fileDocument.open()
    largeBuffer.foreach(item => fileDocument.putOne(item))
    fileDocument.close()

    val items = fileDocument.get().toList

    items should contain(initialContent)
    largeBuffer.foreach { item =>
      items should contain(item)
    }
  }

  it should "allow removing items from the buffer" in {
    fileDocument.open()
    fileDocument.putOne("Item to keep")
    fileDocument.putOne("Item to remove")
    fileDocument.removeOne("Item to remove")
    fileDocument.close()

    val items = fileDocument.get().toList

    items should contain("Item to keep")
    items should not contain "Item to remove"
  }

  it should "handle concurrent buffered writes safely" in {
    val numberOfThreads = 5
    val futures = (1 to numberOfThreads).map { i =>
      Future {
        fileDocument.putOne(s"Content from thread $i")
      }
    }

    Future
      .sequence(futures)
      .map { _ =>
        fileDocument.close()
        val items = fileDocument.get().toList

        items should contain(initialContent)
        (1 to numberOfThreads).foreach { i =>
          items should contain(s"Content from thread $i")
        }
      }
      .futureValue
  }

  it should "handle concurrent reads and writes safely" in {
    val numberOfWrites = 5
    val numberOfReads = 5

    // Writer thread to add items
    val writerFuture = Future {
      fileDocument.open()
      (1 to numberOfWrites).foreach { i =>
        fileDocument.putOne(s"Read-Write Test Write $i")
      }
      fileDocument.close()
    }

    // Reader threads to read items concurrently
    val readerFutures = (1 to numberOfReads).map { _ =>
      Future {
        fileDocument.open()
        val items = fileDocument.get().toList
        fileDocument.close()
        items
      }
    }

    // Wait for all futures to complete
    val combinedFuture = for {
      _ <- writerFuture
      readerResults <- Future.sequence(readerFutures)
    } yield readerResults

    val results = combinedFuture.futureValue

    // Verify the results
    results.foreach { items =>
      items should contain(initialContent)
      (1 to numberOfWrites).foreach { i =>
        items should contain(s"Read-Write Test Write $i")
      }
    }
  }

  it should "handle writing after reopening the file" in {
    fileDocument.open()
    fileDocument.putOne("First Write")
    fileDocument.close()

    // Reopen and write again
    fileDocument.open()
    fileDocument.putOne("Second Write")
    fileDocument.close()

    val items = fileDocument.get().toList

    items should contain(initialContent)
    items should contain("First Write")
    items should contain("Second Write")
  }
}
