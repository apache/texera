package edu.uci.ics.amber.storage.result

import edu.uci.ics.amber.core.storage.result.ArrowFileDocument
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.apache.arrow.vector.{VarCharVector, VectorSchemaRoot}
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.ScalaFutures.convertScalaFuture
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URI
import java.nio.file.{Files, Paths}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

object ArrowFileDocumentSpec {
  def stringSerializer(item: String, index: Int, root: VectorSchemaRoot): Unit = {
    val vector = root.getVector("data").asInstanceOf[VarCharVector]
    vector.setSafe(index, item.getBytes("UTF-8"))
  }

  def stringDeserializer(index: Int, root: VectorSchemaRoot): String = {
    new String(root.getVector("data").asInstanceOf[VarCharVector].get(index))
  }
}

class ArrowFileDocumentSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {

  val stringArrowSchema = new Schema(
    List(
      Field.nullablePrimitive("data", ArrowType.Utf8.INSTANCE)
    ).asJava
  )

  def createDocument(): ArrowFileDocument[String] = {
    val tempPath = Files.createTempFile("arrow_test", ".arrow")
    new ArrowFileDocument[String](
      tempPath.toUri,
      stringArrowSchema,
      ArrowFileDocumentSpec.stringSerializer,
      ArrowFileDocumentSpec.stringDeserializer
    )
  }

  def openDocument(uri: URI): ArrowFileDocument[String] = {
    new ArrowFileDocument[String](
      uri,
      stringArrowSchema,
      ArrowFileDocumentSpec.stringSerializer,
      ArrowFileDocumentSpec.stringDeserializer
    )
  }

  def deleteDocument(doc: ArrowFileDocument[String]): Unit = {
    Files.deleteIfExists(Paths.get(doc.getURI))
  }

  "ArrowFileDocument" should "allow writing and flushing buffered items" in {
    val doc = createDocument()
    doc.open()
    doc.putOne("Buffered Item 1")
    doc.putOne("Buffered Item 2")
    doc.close()

    val items = doc.get().toList
    items should contain theSameElementsAs List("Buffered Item 1", "Buffered Item 2")

    deleteDocument(doc)
  }

  it should "correctly flush the buffer when it reaches the buffer size" in {
    val doc = createDocument()
    val largeBuffer = (1 to doc.bufferSize).map(i => s"Item $i")

    doc.open()
    largeBuffer.foreach(item => doc.putOne(item))
    doc.close()

    val items = doc.get().toList
    items should contain theSameElementsAs largeBuffer

    deleteDocument(doc)
  }

  it should "override file content when reopened for writing" in {
    val doc = createDocument()

    // First write
    doc.open()
    doc.putOne("First Write")
    doc.close()

    // Second write should override the first one
    doc.open()
    doc.putOne("Second Write")
    doc.close()

    val items = doc.get().toList
    items should contain only "Second Write"

    deleteDocument(doc)
  }

  it should "handle concurrent buffered writes safely" in {
    val doc = createDocument()
    val numberOfThreads = 5

    doc.open()
    val futures = (1 to numberOfThreads).map { i =>
      Future {
        doc.putOne(s"Content from thread $i")
      }
    }

    Future.sequence(futures).futureValue
    doc.close()

    val items = doc.get().toList
    (1 to numberOfThreads).foreach { i =>
      items should contain(s"Content from thread $i")
    }

    deleteDocument(doc)
  }

  it should "handle concurrent reads and writes safely" in {
    val doc = createDocument()

    // Writer thread to add items
    doc.open()
    val writerFuture = Future {
      (1 to 10).foreach { i =>
        doc.putOne(s"Write $i")
      }
    }

    // Reader threads to read items concurrently
    val readerFutures = (1 to 3).map { _ =>
      Future {
        doc.get().toList
      }
    }

    Future.sequence(readerFutures).futureValue
    writerFuture.futureValue
    doc.close()

    val finalItems = doc.get().toList
    (1 to 10).foreach { i =>
      finalItems should contain(s"Write $i")
    }

    deleteDocument(doc)
  }
}
