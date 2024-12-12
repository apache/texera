package edu.uci.ics.amber.storage.result

import edu.uci.ics.amber.core.storage.result.ArrowFileDocument
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.{IntVector, VarCharVector, VectorSchemaRoot}
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.ScalaFutures.convertScalaFuture
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URI
import java.nio.file.{Files, Paths}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

class ArrowFileDocumentSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {

  val stringArrowSchema = new Schema(List(
    Field.nullablePrimitive("data", ArrowType.Utf8.INSTANCE)
  ).asJava)

  def stringSerializer(item: String, index: Int, root: VectorSchemaRoot): Unit = {
    val vector = root.getVector("data").asInstanceOf[VarCharVector]
    vector.setSafe(index, item.getBytes("UTF-8"))
  }

  def stringDeserializer(index: Int, root: VectorSchemaRoot): String = {
    new String(root.getVector("data").asInstanceOf[VarCharVector].get(index))
  }

  def createDocument(): ArrowFileDocument[String] = {
    val tempPath = Files.createTempFile("arrow_test", ".arrow")
    new ArrowFileDocument[String](tempPath.toUri, stringArrowSchema, stringSerializer, stringDeserializer)
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
    items should contain("Buffered Item 1")
    items should contain("Buffered Item 2")

    deleteDocument(doc)
  }

  it should "correctly flush the buffer when it reaches the buffer size" in {
    val doc = createDocument()
    val largeBuffer = (1 to doc.bufferSize).map(i => s"Item $i")

    doc.open()
    largeBuffer.foreach(item => doc.putOne(item))
    doc.close()

    val items = doc.get().toList
    largeBuffer.foreach { item =>
      items should contain(item)
    }

    deleteDocument(doc)
  }

  it should "allow removing items from the buffer" in {
    val doc = createDocument()
    doc.open()
    doc.putOne("Item to keep")
    doc.putOne("Item to remove")
    doc.removeOne("Item to remove")
    doc.close()

    val items = doc.get().toList
    items should contain("Item to keep")
    items should not contain "Item to remove"

    deleteDocument(doc)
  }

  it should "handle concurrent buffered writes safely" in {
    val doc = createDocument()
    val numberOfThreads = 5

    val futures = (1 to numberOfThreads).map { i =>
      Future {
        doc.open()
        doc.putOne(s"Content from thread $i")
        doc.close()
      }
    }

    Future.sequence(futures).futureValue

    val items = doc.get().toList
    (1 to numberOfThreads).foreach { i =>
      items should contain(s"Content from thread $i")
    }

    deleteDocument(doc)
  }

  it should "handle writing after reopening the file" in {
    val doc = createDocument()

    // First write
    doc.open()
    doc.putOne("First Write")
    doc.close()

    // Second write
    doc.open()
    doc.putOne("Second Write")
    doc.close()

    val items = doc.get().toList
    items should contain("First Write")
    items should contain("Second Write")

    deleteDocument(doc)
  }
}