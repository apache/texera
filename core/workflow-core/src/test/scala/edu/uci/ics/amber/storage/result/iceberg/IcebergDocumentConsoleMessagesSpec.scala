package edu.uci.ics.amber.storage.result.iceberg

import edu.uci.ics.amber.core.storage.model.{
  BufferedItemWriter,
  VirtualDocument,
  VirtualDocumentSpec
}
import edu.uci.ics.amber.core.storage.{DocumentFactory, VFSURIFactory}
import edu.uci.ics.amber.core.storage.result.IcebergTableSchema
import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  WorkflowIdentity
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.util.IcebergUtil
import org.apache.iceberg.{Schema => IcebergSchema}

import scala.util.{Try, Using}
import org.scalatest.BeforeAndAfterAll

import scala.util.Using.Releasable
import java.net.URI

class IcebergDocumentConsoleMessagesSpec
    extends AnyFlatSpec
    with Matchers
    with VirtualDocumentSpec[Tuple]
    with BeforeAndAfterAll {

  private val amberSchema: Schema = IcebergTableSchema.consoleMessagesSchema
  private val icebergSchema: IcebergSchema = IcebergUtil.toIcebergSchema(amberSchema)
  private val uri: URI = VFSURIFactory.createConsoleMessagesURI(
    WorkflowIdentity(0),
    ExecutionIdentity(0),
    OperatorIdentity("test_operator")
  )

  override def generateSampleItems(): List[Tuple] = {
    List(
      new Tuple(amberSchema, Array("First console message")),
      new Tuple(amberSchema, Array("Second console message")),
      new Tuple(amberSchema, Array("Third console message"))
    )
  }

  implicit val bufferedItemWriterReleasable: Releasable[BufferedItemWriter[Tuple]] =
    (resource: BufferedItemWriter[Tuple]) => resource.close()

  "IcebergDocument" should "write and read console messages successfully" in {
    // Create the document
    val document = getDocument

    Using.resource(document.writer("console_messages_test")) {
      case writer: BufferedItemWriter[Tuple] =>
        try {
          writer.open()
          generateSampleItems().foreach(writer.putOne)
          writer.close()
        } catch {
          case e: Exception => fail(s"Failed to write messages: ${e.getMessage}")
        }
    }

    val retrievedMessages = Try(document.get().toList.collect { case t: Tuple => t }).getOrElse(Nil)
    retrievedMessages should contain theSameElementsAs generateSampleItems()
  }

  override def getDocument: VirtualDocument[Tuple] = {
    DocumentFactory.openDocument(uri)._1 match {
      case doc: VirtualDocument[Tuple] => doc
      case _                           => fail("Failed to open document as VirtualDocument[Tuple]")
    }
  }
}
