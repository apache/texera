package edu.uci.ics.amber.storage.result.iceberg

import edu.uci.ics.amber.core.storage.{
  DocumentFactory,
  IcebergCatalogInstance,
  StorageConfig,
  VFSURIFactory
}
import edu.uci.ics.amber.core.storage.model.{VirtualDocument, VirtualDocumentSpec}
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import edu.uci.ics.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  WorkflowIdentity
}
import edu.uci.ics.amber.core.workflow.PortIdentity
import edu.uci.ics.amber.util.IcebergUtil
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.data.Record
import org.apache.iceberg.{Schema => IcebergSchema}
import org.scalatest.BeforeAndAfterAll

import java.net.URI
import java.sql.Timestamp
import java.util.UUID

class IcebergDocumentSpec extends VirtualDocumentSpec[Tuple] with BeforeAndAfterAll {

  var amberSchema: Schema = _
  var icebergSchema: IcebergSchema = _
  var serde: (IcebergSchema, Tuple) => Record = _
  var deserde: (IcebergSchema, Record) => Tuple = _
  var catalog: Catalog = _
  val tableNamespace = "test_namespace"
  var uri: URI = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Initialize Amber Schema with all possible attribute types
    amberSchema = Schema(
      List(
        new Attribute("col-string", AttributeType.STRING),
        new Attribute("col-int", AttributeType.INTEGER),
        new Attribute("col-bool", AttributeType.BOOLEAN),
        new Attribute("col-long", AttributeType.LONG),
        new Attribute("col-double", AttributeType.DOUBLE),
        new Attribute("col-timestamp", AttributeType.TIMESTAMP),
        new Attribute("col-binary", AttributeType.BINARY)
      )
    )

    // Initialize Iceberg Schema
    icebergSchema = IcebergUtil.toIcebergSchema(amberSchema)

    // Initialize serialization and deserialization functions
    serde = IcebergUtil.toGenericRecord
    deserde = (schema, record) => IcebergUtil.fromRecord(record, amberSchema)

    // Initialize the the Iceberg catalog
    catalog = IcebergUtil.createHadoopCatalog(
      "iceberg_document_test",
      StorageConfig.fileStorageDirectoryPath
    )
    IcebergCatalogInstance.replaceInstance(catalog)
  }

  override def beforeEach(): Unit = {
    // Generate a unique table name for each test
    uri = VFSURIFactory.createResultURI(
      WorkflowIdentity(0),
      ExecutionIdentity(0),
      OperatorIdentity(s"test_table_${UUID.randomUUID().toString.replace("-", "")}"),
      PortIdentity()
    )
    DocumentFactory.createDocument(uri, amberSchema)
    super.beforeEach()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  override def getDocument: VirtualDocument[Tuple] = {
    DocumentFactory.openDocument(uri)._1.asInstanceOf[VirtualDocument[Tuple]]
  }

  override def generateSampleItems(): List[Tuple] = {
    val baseTuples = List(
      Tuple
        .builder(amberSchema)
        .add("col-string", AttributeType.STRING, "Hello World")
        .add("col-int", AttributeType.INTEGER, 42)
        .add("col-bool", AttributeType.BOOLEAN, true)
        .add("col-long", AttributeType.LONG, 12345678901234L)
        .add("col-double", AttributeType.DOUBLE, 3.14159)
        .add("col-timestamp", AttributeType.TIMESTAMP, new Timestamp(System.currentTimeMillis()))
        .add("col-binary", AttributeType.BINARY, Array[Byte](0, 1, 2, 3, 4, 5, 6, 7))
        .build(),
      Tuple
        .builder(amberSchema)
        .add("col-string", AttributeType.STRING, "")
        .add("col-int", AttributeType.INTEGER, -1)
        .add("col-bool", AttributeType.BOOLEAN, false)
        .add("col-long", AttributeType.LONG, -98765432109876L)
        .add("col-double", AttributeType.DOUBLE, -0.001)
        .add("col-timestamp", AttributeType.TIMESTAMP, new Timestamp(0L))
        .add("col-binary", AttributeType.BINARY, Array[Byte](127, -128, 0, 64))
        .build(),
      Tuple
        .builder(amberSchema)
        .add("col-string", AttributeType.STRING, "Special Characters: \n\t\r")
        .add("col-int", AttributeType.INTEGER, Int.MaxValue)
        .add("col-bool", AttributeType.BOOLEAN, true)
        .add("col-long", AttributeType.LONG, Long.MaxValue)
        .add("col-double", AttributeType.DOUBLE, Double.MaxValue)
        .add("col-timestamp", AttributeType.TIMESTAMP, new Timestamp(1234567890L))
        .add("col-binary", AttributeType.BINARY, Array[Byte](1, 2, 3, 4, 5))
        .build()
    )

    def generateRandomBinary(size: Int): Array[Byte] = {
      val array = new Array[Byte](size)
      scala.util.Random.nextBytes(array)
      array
    }

    val additionalTuples = (1 to 20000).map { i =>
      Tuple
        .builder(amberSchema)
        .add("col-string", AttributeType.STRING, if (i % 7 == 0) null else s"Generated String $i")
        .add("col-int", AttributeType.INTEGER, if (i % 5 == 0) null else i)
        .add("col-bool", AttributeType.BOOLEAN, if (i % 6 == 0) null else i % 2 == 0)
        .add("col-long", AttributeType.LONG, if (i % 4 == 0) null else i.toLong * 1000000L)
        .add("col-double", AttributeType.DOUBLE, if (i % 3 == 0) null else i * 0.12345)
        .add(
          "col-timestamp",
          AttributeType.TIMESTAMP,
          if (i % 8 == 0) null
          else new Timestamp(System.currentTimeMillis() + i * 1000L)
        )
        .add("col-binary", AttributeType.BINARY, if (i % 9 == 0) null else generateRandomBinary(10))
        .build()
    }

    baseTuples ++ additionalTuples
  }
}
