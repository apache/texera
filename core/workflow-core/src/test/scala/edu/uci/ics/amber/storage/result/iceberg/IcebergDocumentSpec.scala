package edu.uci.ics.amber.storage.result.iceberg

import edu.uci.ics.amber.core.storage.{IcebergCatalogInstance, StorageConfig}
import edu.uci.ics.amber.core.storage.model.VirtualDocumentSpec
import edu.uci.ics.amber.core.storage.result.iceberg.IcebergDocument
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import edu.uci.ics.amber.util.IcebergUtil
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.data.Record
import org.apache.iceberg.{Schema => IcebergSchema}
import org.apache.iceberg.catalog.TableIdentifier

import java.sql.Timestamp
import java.util.UUID

class IcebergDocumentSpec extends VirtualDocumentSpec[Tuple] {

  // Define Amber Schema with all possible attribute types
  val amberSchema: Schema = Schema(
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

  // Define Iceberg Schema
  val icebergSchema: IcebergSchema = IcebergUtil.toIcebergSchema(amberSchema)

  // Serialization function: Tuple -> Record
  val serde: Tuple => Record = tuple => IcebergUtil.toGenericRecord(tuple)

  // Deserialization function: Record -> Tuple
  val deserde: (IcebergSchema, Record) => Tuple = (schema, record) =>
    IcebergUtil.fromRecord(record, amberSchema)

  // Create catalog instance
  val catalog: Catalog = IcebergUtil.createJdbcCatalog(
    "iceberg_document_test",
    StorageConfig.fileStorageDirectoryUri,
    StorageConfig.icebergCatalogUrl,
    StorageConfig.icebergCatalogUsername,
    StorageConfig.icebergCatalogPassword
  )

  IcebergCatalogInstance.replaceInstance(catalog)

  val tableNamespace = "test_namespace"
  var tableName: String = _

  override def beforeEach(): Unit = {
    // Generate a unique table name for each test
    tableName = s"test_table_${UUID.randomUUID().toString.replace("-", "")}"
    super.beforeEach()
  }

  // Implementation of getDocument
  override def getDocument: IcebergDocument[Tuple] = {
    new IcebergDocument[Tuple](
      tableNamespace,
      tableName,
      icebergSchema,
      serde,
      deserde
    )
  }

  // Implementation of isDocumentClearedgetSam
  override def isDocumentCleared: Boolean = {
    val identifier = TableIdentifier.of(tableNamespace, tableName)
    !catalog.tableExists(identifier)
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

    // Function to generate random binary data
    def generateRandomBinary(size: Int): Array[Byte] = {
      val array = new Array[Byte](size)
      scala.util.Random.nextBytes(array)
      array
    }

    // Generate additional tuples with random data and occasional nulls
    val additionalTuples = (1 to 6000).map { i =>
      Tuple
        .builder(amberSchema)
        .add(
          "col-string",
          AttributeType.STRING,
          if (i % 7 == 0) null else s"Generated String $i"
        )
        .add(
          "col-int",
          AttributeType.INTEGER,
          if (i % 5 == 0) null else i
        )
        .add(
          "col-bool",
          AttributeType.BOOLEAN,
          if (i % 6 == 0) null else i % 2 == 0
        )
        .add(
          "col-long",
          AttributeType.LONG,
          if (i % 4 == 0) null else i.toLong * 1000000L
        )
        .add(
          "col-double",
          AttributeType.DOUBLE,
          if (i % 3 == 0) null else i * 0.12345
        )
        .add(
          "col-timestamp",
          AttributeType.TIMESTAMP,
          if (i % 8 == 0) null
          else new Timestamp(System.currentTimeMillis() + i * 1000L)
        )
        .add(
          "col-binary",
          AttributeType.BINARY,
          if (i % 9 == 0) null
          else generateRandomBinary(scala.util.Random.nextInt(10) + 1)
        )
        .build()
    }

    // Combine the base tuples with the generated tuples
    baseTuples ++ additionalTuples
  }
}
