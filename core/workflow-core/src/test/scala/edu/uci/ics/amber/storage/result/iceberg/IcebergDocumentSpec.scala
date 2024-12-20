package edu.uci.ics.amber.storage.result.iceberg

import edu.uci.ics.amber.core.storage.StorageConfig
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
      new Attribute("col-timestamp", AttributeType.TIMESTAMP)
//      new Attribute("col-binary", AttributeType.BINARY)
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
    StorageConfig.jdbcUrl,
    StorageConfig.jdbcUsername,
    StorageConfig.jdbcPassword
  )

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
      catalog,
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
        .build(),
      Tuple
        .builder(amberSchema)
        .add("col-string", AttributeType.STRING, "")
        .add("col-int", AttributeType.INTEGER, -1)
        .add("col-bool", AttributeType.BOOLEAN, false)
        .add("col-long", AttributeType.LONG, -98765432109876L)
        .add("col-double", AttributeType.DOUBLE, -0.001)
        .add("col-timestamp", AttributeType.TIMESTAMP, new Timestamp(0L))
        .build(),
      Tuple
        .builder(amberSchema)
        .add("col-string", AttributeType.STRING, "Special Characters: \n\t\r")
        .add("col-int", AttributeType.INTEGER, Int.MaxValue)
        .add("col-bool", AttributeType.BOOLEAN, true)
        .add("col-long", AttributeType.LONG, Long.MaxValue)
        .add("col-double", AttributeType.DOUBLE, Double.MaxValue)
        .add("col-timestamp", AttributeType.TIMESTAMP, new Timestamp(1234567890L))
        .build()
    )

    // Generate additional tuples with random data
    val additionalTuples = (1 to 20000).map { i =>
      Tuple
        .builder(amberSchema)
        .add("col-string", AttributeType.STRING, s"Generated String $i")
        .add("col-int", AttributeType.INTEGER, i)
        .add("col-bool", AttributeType.BOOLEAN, i % 2 == 0)
        .add("col-long", AttributeType.LONG, i.toLong * 1000000L)
        .add("col-double", AttributeType.DOUBLE, i * 0.12345)
        .add(
          "col-timestamp",
          AttributeType.TIMESTAMP,
          new Timestamp(System.currentTimeMillis() + i * 1000L)
        )
        .build()
    }

    // Combine the base tuples with the generated tuples
    baseTuples ++ additionalTuples
  }
}
