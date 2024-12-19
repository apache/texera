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

  // Define Amber Schema
  val amberSchema: Schema = Schema(
    List(
      new Attribute("id", AttributeType.LONG),
      new Attribute("name", AttributeType.STRING),
      new Attribute("score", AttributeType.DOUBLE),
      new Attribute("timestamp", AttributeType.TIMESTAMP)
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

  // Implementation of isDocumentCleared
  override def isDocumentCleared: Boolean = {
    val identifier = TableIdentifier.of(tableNamespace, tableName)
    !catalog.tableExists(identifier)
  }

  // Implementation of generateSampleItems
  override def generateSampleItems(): List[Tuple] = {
    List(
      Tuple
        .builder(amberSchema)
        .add("id", AttributeType.LONG, 1L)
        .add("name", AttributeType.STRING, "Alice")
        .add("score", AttributeType.DOUBLE, 95.5)
        .add("timestamp", AttributeType.TIMESTAMP, new Timestamp(System.currentTimeMillis()))
        .build(),
      Tuple
        .builder(amberSchema)
        .add("id", AttributeType.LONG, 2L)
        .add("name", AttributeType.STRING, "Bob")
        .add("score", AttributeType.DOUBLE, 88.0)
        .add("timestamp", AttributeType.TIMESTAMP, new Timestamp(System.currentTimeMillis()))
        .build()
    )
  }
}
