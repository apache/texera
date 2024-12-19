package edu.uci.ics.amber.storage.result.iceberg

import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.amber.core.storage.result.iceberg.IcebergDocument
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import edu.uci.ics.amber.util.IcebergUtil
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.data.Record
import org.apache.iceberg.types.Types
import org.apache.iceberg.{Schema => IcebergSchema}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfterEach

import java.sql.Timestamp
import java.util.UUID

class IcebergDocumentSpec extends AnyFlatSpec with BeforeAndAfterEach {

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

  // Table name (unique for each test)
  var tableName: String = _

  var tableCatalog: Catalog = IcebergUtil.createJdbcCatalog(
    "iceberg document test",
    StorageConfig.fileStorageDirectoryUri,
    StorageConfig.jdbcUrl,
    StorageConfig.jdbcUsername,
    StorageConfig.jdbcPassword
  )

  val tableNamespace = "test"

  // IcebergDocument instance
  var icebergDocument: IcebergDocument[Tuple] = _

  override def beforeEach(): Unit = {
    // Generate a unique table name for each test
    tableName = s"test_table_${UUID.randomUUID().toString.replace("-", "")}"

    // Initialize IcebergDocument
    icebergDocument = new IcebergDocument[Tuple](
      tableCatalog,
      tableNamespace,
      tableName,
      icebergSchema,
      serde,
      deserde
    )
  }

  "IcebergDocument" should "write and read tuples successfully" in {
    val tuple1 = Tuple
      .builder(amberSchema)
      .add("id", AttributeType.LONG, 1L)
      .add("name", AttributeType.STRING, "Alice")
      .add("score", AttributeType.DOUBLE, 95.5)
      .add("timestamp", AttributeType.TIMESTAMP, new Timestamp(System.currentTimeMillis()))
      .build()

    val tuple2 = Tuple
      .builder(amberSchema)
      .add("id", AttributeType.LONG, 2L)
      .add("name", AttributeType.STRING, "Bob")
      .add("score", AttributeType.DOUBLE, 88.0)
      .add("timestamp", AttributeType.TIMESTAMP, new Timestamp(System.currentTimeMillis()))
      .build()

    val tuple3 = Tuple
      .builder(amberSchema)
      .add("id", AttributeType.LONG, 3L)
      .add("name", AttributeType.STRING, "John")
      .add("score", AttributeType.DOUBLE, 75.5)
      .add("timestamp", AttributeType.TIMESTAMP, new Timestamp(System.currentTimeMillis()))
      .build()

    val tuple4 = Tuple
      .builder(amberSchema)
      .add("id", AttributeType.LONG, 4L)
      .add("name", AttributeType.STRING, "Bob")
      .add("score", AttributeType.DOUBLE, 80.0)
      .add("timestamp", AttributeType.TIMESTAMP, new Timestamp(System.currentTimeMillis()))
      .build()

    // Get writer and write tuples
    var writer = icebergDocument.writer()
    writer.open()
    writer.putOne(tuple1)
    writer.putOne(tuple2)
    writer.close()

    // Read tuples back
    var retrievedTuples = icebergDocument.get().toList

    assert(retrievedTuples.contains(tuple1))
    assert(retrievedTuples.contains(tuple2))
    assert(retrievedTuples.size == 2)

    // Get writer and write tuples
    writer = icebergDocument.writer()
    writer.open()
    writer.putOne(tuple3)
    writer.putOne(tuple4)
    writer.close()

    retrievedTuples = icebergDocument.get().toList

    assert(retrievedTuples.contains(tuple1))
    assert(retrievedTuples.contains(tuple2))
    assert(retrievedTuples.contains(tuple3))
    assert(retrievedTuples.contains(tuple4))
    assert(retrievedTuples.size == 4)

    icebergDocument.clear()
  }

  it should "handle empty reads gracefully" in {
    val retrievedTuples = icebergDocument.get().toList
    assert(retrievedTuples.isEmpty)
  }
}
