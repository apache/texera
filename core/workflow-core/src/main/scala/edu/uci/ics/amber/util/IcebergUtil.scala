package edu.uci.ics.amber.util

import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.catalog.{Catalog, TableIdentifier}
import org.apache.iceberg.data.parquet.GenericParquetReaders
import org.apache.iceberg.types.Types
import org.apache.iceberg.data.{GenericRecord, Record}
import org.apache.iceberg.hadoop.{HadoopCatalog, HadoopFileIO}
import org.apache.iceberg.io.{CloseableIterable, InputFile}
import org.apache.iceberg.parquet.{Parquet, ParquetValueReader}
import org.apache.iceberg.types.Type.PrimitiveType
import org.apache.iceberg.{
  CatalogProperties,
  DataFile,
  PartitionSpec,
  Table,
  TableProperties,
  Schema => IcebergSchema
}

import java.nio.ByteBuffer
import java.nio.file.Path
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.ZoneId
import scala.jdk.CollectionConverters._

/**
  * Util functions to interact with Iceberg Tables
  */
object IcebergUtil {

  /**
    * Creates and initializes a HadoopCatalog with the given parameters.
    * - Uses an empty Hadoop `Configuration`, meaning the local file system (or `file:/`) will be used by default
    * instead of HDFS.
    * - The `warehouse` parameter specifies the root directory for storing table data.
    * - Sets the file I/O implementation to `HadoopFileIO`.
    *
    * @param catalogName the name of the catalog.
    * @param warehouse   the root path for the warehouse where the tables are stored.
    * @return the initialized HadoopCatalog instance.
    */
  def createHadoopCatalog(
      catalogName: String,
      warehouse: Path
  ): HadoopCatalog = {
    val catalog = new HadoopCatalog()
    catalog.setConf(new Configuration) // Empty configuration, defaults to `file:/`
    catalog.initialize(
      catalogName,
      Map(
        "warehouse" -> warehouse.toString,
        CatalogProperties.FILE_IO_IMPL -> classOf[HadoopFileIO].getName
      ).asJava
    )

    catalog
  }

  /**
    * Creates a new Iceberg table with the specified schema and properties.
    * - Drops the existing table if `overrideIfExists` is true and the table already exists.
    * - Creates an unpartitioned table with custom commit retry properties.
    *
    * @param catalog the Iceberg catalog to manage the table.
    * @param tableNamespace the namespace of the table.
    * @param tableName the name of the table.
    * @param tableSchema the schema of the table.
    * @param overrideIfExists whether to drop and recreate the table if it exists.
    * @return the created Iceberg table.
    */
  def createTable(
      catalog: Catalog,
      tableNamespace: String,
      tableName: String,
      tableSchema: IcebergSchema,
      overrideIfExists: Boolean
  ): Table = {
    val tableProperties = Map(
      TableProperties.COMMIT_NUM_RETRIES -> StorageConfig.icebergTableCommitNumRetries.toString,
      TableProperties.COMMIT_MAX_RETRY_WAIT_MS -> StorageConfig.icebergTableCommitMaxRetryWaitMs.toString,
      TableProperties.COMMIT_MIN_RETRY_WAIT_MS -> StorageConfig.icebergTableCommitMinRetryWaitMs.toString
    )
    val identifier = TableIdentifier.of(tableNamespace, tableName)
    if (catalog.tableExists(identifier) && overrideIfExists) {
      catalog.dropTable(identifier)
    }
    catalog.createTable(
      identifier,
      tableSchema,
      PartitionSpec.unpartitioned,
      tableProperties.asJava
    )
  }

  /**
    * Loads metadata for an existing Iceberg table.
    * - Returns `Some(Table)` if the table exists and is successfully loaded.
    * - Returns `None` if the table does not exist or cannot be loaded.
    *
    * @param catalog the Iceberg catalog to load the table from.
    * @param tableNamespace the namespace of the table.
    * @param tableName the name of the table.
    * @return an Option containing the table, or None if not found.
    */
  def loadTableMetadata(
      catalog: Catalog,
      tableNamespace: String,
      tableName: String
  ): Option[Table] = {
    val identifier = TableIdentifier.of(tableNamespace, tableName)
    try {
      Some(catalog.loadTable(identifier))
    } catch {
      case _: Exception => None
    }
  }

  /**
    * Converts a custom Amber `Schema` to an Iceberg `Schema`.
    *
    * @param amberSchema The custom Amber Schema.
    * @return An Iceberg Schema.
    */
  def toIcebergSchema(amberSchema: Schema): IcebergSchema = {
    val icebergFields = amberSchema.getAttributes.zipWithIndex.map {
      case (attribute, index) =>
        Types.NestedField.optional(index + 1, attribute.getName, toIcebergType(attribute.getType))
    }
    new IcebergSchema(icebergFields.asJava)
  }

  /**
    * Converts a custom Amber `AttributeType` to an Iceberg `Type`.
    *
    * @param attributeType The custom Amber AttributeType.
    * @return The corresponding Iceberg Type.
    */
  def toIcebergType(attributeType: AttributeType): PrimitiveType = {
    attributeType match {
      case AttributeType.STRING    => Types.StringType.get()
      case AttributeType.INTEGER   => Types.IntegerType.get()
      case AttributeType.LONG      => Types.LongType.get()
      case AttributeType.DOUBLE    => Types.DoubleType.get()
      case AttributeType.BOOLEAN   => Types.BooleanType.get()
      case AttributeType.TIMESTAMP => Types.TimestampType.withoutZone()
      case AttributeType.BINARY    => Types.BinaryType.get()
      case AttributeType.ANY =>
        throw new IllegalArgumentException("ANY type is not supported in Iceberg")
    }
  }

  /**
    * Converts a custom Amber `Tuple` to an Iceberg `GenericRecord`, handling `null` values.
    *
    * @param tuple The custom Amber Tuple.
    * @return An Iceberg GenericRecord.
    */
  def toGenericRecord(icebergSchema: IcebergSchema, tuple: Tuple): Record = {
    val record = GenericRecord.create(icebergSchema)

    tuple.schema.getAttributes.zipWithIndex.foreach {
      case (attribute, index) =>
        val value = tuple.getField[AnyRef](index) match {
          case null               => null
          case ts: Timestamp      => ts.toInstant.atZone(ZoneId.systemDefault()).toLocalDateTime
          case bytes: Array[Byte] => ByteBuffer.wrap(bytes)
          case other              => other
        }
        record.setField(attribute.getName, value)
    }

    record
  }

  /**
    * Converts an Iceberg `Record` to an Amber `Tuple`
    *
    * @param record      The Iceberg Record.
    * @param amberSchema The corresponding Amber Schema.
    * @return An Amber Tuple.
    */
  def fromRecord(record: Record, amberSchema: Schema): Tuple = {
    val fieldValues = amberSchema.getAttributes.map { attribute =>
      val value = record.getField(attribute.getName) match {
        case null               => null
        case ldt: LocalDateTime => Timestamp.valueOf(ldt)
        case buffer: ByteBuffer =>
          val bytes = new Array[Byte](buffer.remaining())
          buffer.get(bytes)
          bytes
        case other => other
      }
      value
    }

    Tuple(amberSchema, fieldValues.toArray)
  }

  /**
    * Converts an Iceberg `Schema` to an Amber `Schema`.
    *
    * @param icebergSchema The Iceberg Schema.
    * @return The corresponding Amber Schema.
    */
  def fromIcebergSchema(icebergSchema: IcebergSchema): Schema = {
    val attributes = icebergSchema
      .columns()
      .asScala
      .map { field =>
        new Attribute(field.name(), fromIcebergType(field.`type`().asPrimitiveType()))
      }
      .toList

    Schema(attributes)
  }

  /**
    * Converts an Iceberg `Type` to an Amber `AttributeType`.
    *
    * @param icebergType The Iceberg Type.
    * @return The corresponding Amber AttributeType.
    */
  def fromIcebergType(icebergType: PrimitiveType): AttributeType = {
    icebergType match {
      case _: Types.StringType    => AttributeType.STRING
      case _: Types.IntegerType   => AttributeType.INTEGER
      case _: Types.LongType      => AttributeType.LONG
      case _: Types.DoubleType    => AttributeType.DOUBLE
      case _: Types.BooleanType   => AttributeType.BOOLEAN
      case _: Types.TimestampType => AttributeType.TIMESTAMP
      case _: Types.BinaryType    => AttributeType.BINARY
      case _                      => throw new IllegalArgumentException(s"Unsupported Iceberg type: $icebergType")
    }
  }

  /**
    * Util function to create a Record iterator over the given DataFile in Iceberg
    * @param dataFile the data file
    * @param schema the schema of the table
    * @param table the iceberg table
    * @return an iterator over the records in the data file
    */
  def readDataFileAsIterator(
      dataFile: DataFile,
      schema: IcebergSchema,
      table: Table
  ): Iterator[Record] = {
    val inputFile: InputFile = table.io().newInputFile(dataFile)
    val readerFunc
        : java.util.function.Function[org.apache.parquet.schema.MessageType, ParquetValueReader[
          _
        ]] =
      (messageType: org.apache.parquet.schema.MessageType) =>
        GenericParquetReaders.buildReader(schema, messageType)
    val closeableIterable: CloseableIterable[Record] =
      Parquet
        .read(inputFile)
        .project(schema)
        .createReaderFunc(readerFunc)
        .build()
    closeableIterable.iterator().asScala
  }

}
