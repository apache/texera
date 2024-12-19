package edu.uci.ics.amber.util

import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.amber.core.storage.result.iceberg.fileio.LocalFileIO
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.iceberg.catalog.{Catalog, TableIdentifier}
import org.apache.iceberg.types.Types
import org.apache.iceberg.data.{GenericRecord, Record}
import org.apache.iceberg.jdbc.JdbcCatalog
import org.apache.iceberg.types.Type.PrimitiveType
import org.apache.iceberg.{CatalogProperties, Table, Schema => IcebergSchema}

import java.net.URI
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.ZoneId
import scala.jdk.CollectionConverters._

object IcebergUtil {

  /**
    * Creates and initializes a JdbcCatalog with the given parameters.
    *
    * @param catalogName  The name of the catalog.
    * @param warehouseUri    The warehouse directory path.
    * @param jdbcUri          The JDBC URI for the catalog.
    * @param jdbcUser     The JDBC username.
    * @param jdbcPassword The JDBC password.
    * @return The initialized JdbcCatalog.
    */
  def createJdbcCatalog(
      catalogName: String,
      warehouseUri: URI,
      jdbcUri: String,
      jdbcUser: String,
      jdbcPassword: String
  ): JdbcCatalog = {
    val catalog = new JdbcCatalog()
    catalog.initialize(
      catalogName,
      Map(
        "warehouse" -> warehouseUri.toString,
        "uri" -> jdbcUri,
        "jdbc.user" -> jdbcUser,
        "jdbc.password" -> jdbcPassword,
        CatalogProperties.FILE_IO_IMPL -> classOf[LocalFileIO].getName
      ).asJava
    )
    catalog
  }

  def loadOrCreateTable(
      catalog: Catalog,
      tableNamespace: String,
      tableName: String,
      tableSchema: IcebergSchema
  ): Table = {
    val identifier = TableIdentifier.of(tableNamespace, tableName)
    if (!catalog.tableExists(identifier)) {
      catalog.createTable(identifier, tableSchema)
    } else {
      catalog.loadTable(identifier)
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
        Types.NestedField.required(index + 1, attribute.getName, toIcebergType(attribute.getType))
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
    * Converts a custom Amber `Tuple` to an Iceberg `GenericRecord`.
    *
    * @param tuple The custom Amber Tuple.
    * @return An Iceberg GenericRecord.
    */
  def toGenericRecord(tuple: Tuple): Record = {
    // Convert the Amber schema to an Iceberg schema
    val icebergSchema = toIcebergSchema(tuple.schema)
    val record = GenericRecord.create(icebergSchema)

    tuple.schema.getAttributes.zipWithIndex.foreach {
      case (attribute, index) =>
        val value = tuple.getField[AnyRef](index) match {
          case ts: Timestamp => ts.toInstant.atZone(ZoneId.systemDefault()).toLocalDateTime
          case other         => other
        }
        record.setField(attribute.getName, value)
    }

    record
  }

  /**
    * Converts an Iceberg `Record` to an Amber `Tuple`.
    *
    * @param record The Iceberg Record.
    * @param amberSchema The corresponding Amber Schema.
    * @return An Amber Tuple.
    */
  def fromRecord(record: Record, amberSchema: Schema): Tuple = {
    val fieldValues = amberSchema.getAttributes.map { attribute =>
      val value = record.getField(attribute.getName) match {
        case ldt: LocalDateTime => Timestamp.valueOf(ldt)
        case other              => other
      }
      value
    }

    Tuple(amberSchema, fieldValues.toArray)
  }

  /**
    * Converts an Iceberg `Schema` to a custom Amber `Schema`.
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
    * Converts an Iceberg `Type` to a custom Amber `AttributeType`.
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
}
