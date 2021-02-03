package edu.uci.ics.texera.workflow.operators.source

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}

import java.sql._

abstract class SQLSourceOpDesc extends SourceOperatorDescriptor {
  @JsonProperty(value = "host", required = true)
  @JsonPropertyDescription("host IP address")
  var host: String = _

  @JsonProperty(value = "port", required = true, defaultValue = "default")
  @JsonPropertyDescription("port")
  var port: String = _

  @JsonProperty(value = "database", required = true)
  @JsonPropertyDescription("database name")
  var database: String = _

  @JsonProperty(value = "table", required = true)
  @JsonPropertyDescription("table name")
  var table: String = _

  @JsonProperty(value = "username", required = true)
  @JsonPropertyDescription("username")
  var username: String = _

  @JsonProperty(value = "password", required = true)
  @JsonPropertyDescription("password")
  var password: String = _

  @JsonProperty(value = "limit")
  @JsonPropertyDescription("query result count upper limit")
  @JsonDeserialize(contentAs = classOf[java.lang.Long])
  var limit: Option[Long] = None

  @JsonProperty(value = "offset")
  @JsonPropertyDescription("query offset")
  @JsonDeserialize(contentAs = classOf[java.lang.Long])
  var offset: Option[Long] = None

  @JsonProperty(value = "column name")
  @JsonPropertyDescription("the column to be keyword-searched")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  @AutofillAttributeName
  var column: Option[String] = None

  @JsonProperty(value = "keywords")
  @JsonPropertyDescription("search terms in boolean expression")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  var keywords: Option[String] = None

  @JsonProperty(value = "progressive", defaultValue = "false")
  @JsonPropertyDescription("progressively yield outputs")
  var progressive: Boolean = false

  @JsonProperty(value = "batch by column")
  @JsonPropertyDescription("batch by column")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  @AutofillAttributeName
  var batchByColumn: Option[String] = None

  @JsonProperty(value = "batch by interval", defaultValue = "1000000000")
  @JsonPropertyDescription("batch by interval")
  var interval = 0L

  /**
    * Make sure all the required parameters are not empty,
    * then query the remote PostgreSQL server for the table schema
    *
    * @return Texera.Tuple.Schema
    */
  override def sourceSchema: Schema = {
    if (
      this.host == null || this.port == null || this.database == null || this.table == null || this.username == null || this.password == null
    ) return null
    querySchema
  }

  /**
    * Establish a connection with the database server base on the info provided by the user
    * query the MetaData of the table and generate a Texera.tuple.schema accordingly
    * the "switch" code block shows how SQL data types are mapped to Texera AttributeTypes
    *
    * @return Schema
    */
  protected def querySchema = {
    updatePort()
    val schemaBuilder = Schema.newBuilder
    try {
      val connection = establishConn
      connection.setReadOnly(true)
      val databaseMetaData = connection.getMetaData
      val columns = databaseMetaData.getColumns(null, null, this.table, null)
      while ({ columns.next }) {
        val columnName = columns.getString("COLUMN_NAME")
        val datatype = columns.getInt("DATA_TYPE")
        datatype match {
          case Types.BIT      => // -7 Types.BIT
          case Types.TINYINT  => // -6 Types.TINYINT
          case Types.SMALLINT => // 5 Types.SMALLINT
          case Types.INTEGER => // 4 Types.INTEGER
            schemaBuilder.add(new Attribute(columnName, AttributeType.INTEGER))
          case Types.FLOAT  => // 6 Types.FLOAT
          case Types.REAL   => // 7 Types.REAL
          case Types.DOUBLE => // 8 Types.DOUBLE
          case Types.NUMERIC => // 3 Types.NUMERIC
            schemaBuilder.add(new Attribute(columnName, AttributeType.DOUBLE))
          case Types.BOOLEAN => // 16 Types.BOOLEAN
            schemaBuilder.add(new Attribute(columnName, AttributeType.BOOLEAN))
          case Types.BINARY      => //-2 Types.BINARY
          case Types.DATE        => //91 Types.DATE
          case Types.TIME        => //92 Types.TIME
          case Types.LONGVARCHAR => //-1 Types.LONGVARCHAR
          case Types.CHAR        => //1 Types.CHAR
          case Types.VARCHAR     => //12 Types.VARCHAR
          case Types.NULL        => //0 Types.NULL
          case Types.OTHER => //1111 Types.OTHER
            schemaBuilder.add(new Attribute(columnName, AttributeType.STRING))
          case Types.BIGINT => //-5 Types.BIGINT
            schemaBuilder.add(new Attribute(columnName, AttributeType.LONG))
          case Types.TIMESTAMP => // 93 Types.TIMESTAMP
            schemaBuilder.add(new Attribute(columnName, AttributeType.TIMESTAMP))
          case _ =>
            throw new RuntimeException(
              this.getClass.getSimpleName + ": unknown data type: " + datatype
            )
        }
      }
      connection.close()
      schemaBuilder.build
    } catch {
      case e @ (_: SQLException | _: ClassCastException) =>
        e.printStackTrace()
        throw new RuntimeException(
          this.getClass.getSimpleName + " failed to connect to the database. " + e.getMessage
        )
    }
  }

  @throws[SQLException]
  protected def establishConn: Connection

  protected def updatePort()
}
