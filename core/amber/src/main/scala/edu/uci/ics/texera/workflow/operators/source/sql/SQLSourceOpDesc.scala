package edu.uci.ics.texera.workflow.operators.source.sql

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.texera.workflow.common.metadata.annotations.{
  AutofillAttributeName,
  EnablePresets,
  UIWidget
}
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}

import java.sql._

abstract class SQLSourceOpDesc extends SourceOperatorDescriptor {

  @EnablePresets
  @JsonProperty(required = true)
  @JsonSchemaTitle("Host")
  var host: String = _

  @EnablePresets
  @JsonProperty(required = true, defaultValue = "default")
  @JsonSchemaTitle("Port")
  @JsonPropertyDescription("A port number or 'default'")
  var port: String = _

  @EnablePresets
  @JsonProperty(required = true)
  @JsonSchemaTitle("Database")
  var database: String = _

  @EnablePresets
  @JsonProperty(required = true)
  @JsonSchemaTitle("Table Name")
  var table: String = _

  @EnablePresets
  @JsonProperty(required = true)
  @JsonSchemaTitle("Username")
  var username: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Password")
  @JsonSchemaInject(json = UIWidget.UIWidgetPassword)
  var password: String = _

  @JsonProperty()
  @JsonSchemaTitle("Limit")
  @JsonPropertyDescription("max output count")
  @JsonDeserialize(contentAs = classOf[java.lang.Long])
  var limit: Option[Long] = None

  @JsonProperty()
  @JsonSchemaTitle("Offset")
  @JsonPropertyDescription("starting point of output")
  @JsonDeserialize(contentAs = classOf[java.lang.Long])
  var offset: Option[Long] = None

  @JsonProperty(defaultValue = "false")
  @JsonSchemaTitle("Keyword Search?")
  @JsonDeserialize(contentAs = classOf[java.lang.Boolean])
  @JsonSchemaInject(json = """{"toggleHidden" : ["keywordSearchByColumn", "keywords"]}""")
  var keywordSearch: Option[Boolean] = Option(false)

  @JsonProperty()
  @JsonSchemaTitle("Keyword Search Column")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  @AutofillAttributeName
  var keywordSearchByColumn: Option[String] = None

  @JsonProperty()
  @JsonSchemaTitle("Keywords to Search")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  @JsonSchemaInject(json = UIWidget.UIWidgetTextArea)
  var keywords: Option[String] = None

  @JsonProperty(defaultValue = "false")
  @JsonSchemaTitle("Progressive?")
  @JsonDeserialize(contentAs = classOf[java.lang.Boolean])
  @JsonSchemaInject(json = """{"toggleHidden" : ["batchByColumn", "min", "max", "interval"]}""")
  var progressive: Option[Boolean] = Option(false)

  @JsonProperty()
  @JsonSchemaTitle("Batch by Column")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  @AutofillAttributeName
  var batchByColumn: Option[String] = None

  @JsonProperty(defaultValue = "auto")
  @JsonSchemaTitle("Min")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  @JsonSchemaInject(json = """{"dependOn" : "batchByColumn"}""")
  var min: Option[String] = None

  @JsonProperty(defaultValue = "auto")
  @JsonSchemaTitle("Max")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  @JsonSchemaInject(json = """{"dependOn" : "batchByColumn"}""")
  var max: Option[String] = None

  @JsonProperty(defaultValue = "1000000000")
  @JsonSchemaTitle("Batch by Interval")
  @JsonSchemaInject(json = """{"dependOn" : "batchByColumn"}""")
  var interval = 0L

  /**
    * Make sure all the required parameters are not empty,
    * then query the remote PostgreSQL server for the table schema
    *
    * @return Texera.Tuple.Schema
    */
  override def sourceSchema(): Schema = {
    if (
      this.host == null || this.port == null || this.database == null
      || this.table == null || this.username == null || this.password == null
    )
      return null
    querySchema
  }

  // needs to define getters for sub classes to override Jackson Annotations
  def getKeywords: Option[String] = keywords

  /**
    * Establish a connection with the database server base on the info provided by the user
    * query the MetaData of the table and generate a Texera.tuple.schema accordingly
    * the "switch" code block shows how SQL data types are mapped to Texera AttributeTypes
    *
    * @return Schema
    */
  protected def querySchema: Schema = {
    updatePort()
    val schemaBuilder = Schema.newBuilder
    try {
      val connection = establishConn
      connection.setReadOnly(true)
      val databaseMetaData = connection.getMetaData
      val columns = databaseMetaData.getColumns(null, null, this.table, null)
      while ({
        columns.next
      }) {
        val columnName = columns.getString("COLUMN_NAME")
        val datatype = columns.getInt("DATA_TYPE")
        datatype match {
          case Types.TINYINT | // -6 Types.TINYINT
              Types.SMALLINT | // 5 Types.SMALLINT
              Types.INTEGER => // 4 Types.INTEGER
            schemaBuilder.add(new Attribute(columnName, AttributeType.INTEGER))
          case Types.FLOAT | // 6 Types.FLOAT
              Types.REAL | // 7 Types.REAL
              Types.DOUBLE | // 8 Types.DOUBLE
              Types.NUMERIC => // 3 Types.NUMERIC
            schemaBuilder.add(new Attribute(columnName, AttributeType.DOUBLE))
          case Types.BIT | // -7 Types.BIT
              Types.BOOLEAN => // 16 Types.BOOLEAN
            schemaBuilder.add(new Attribute(columnName, AttributeType.BOOLEAN))
          case Types.BINARY => //-2 Types.BINARY
            schemaBuilder.add(new Attribute(columnName, AttributeType.BINARY))
          case Types.DATE | //91 Types.DATE
              Types.TIME | //92 Types.TIME
              Types.LONGVARCHAR | //-1 Types.LONGVARCHAR
              Types.CHAR | //1 Types.CHAR
              Types.VARCHAR | //12 Types.VARCHAR
              Types.NULL | //0 Types.NULL
              Types.OTHER => //1111 Types.OTHER
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
  protected def establishConn: Connection = null

  protected def updatePort(): Unit
}
