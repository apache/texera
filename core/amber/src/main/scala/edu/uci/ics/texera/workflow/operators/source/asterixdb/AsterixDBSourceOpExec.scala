package edu.uci.ics.texera.workflow.operators.source.asterixdb

import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.source.SQLSourceOpExec
import scalaj.http.Http

import java.sql._
import scala.collection.Iterator

class AsterixDBSourceOpExec private[asterixdb] (
    schema: Schema,
    host: String,
    port: String,
    database: String,
    table: String,
    username: String,
    password: String,
    limit: Option[Long],
    offset: Option[Long],
    column: Option[String],
    keywords: Option[String],
    progressive: Boolean,
    batchByColumn: Option[String],
    interval: Long
) extends SQLSourceOpExec(
      schema,
      table,
      limit,
      offset,
      column,
      keywords,
      progressive,
      batchByColumn,
      interval
    ) {

  val FETCH_TABLE_NAMES_SQL =
    "SELECT table_name FROM information_schema.tables WHERE table_schema = ?;"

  @throws[SQLException]
  @throws[RuntimeException]
  override def open(): Unit = {
    val sql = "select count(*) from Metadata.`Dataset`;"
    val response = queryAsterixDB(sql)

    print(response)
//    print(objectMapper.readValue(response, classOf[AsterixDBResponse]).results)

  }

  override def produceTexeraTuple(): Iterator[Tuple] = {
    new Iterator[Tuple]() {
      override def hasNext: Boolean = { false }
      override def next(): Tuple = { null }
    }
  }

  /**
    * Establishes the connection to database.
    * @throws SQLException all possible exceptions from JDBC
    * @return a SQL connection over JDBC
    */
  override protected def establishConn(): Connection = ???

  /**
    * Fetch all table names from the given database. This is used to
    * check the input table name to prevent from SQL injection.
    * @throws SQLException all possible exceptions from JDBC
    */
  override protected def loadTableNames(): Unit = ???

  override protected def addKeywordSearch(queryBuilder: StringBuilder): Unit = ???

  private def queryAsterixDB(statement: String): String = {
    val asterixAddress = "http://" + host + ":" + port + "/query/service"

    Http(asterixAddress).postForm
      .param("statement", statement)
      .header("Content-Type", "application/x-www-form-urlencoded")
      .header("Charset", "UTF-8")
      .asString
      .body
  }

}

abstract class AsterixDBResponse() {
  val requestID: String
  val signature: String
  val results: String
  val plans: String
  val status: String
  val metrics: String
}
