package edu.uci.ics.texera.workflow.operators.source.asterixdb

import com.fasterxml.jackson.databind.JsonNode
import com.github.tototoshi.csv.CSVParser
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, Schema}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType._
import edu.uci.ics.texera.workflow.operators.source.asterixdb.AsterixDBConnUtil.queryAsterixDB

import java.sql._
import java.util
import scala.collection.Iterator
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

class AsterixDBSourceOpExec private[asterixdb] (
    schema: Schema,
    host: String,
    port: String,
    database: String,
    table: String,
    username: String,
    password: String,
    var curLimit: Option[Long],
    var curOffset: Option[Long],
    column: Option[String],
    keywords: Option[String],
    progressive: Boolean,
    batchByColumn: Option[String],
    interval: Long
) extends SourceOperatorExecutor {

  val FETCH_TABLE_NAMES_SQL =
    "SELECT table_name FROM information_schema.tables WHERE table_schema = ?;"

  // connection and query related
  val tableNames: ArrayBuffer[String] = ArrayBuffer()
  val batchByAttribute: Option[Attribute] =
    if (progressive) Option(schema.getAttribute(batchByColumn.get)) else None
  var connection: Connection = _
  var curQuery: Option[String] = None
  var curResultSet: Option[util.Iterator[JsonNode]] = None
  var curLowerBound: Number = _
  var upperBound: Number = _
  var cachedTuple: Option[Tuple] = None
  var querySent: Boolean = false

  @throws[RuntimeException]
  override def open(): Unit = {

    // fetch for all tables, it is also equivalent to a health check
    val tables = queryAsterixDB(host, port, "select `DatasetName` from Metadata.`Dataset`;")
    tables.get.forEachRemaining(table => {
      tableNames.append(table.toString.stripPrefix("\"\\\"").stripSuffix("\\\"\\r\\n\""))
    })

  }

  override def produceTexeraTuple(): Iterator[Tuple] = {
    new Iterator[Tuple]() {
      override def hasNext: Boolean = {

        cachedTuple match {
          // if existing Tuple in cache, means there exist next Tuple.
          case Some(_) => true
          case None    =>
            // cache the next Tuple
            cachedTuple = Option(next())
            cachedTuple.isDefined
        }
      }
      override def next(): Tuple = {
        // if has the next Tuple in cache, return it and clear the cache
        cachedTuple.foreach(tuple => {
          cachedTuple = None
          return tuple
        })

        // otherwise, send query to fetch for the next Tuple
        curResultSet match {
          case Some(resultSet) =>
            if (resultSet.hasNext) {

              // manually skip until the offset position in order to adapt to progressive batches
              curOffset.fold()(offset => {
                if (offset > 0) {
                  curOffset = Option(offset - 1)
                  return next()
                }
              })

              // construct Texera.Tuple from the next result.
              val tuple = buildTupleFromRow

              if (tuple == null)
                return next()

              // update the limit in order to adapt to progressive batches
              curLimit.fold()(limit => {
                if (limit > 0) {
                  curLimit = Option(limit - 1)
                }
              })
              tuple
            } else {
              // close the current resultSet and query
              curResultSet = None
              curQuery = None
              next()
            }
          case None =>
            curQuery = getNextQuery
            curQuery match {
              case Some(query) =>
                curResultSet = queryAsterixDB(host, port, query)
                next()
              case None =>
                curResultSet = None
                null
            }
        }

      }
    }
  }

  override def close(): Unit = {
    curResultSet = None
    curQuery = None
  }

  /**
    * Build a Texera.Tuple from a row of curResultSet
    *
    * @return the new Texera.Tuple
    */
  @throws[SQLException]
  private def buildTupleFromRow: Tuple = {

    val tupleBuilder = Tuple.newBuilder
    val row = curResultSet.get.next().textValue()
    var values: Option[List[String]] = None

    // FIXME: this parser would result some error rows, which has to be skipped.
    try values = CSVParser.parse(row, '\\', ',', '"')
    catch {
      case _: Exception => return null
    }

    for (i <- 0 until schema.getAttributes.size()) {

      val attr = schema.getAttributes.get(i)
      breakable {
        val columnType = attr.getType

        var value: String = null
        try value = values.get(i)
        catch {
          case _: Throwable =>
        }

        if (value == null) {
          // add the field as null
          tupleBuilder.add(attr, null)
          break
        }

        // otherwise, transform the type of the value
        columnType match {
          case INTEGER =>
            tupleBuilder.add(attr, value.toInt)
          case LONG =>
            tupleBuilder.add(attr, value.toLong)
          case DOUBLE =>
            tupleBuilder.add(attr, value.toDouble)
          case STRING =>
            tupleBuilder.add(attr, value)
          case BOOLEAN =>
            tupleBuilder.add(attr, !value.equals("0"))
          case TIMESTAMP =>
            tupleBuilder.add(attr, Timestamp.valueOf(value))
          case ANY | _ =>
            throw new RuntimeException("Unhandled attribute type: " + columnType)
        }
      }
    }
    tupleBuilder.build
  }

  /**
    * Get the next query.
    * - If progressive mode is enabled, this method will be invoked
    * many times, each yielding the next mini query.
    * - If progressive mode is not enabled, this method will be invoked
    * only once, returning the one giant query.
    * @throws SQLException all possible exceptions from JDBC
    * @return a PreparedStatement to be filled with values.
    */
  @throws[SQLException]
  private def getNextQuery: Option[String] = {
    if (hasNextQuery) {

      val queryBuilder = new StringBuilder

      // Add base SELECT * with true condition
      // TODO: add more selection conditions, including alias
      addBaseSelect(queryBuilder)

      // add limit if provided
      if (curLimit.isDefined) {
        if (curLimit.get > 0) addLimit(queryBuilder)
        else
          // there should be no more queries as limit is equal or less than 0
          return None
      }

      // add fixed offset if not progressive
      if (!progressive && curOffset.isDefined) addOffset(queryBuilder)

      Option(queryBuilder.result())
    } else None
  }

  private def hasNextQuery: Boolean = {
    val result = !querySent
    querySent = true
    result
  }

  private def addBaseSelect(queryBuilder: StringBuilder): Unit = {
    if (database.equals("twitter") && table.equals("ds_tweet")) {

      val user_mentions_flatten_query = Range(0, 100)
        .map(i => "if_missing_or_null(to_string(to_array(user_mentions)[" + i + "]), \"\")")
        .mkString(", ")

      queryBuilder ++= "\n" + "select id, create_at, text, in_reply_to_status, in_reply_to_user, favorite_count" +
        ", retweet_count, lang, is_retweet, if_missing(string_join(hashtags, \", \"), \"\") hashtags" +
        ", rtrim(string_join([" + user_mentions_flatten_query + "], \", \"), \", \")  user_mentions, user.id user_id" +
        ", user.name" +
        ", user.screen_name" +
        ", user.location" +
        ", user.description" +
        ", user.followers_count" +
        ", user.friends_count, user.statues_count, geo_tag.stateName, geo_tag.countyName" +
        ", geo_tag.cityName, place.country, place.bounding_box " +
        s" from $database.$table WHERE 1 = 1 "

    } else {
      queryBuilder ++= "\n" + s"SELECT * FROM $database.$table WHERE 1 = 1 "
    }
  }

  private def addLimit(queryBuilder: StringBuilder): Unit = {
    queryBuilder ++= " LIMIT " + curLimit.get
  }

  private def addOffset(queryBuilder: StringBuilder): Unit = {
    queryBuilder ++= " OFFSET " + curOffset.get
  }
}
