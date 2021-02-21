package edu.uci.ics.texera.workflow.operators.source.asterixdb

import com.fasterxml.jackson.databind.JsonNode
import com.github.tototoshi.csv.CSVParser
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType._
import edu.uci.ics.texera.workflow.operators.source.asterixdb.AsterixDBConnUtil.queryAsterixDB

import java.sql._
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

  // connection and query related
  val tableNames: ArrayBuffer[String] = ArrayBuffer()
  val batchByAttribute: Option[Attribute] =
    if (progressive) Option(schema.getAttribute(batchByColumn.get)) else None
  var connection: Connection = _
  var curQuery: Option[String] = None
  var curResultSet: Option[Iterator[JsonNode]] = None
  var curLowerBound: Number = _
  var upperBound: Number = _
  var cachedTuple: Option[Tuple] = None
  var querySent: Boolean = false

  @throws[RuntimeException]
  override def open(): Unit = {

    // fetch for all tables, it is also equivalent to a health check
    val tables = queryAsterixDB(host, port, "select `DatasetName` from Metadata.`Dataset`;")
    tables.get.foreach(table => {
      tableNames.append(table.toString.stripPrefix("\"\\\"").stripSuffix("\\\"\\r\\n\""))
    })

    // validates the input table name
    if (!tableNames.contains(table))
      throw new RuntimeException("Can't find the given table `" + table + "`.")

    // load for batch column value boundaries used to split mini queries
    if (progressive) loadBatchColumnBoundaries()

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

  @throws[RuntimeException]
  def addKeywordSearch(queryBuilder: StringBuilder): Unit = {
    val columnType = schema.getAttribute(column.get).getType

    if (columnType == AttributeType.STRING) {
      // add naive support for full text search.
      // input is either
      //      ['a','b','c'], {'mode':'any'}
      // or
      //      ['a','b','c'], {'mode':'all'}
      queryBuilder ++= " AND ftcontains(" + column.get + ", " + keywords.get + ") "
    } else
      throw new RuntimeException("Can't do keyword search on type " + columnType.toString)
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
            tupleBuilder.add(attr, new Timestamp(value.toLong))
          case ANY | _ =>
            throw new RuntimeException("Unhandled attribute type: " + columnType)
        }
      }
    }
    tupleBuilder.build
  }

  private def hasNextQuery: Boolean = {
    batchByAttribute match {
      case Some(attribute) =>
        attribute.getType match {
          case INTEGER | LONG | TIMESTAMP =>
            curLowerBound.longValue <= upperBound.longValue
          case DOUBLE =>
            curLowerBound.doubleValue <= upperBound.doubleValue
          case STRING | ANY | BOOLEAN | _ =>
            throw new RuntimeException("Unexpected type: " + attribute.getType)
        }
      case None =>
        val hasNextQuery = !querySent
        querySent = true
        hasNextQuery
    }
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

      // TODO: add more selection conditions, including alias
      addBaseSelect(queryBuilder)

      // add keyword search if applicable
      if (column.isDefined && keywords.isDefined)
        addKeywordSearch(queryBuilder)

      // add sliding window if progressive mode is enabled
      if (progressive) addBatchSlidingWindow(queryBuilder)

      // add limit if provided
      if (curLimit.isDefined) {
        if (curLimit.get > 0) addLimit(queryBuilder)
        else
          // there should be no more queries as limit is equal or less than 0
          return None
      }

      // add fixed offset if not progressive
      if (!progressive && curOffset.isDefined) addOffset(queryBuilder)

      // end
      terminateSQL(queryBuilder)

      Option(queryBuilder.result())
    } else None
  }

  private def addBaseSelect(queryBuilder: StringBuilder): Unit = {
    if (database.equals("twitter") && table.equals("ds_tweet")) {
      // special case, support flattened twitter.ds_tweet

      val user_mentions_flatten_query = Range(0, 100)
        .map(i => "if_missing_or_null(to_string(to_array(user_mentions)[" + i + "]), \"\")")
        .mkString(", ")

      queryBuilder ++= "\n" +
        "SELECT id" +
        ", unix_time_from_datetime_in_ms(create_at)" +
        ", text" +
        ", in_reply_to_status" +
        ", in_reply_to_user" +
        ", favorite_count" +
        ", retweet_count" +
        ", lang" +
        ", is_retweet" +
        ", if_missing(string_join(hashtags, \", \"), \"\") hashtags" +
        ", rtrim(string_join([" + user_mentions_flatten_query + "], \", \"), \", \")  user_mentions" +
        ", user.id user_id" +
        ", user.name" +
        ", user.screen_name" +
        ", user.location" +
        ", user.description" +
        ", user.followers_count" +
        ", user.friends_count" +
        ", user.statues_count" +
        ", geo_tag.stateName" +
        ", geo_tag.countyName" +
        ", geo_tag.cityName" +
        ", place.country" +
        ", place.bounding_box " +
        s" FROM $database.$table WHERE 1 = 1 "

    } else {
      // general case, select everything, assuming the table is flattened.
      queryBuilder ++= "\n" + s"SELECT * FROM $database.$table WHERE 1 = 1 "
    }
  }

  private def addLimit(queryBuilder: StringBuilder): Unit = {
    queryBuilder ++= " LIMIT " + curLimit.get
  }

  private def addOffset(queryBuilder: StringBuilder): Unit = {
    queryBuilder ++= " OFFSET " + curOffset.get
  }

  private def terminateSQL(queryBuilder: StringBuilder): Unit = {
    queryBuilder ++= ";"
  }

  @throws[RuntimeException]
  private def addBatchSlidingWindow(queryBuilder: StringBuilder): Unit = {
    var nextLowerBound: Number = null
    var isLastBatch = false

    // FIXME: does not support Timestamp now
    batchByAttribute match {
      case Some(attribute) =>
        attribute.getType match {
          case INTEGER | LONG | TIMESTAMP =>
            nextLowerBound = curLowerBound.longValue + interval
            isLastBatch = nextLowerBound.longValue >= upperBound.longValue
          case DOUBLE =>
            nextLowerBound = curLowerBound.doubleValue + interval
            isLastBatch = nextLowerBound.doubleValue >= upperBound.doubleValue
          case BOOLEAN | STRING | ANY | _ =>
            throw new RuntimeException("Unexpected type: " + attribute.getType)
        }
        queryBuilder ++= " AND " + attribute.getName + " >= " + batchAttributeToString(
          curLowerBound
        ) +
          " AND " + attribute.getName +
          (if (isLastBatch)
             " <= " + batchAttributeToString(upperBound)
           else
             " < " + batchAttributeToString(nextLowerBound))
      case None =>
        throw new RuntimeException(
          "no valid batchByColumn to iterate: " + batchByColumn.getOrElse("")
        )
    }
    curLowerBound = nextLowerBound
  }

  @throws[RuntimeException]
  private def batchAttributeToString(value: Number): String = {
    batchByAttribute match {
      case Some(attribute) =>
        attribute.getType match {
          case LONG | INTEGER | DOUBLE =>
            String.valueOf(value)
          case TIMESTAMP =>
            new Timestamp(value.longValue).toString
          case BOOLEAN | STRING | ANY | _ =>
            throw new RuntimeException("Unexpected type: " + attribute.getType)
        }
      case None =>
        throw new RuntimeException(
          "No valid batchByColumn to iterate: " + batchByColumn.getOrElse("")
        )
    }
  }
  @throws[SQLException]
  private def loadBatchColumnBoundaries(): Unit = {
    batchByAttribute match {
      case Some(attribute) =>
        if (attribute.getName.nonEmpty) {
          upperBound = getBatchByBoundary("MAX").getOrElse(0)
          curLowerBound = getBatchByBoundary("MIN").getOrElse(0)
        }
      case None =>
    }
  }

  @throws[SQLException]
  private def getBatchByBoundary(side: String): Option[Number] = {
    batchByAttribute match {
      case Some(attribute) =>
        var result: Number = null
        val resultString = queryAsterixDB(
          host,
          port,
          "SELECT " + side + "(" + attribute.getName + ") FROM " + database + "." + table + ";"
        ).get.next().textValue().stripLineEnd

        schema.getAttribute(attribute.getName).getType match {
          case INTEGER =>
            result = resultString.toInt

          case LONG =>
            result = resultString.toLong

          case TIMESTAMP =>
            result = Timestamp.valueOf(resultString).getTime

          case DOUBLE =>
            result = resultString.toDouble

          case BOOLEAN =>
          case STRING  =>
          case ANY     =>
          case _ =>
            throw new IllegalStateException("Unexpected value: " + attribute.getType)
        }
        Option(result)
      case None => None
    }
  }
}
