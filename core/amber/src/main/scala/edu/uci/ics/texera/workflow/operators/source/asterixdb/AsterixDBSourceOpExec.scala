package edu.uci.ics.texera.workflow.operators.source.asterixdb

import com.github.tototoshi.csv.CSVParser
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, Schema}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType._
import edu.uci.ics.texera.workflow.operators.source.asterixdb.AsterixDBConnUtil.{
  queryAsterixDB,
  updateAsterixDBVersionMapping
}
import edu.uci.ics.texera.workflow.operators.source.SQLSourceOpExec

import java.sql._
import java.time.{Instant, ZoneId, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.collection.Iterator
import scala.util.control.Breaks.{break, breakable}

class AsterixDBSourceOpExec private[asterixdb] (
    schema: Schema,
    host: String,
    port: String,
    database: String,
    table: String,
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
  // update AsterixDB API version upon initialization.
  updateAsterixDBVersionMapping(host, port)

  // format Timestamp. TODO: move to some util package
  val formatter: DateTimeFormatter =
    DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC))

  var curQueryString: Option[String] = None
  var curResultIterator: Option[Iterator[AnyRef]] = None

  /**
    * A generator of a Texera.Tuple, which converted from a CSV row of fields from AsterixDB
    * @return Iterator[Tuple]
    */
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

      @throws[RuntimeException]
      override def next: Tuple = {
        // if has the next Tuple in cache, return it and clear the cache
        cachedTuple.foreach(tuple => {
          cachedTuple = None
          return tuple
        })

        // otherwise, send query to fetch for the next Tuple
        curResultIterator match {
          case Some(resultSet) =>
            if (resultSet.hasNext) {

              // manually skip until the offset position in order to adapt to progressive batches
              curOffset.fold()(offset => {
                if (offset > 0) {
                  curOffset = Option(offset - 1)
                  return next
                }
              })

              // construct Texera.Tuple from the next result.
              val tuple = buildTupleFromRow

              if (tuple == null)
                return next

              // update the limit in order to adapt to progressive batches
              curLimit.fold()(limit => {
                if (limit > 0) {
                  curLimit = Option(limit - 1)
                }
              })
              tuple
            } else {
              // close the current resultSet and query
              curResultIterator = None
              curQueryString = None
              next
            }
          case None =>
            curQueryString = generateSqlQuery
            curQueryString match {
              case Some(query) =>
                curResultIterator = queryAsterixDB(host, port, query)
                next
              case None =>
                curResultIterator = None
                null
            }
        }

      }
    }
  }

  /**
    * close curResultIterator, curQueryString
    */
  override def close(): Unit = {
    curResultIterator = None
    curQueryString = None
  }

  /**
    * add naive support for full text search.
    * input is either
    *     ['hello', 'world'], {'mode':'any'}
    * or
    *     ['hello', 'world'], {'mode':'all'}
    * @param queryBuilder queryBuilder for concatenation
    * @throws RuntimeException if attribute does not support string based search
    */
  @throws[RuntimeException]
  def addKeywordSearch(queryBuilder: StringBuilder): Unit = {

    val columnType = schema.getAttribute(column.get).getType

    if (columnType == AttributeType.STRING) {

      queryBuilder ++= " AND ftcontains(" + column.get + ", " + keywords.get + ") "
    } else
      throw new RuntimeException("Can't do keyword search on type " + columnType.toString)
  }

  /**
    * Fetch for a numeric value of the boundary of the batchByColumn.
    * @param side either "MAX" or "MIN" for boundary
    * @throws RuntimeException all possible exceptions from HTTP connection
    * @return a numeric value, could be Int, Long or Double
    */
  @throws[RuntimeException]
  override def getBatchByBoundary(side: String): Option[Number] = {
    batchByAttribute match {
      case Some(attribute) =>
        var result: Number = null
        val resultString = queryAsterixDB(
          host,
          port,
          "SELECT " + side + "(" + attribute.getName + ") FROM " + database + "." + table + ";"
        ).get.next().toString.stripLineEnd

        // TODO: move this to some util package
        schema.getAttribute(attribute.getName).getType match {
          case INTEGER =>
            result = resultString.toInt
          case LONG =>
            result = resultString.toLong
          case TIMESTAMP =>
            result = Instant.parse(resultString.stripSuffix("\"").stripPrefix("\"")).toEpochMilli
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

  /**
    * Build a Texera.Tuple from a row of curResultIterator
    *
    * @return the new Texera.Tuple
    */
  @throws[RuntimeException]
  override def buildTupleFromRow: Tuple = {

    val tupleBuilder = Tuple.newBuilder
    val row = curResultIterator.get.next().toString

    var values: Option[List[String]] = None
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
            tupleBuilder.add(
              attr,
              new Timestamp(Instant.parse(value.stripSuffix("\"").stripPrefix("\"")).toEpochMilli)
            )
          case ANY | _ =>
            throw new RuntimeException("Unhandled attribute type: " + columnType)
        }
      }
    }
    tupleBuilder.build
  }

  override def addBaseSelect(queryBuilder: StringBuilder): Unit = {
    if (database.equals("twitter") && table.equals("ds_tweet")) {
      // special case, support flattened twitter.ds_tweet

      val user_mentions_flatten_query = Range(0, 100)
        .map(i => "if_missing_or_null(to_string(to_array(user_mentions)[" + i + "]), \"\")")
        .mkString(", ")

      queryBuilder ++= "\n" +
        "SELECT id" +
        ", create_at" +
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

  override def addLimit(queryBuilder: StringBuilder): Unit = {
    queryBuilder ++= " LIMIT " + curLimit.get
  }

  override def addOffset(queryBuilder: StringBuilder): Unit = {
    queryBuilder ++= " OFFSET " + curOffset.get
  }

  @throws[RuntimeException]
  override def batchAttributeToString(value: Number): String = {
    batchByAttribute match {
      case Some(attribute) =>
        attribute.getType match {
          case LONG | INTEGER | DOUBLE =>
            String.valueOf(value)
          case TIMESTAMP =>
            "datetime('" + formatter.format(new Timestamp(value.longValue).toInstant) + "')"
          case BOOLEAN | STRING | ANY | _ =>
            throw new RuntimeException("Unexpected type: " + attribute.getType)
        }
      case None =>
        throw new RuntimeException(
          "No valid batchByColumn to iterate: " + batchByColumn.getOrElse("")
        )
    }
  }

  /**
    * Fetch all table names from the given database. This is used to
    * check the input table name to prevent from SQL injection.
    * @throws RuntimeException all possible exceptions from HTTP connection
    */
  @throws[RuntimeException]
  override protected def loadTableNames(): Unit = {
    // fetch for all tables, it is also equivalent to a health check
    val tables = queryAsterixDB(host, port, "select `DatasetName` from Metadata.`Dataset`;")
    tables.get.foreach(table => {
      tableNames.append(table.toString.stripPrefix("\"").stripLineEnd.stripSuffix("\""))
    })
  }

}
