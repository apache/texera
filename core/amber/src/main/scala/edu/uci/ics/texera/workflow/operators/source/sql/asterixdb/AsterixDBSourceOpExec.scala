package edu.uci.ics.texera.workflow.operators.source.sql.asterixdb

import com.github.tototoshi.csv.CSVParser
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType._
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.parseField
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, Schema}
import edu.uci.ics.texera.workflow.operators.source.sql.SQLSourceOpExec
import edu.uci.ics.texera.workflow.operators.source.sql.asterixdb.AsterixDBConnUtil.{
  queryAsterixDB,
  updateAsterixDBVersionMapping
}

import java.sql._
import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZoneOffset}
import scala.collection.Iterator
import scala.jdk.CollectionConverters.asScalaBufferConverter
import scala.util.control.Breaks.{break, breakable}
import scala.util.{Failure, Success, Try}

class AsterixDBSourceOpExec private[asterixdb] (
    schema: Schema,
    host: String,
    port: String,
    database: String,
    table: String,
    limit: Option[Long],
    offset: Option[Long],
    search: Option[Boolean],
    searchByColumn: Option[String],
    keywords: Option[String],
    progressive: Option[Boolean],
    batchByColumn: Option[String],
    min: Option[String],
    max: Option[String],
    interval: Long
) extends SQLSourceOpExec(
      schema,
      table,
      limit,
      offset,
      search,
      searchByColumn,
      keywords,
      progressive,
      batchByColumn,
      min,
      max,
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

      override def next: Tuple = {
        // if has the next Tuple in cache, return it and clear the cache
        cachedTuple.foreach(tuple => {
          cachedTuple = None
          return tuple
        })

        // otherwise, send query to fetch for the next Tuple

        while (true) {
          breakable {
            curResultIterator match {
              case Some(resultSet) =>
                if (resultSet.hasNext) {

                  // manually skip until the offset position in order to adapt to progressive batches
                  curOffset.fold()(offset => {
                    if (offset > 0) {
                      curOffset = Option(offset - 1)
                      break
                    }
                  })

                  // construct Texera.Tuple from the next result.
                  val tuple = buildTupleFromRow

                  if (tuple == null)
                    break

                  // update the limit in order to adapt to progressive batches
                  curLimit.fold()(limit => {
                    if (limit > 0) {
                      curLimit = Option(limit - 1)
                    }
                  })
                  return tuple
                } else {
                  // close the current resultSet and query
                  curResultIterator = None
                  curQueryString = None
                  break
                }
              case None =>
                curQueryString = if (hasNextQuery) generateSqlQuery else None
                curQueryString match {
                  case Some(query) =>
                    curResultIterator = queryAsterixDB(host, port, query)
                    break
                  case None =>
                    curResultIterator = None
                    return null
                }
            }
          }
        }
        null

      }
    }
  }

  /**
    * Build a Texera.Tuple from a row of curResultIterator
    *
    * @return the new Texera.Tuple
    */
  override def buildTupleFromRow: Tuple = {

    val tupleBuilder = Tuple.newBuilder(schema)
    val row = curResultIterator.get.next().toString

    var values: Option[List[String]] = None
    try {
      values = CSVParser.parse(row, '\\', ',', '"')
      if (values == null) {
        return null
      }
      for (i <- 0 until schema.getAttributes.size()) {
        val attr = schema.getAttributes.get(i)
        breakable {
          val columnType = attr.getType

          var value: String = null
          Try({ value = values.get(i) })

          if (value == null || value.equals("null")) {
            // add the field as null
            tupleBuilder.add(attr, null)
            break
          }

          // otherwise, transform the type of the value
          tupleBuilder.add(
            attr,
            parseField(value.stripSuffix("\"").stripPrefix("\""), columnType)
          )
        }
      }
      tupleBuilder.build
    } catch {
      case _: Exception =>
        null
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
    * @throws IllegalArgumentException if attribute does not support string based search
    */
  @throws[IllegalArgumentException]
  def addKeywordSearch(queryBuilder: StringBuilder): Unit = {

    val columnType = schema.getAttribute(searchByColumn.get).getType

    if (columnType == AttributeType.STRING) {

      queryBuilder ++= " AND ftcontains(" + searchByColumn.get + ", " + keywords.get + ") "
    } else
      throw new IllegalArgumentException("Can't do keyword search on type " + columnType.toString)
  }

  /**
    * Fetch for a numeric value of the boundary of the batchByColumn.
    * @param side either "MAX" or "MIN" for boundary
    * @return a numeric value, could be Int, Long or Double
    */
  override def fetchBatchByBoundary(side: String): Number = {
    batchByAttribute match {
      case Some(attribute) =>
        val resultString = queryAsterixDB(
          host,
          port,
          "SELECT " + side + "(" + attribute.getName + ") FROM " + database + "." + table + ";"
        ).get.next().toString.stripLineEnd
        Try(
          parseField(
            resultString.stripSuffix("\"").stripPrefix("\""),
            attribute.getType
          )
        ) match {
          case Success(timestamp: Timestamp) => parseField(timestamp, LONG).asInstanceOf[Number]
          case Success(otherTypes)           => otherTypes.asInstanceOf[Number]
          case Failure(_)                    => 0
        }

      case None => 0
    }
  }

  override def addBaseSelect(queryBuilder: StringBuilder): Unit = {
    queryBuilder ++= "\n" + s"SELECT ${schema.getAttributeNames.asScala.zipWithIndex
      .map((entry: (String, Int)) => { s"if_missing(${entry._1},null) field_${entry._2}" })
      .mkString(", ")} FROM $database.$table WHERE 1 = 1 "
  }

  override def addLimit(queryBuilder: StringBuilder): Unit = {
    queryBuilder ++= " LIMIT " + curLimit.get
  }

  override def addOffset(queryBuilder: StringBuilder): Unit = {
    queryBuilder ++= " OFFSET " + curOffset.get
  }

  @throws[IllegalArgumentException]
  override def batchAttributeToString(value: Number): String = {
    batchByAttribute match {
      case Some(attribute) =>
        attribute.getType match {
          case LONG | INTEGER | DOUBLE =>
            String.valueOf(value)
          case TIMESTAMP =>
            "datetime('" + formatter.format(new Timestamp(value.longValue).toInstant) + "')"
          case BOOLEAN | STRING | ANY | _ =>
            throw new IllegalArgumentException("Unexpected type: " + attribute.getType)
        }
      case None =>
        throw new IllegalArgumentException(
          "No valid batchByColumn to iterate: " + batchByColumn.getOrElse("")
        )
    }
  }

  /**
    * Fetch all table names from the given database. This is used to
    * check the input table name to prevent from SQL injection.
    */
  override protected def loadTableNames(): Unit = {
    // fetch for all tables, it is also equivalent to a health check
    val tables = queryAsterixDB(host, port, "select `DatasetName` from Metadata.`Dataset`;")
    tables.get.foreach(table => {
      tableNames.append(table.toString.stripPrefix("\"").stripLineEnd.stripSuffix("\""))
    })
  }

}
