package edu.uci.ics.amber.operator.source.sql.asterixdb

import com.github.tototoshi.csv.CSVParser
import edu.uci.ics.amber.core.tuple.AttributeTypeUtils.parseField
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema, Tuple, TupleLike}
import edu.uci.ics.amber.operator.filter.FilterPredicate
import AsterixDBConnUtil.{queryAsterixDB, updateAsterixDBVersionMapping}
import edu.uci.ics.amber.operator.source.sql.SQLSourceOpExec

import java.sql._
import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZoneOffset}
import scala.collection.Iterator
import scala.util.control.Breaks.{break, breakable}
import scala.util.{Failure, Success, Try}

class AsterixDBSourceOpExec private[asterixdb] (
    host: String,
    port: String,
    database: String,
    table: String,
    limit: Option[Long],
    offset: Option[Long],
    progressive: Option[Boolean],
    batchByColumn: Option[String],
    min: Option[String],
    max: Option[String],
    interval: Long,
    keywordSearch: Boolean,
    keywordSearchByColumn: String,
    keywords: String,
    geoSearch: Boolean,
    geoSearchByColumns: List[String],
    geoSearchBoundingBox: List[String],
    regexSearch: Boolean,
    regexSearchByColumn: String,
    regex: String,
    filterCondition: Boolean,
    filterPredicates: List[FilterPredicate],
    schemaFunc: () => Schema
) extends SQLSourceOpExec(
      table,
      limit,
      offset,
      progressive,
      batchByColumn,
      min,
      max,
      interval,
      keywordSearch,
      keywordSearchByColumn,
      keywords,
      schemaFunc
    ) {

  // format Timestamp. TODO: move to some util package
  private val formatter: DateTimeFormatter =
    DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC))

  private var curQueryString: Option[String] = None
  private var curResultIterator: Option[Iterator[AnyRef]] = None

  override def open(): Unit = {
    // update AsterixDB API version upon open
    updateAsterixDBVersionMapping(host, port)
    super.open()
  }

  /**
    * A generator of a Tuple, which converted from a CSV row of fields from AsterixDB
    * @return Iterator[TupleLike]
    */
  override def produceTuple(): Iterator[TupleLike] = {
    new Iterator[TupleLike]() {
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

        while (true) {
          breakable {
            curResultIterator match {
              case Some(resultSet) =>
                if (resultSet.hasNext) {

                  // manually skip until the offset position in order to adapt to progressive batches
                  curOffset.foreach(offset => {
                    if (offset > 0) {
                      curOffset = Option(offset - 1)
                      break()
                    }
                  })

                  // construct Tuple from the next result.
                  val tuple = buildTupleFromRow

                  if (tuple == null)
                    break()

                  // update the limit in order to adapt to progressive batches
                  curLimit.foreach(limit => {
                    if (limit > 0) {
                      curLimit = Option(limit - 1)
                    }
                  })
                  return tuple
                } else {
                  // close the current resultSet and query
                  curResultIterator = None
                  curQueryString = None
                  break()
                }
              case None =>
                curQueryString = if (hasNextQuery) generateSqlQuery else None
                curQueryString match {
                  case Some(query) =>
                    curResultIterator = queryAsterixDB(host, port, query)
                    break()
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
    * Build a Tuple from a row of curResultIterator
    *
    * @return the new Tuple
    */
  override def buildTupleFromRow: Tuple = {

    val tupleBuilder = Tuple.builder(schema)
    val row = curResultIterator.get.next().toString

    var values: Option[List[String]] = None
    try {
      values = CSVParser.parse(row, '\\', ',', '"')
      if (values == null) {
        return null
      }
      for (i <- schema.getAttributes.indices) {
        val attr = schema.getAttributes(i)
        breakable {
          val columnType = attr.getType

          var value: String = null
          Try({ value = values.get(i) })

          if (value == null || value.equals("null")) {
            // add the field as null
            tupleBuilder.add(attr, null)
            break()
          }

          // otherwise, transform the type of the value
          tupleBuilder.add(
            attr,
            parseField(value.stripSuffix("\"").stripPrefix("\""), columnType)
          )
        }
      }
      tupleBuilder.build()
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
  def addFilterConditions(queryBuilder: StringBuilder): Unit = {
    if (keywordSearch) {
      addKeywordSearch(queryBuilder)
    }

    if (regexSearch) {
      addRegexSearch(queryBuilder)
    }

    if (geoSearch) {
      addGeoSearch(queryBuilder)
    }

    if (filterCondition) {
      addGeneralFilterCondition(queryBuilder)
    }
  }

  private def addKeywordSearch(queryBuilder: StringBuilder): Unit = {
    if (keywordSearchByColumn != null && keywords != null) {
      val columnType = schema.getAttribute(keywordSearchByColumn).getType
      if (columnType == AttributeType.STRING) {
        queryBuilder ++= " AND ftcontains(" + keywordSearchByColumn + ", " + keywords + ") "
      } else
        throw new IllegalArgumentException("Can't do keyword search on type " + columnType.toString)
    }
  }

  private def addRegexSearch(queryBuilder: StringBuilder): Unit = {
    if (regexSearchByColumn != null && regex != null) {
      val regexColumnType = schema.getAttribute(regexSearchByColumn).getType
      if (regexColumnType == AttributeType.STRING) {
        queryBuilder ++= " AND regexp_contains(" + regexSearchByColumn + ", \"" + regex + "\") "
      } else
        throw new IllegalArgumentException(
          "Can't do regex search on type " + regexColumnType.toString
        )
    }
  }

  private def addGeoSearch(queryBuilder: StringBuilder): Unit = {
    // geolocation must contain more than 1 points to from a rectangle or polygon
    if (geoSearchBoundingBox.size > 1 && geoSearchByColumns.nonEmpty) {
      val shape = {
        val points = geoSearchBoundingBox.flatMap(s => s.split(",").map(sub => sub.toDouble))
        if (geoSearchBoundingBox.size == 2) {
          "create_rectangle(create_point(%.6f,%.6f), create_point(%.6f,%.6f))".format(points: _*)
        } else {
          "create_polygon([" + points.map(x => "%.6f".format(x)).mkString(",") + "])"
        }
      }
      queryBuilder ++= " AND ("
      queryBuilder ++= geoSearchByColumns
        .map { attr => s"spatial_intersect($attr, $shape)" }
        .mkString(" OR ")
      queryBuilder ++= " ) "
    }
  }

  private def addGeneralFilterCondition(queryBuilder: StringBuilder): Unit = {
    if (filterCondition && filterPredicates.nonEmpty) {
      val filterString = filterPredicates
        .map(p => s"(${p.attribute} ${p.condition.getName} ${p.value})")
        .mkString(" OR ")
      queryBuilder ++= s" AND ( $filterString ) "
    }
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
          case Success(timestamp: Timestamp) =>
            parseField(timestamp, AttributeType.LONG).asInstanceOf[Number]
          case Success(otherTypes) => otherTypes.asInstanceOf[Number]
          case Failure(_)          => 0
        }

      case None => 0
    }
  }

  override def addBaseSelect(queryBuilder: StringBuilder): Unit = {
    queryBuilder ++= "\n" + s"SELECT ${schema.getAttributeNames.zipWithIndex
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
          case AttributeType.LONG | AttributeType.INTEGER | AttributeType.DOUBLE =>
            String.valueOf(value)
          case AttributeType.TIMESTAMP =>
            "datetime('" + formatter.format(new Timestamp(value.longValue).toInstant) + "')"
          case AttributeType.BOOLEAN | AttributeType.STRING | AttributeType.ANY | _ =>
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
