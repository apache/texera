package edu.uci.ics.texera.workflow.operators.source

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}

import java.sql._
import scala.collection.Iterator
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

abstract class SQLSourceOpExec(
    // source configs
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
    // progressiveness related
    progressive: Boolean,
    batchByColumn: Option[String],
    interval: Long
) extends SourceOperatorExecutor {

  // connection and query related
  var tableNames: ArrayBuffer[String] = ArrayBuffer()
  protected var batchByAttribute: Option[Attribute] =
    if (progressive && batchByColumn != null && batchByColumn.isDefined)
      Option(schema.getAttribute(batchByColumn.get))
    else None
  protected var connection: Connection = _
  protected var curQuery: Option[PreparedStatement] = None
  protected var curResultSet: Option[ResultSet] = None
  protected var curLowerBound: Number = _
  protected var upperBound: Number = _
  protected var cachedTuple: Option[Tuple] = None
  protected var querySent: Boolean = _

  override def produceTexeraTuple(): Iterator[Tuple] =
    new Iterator[Tuple]() {
      override def hasNext: Boolean = {
        // if existing Tuple in cache, means there exist next Tuple.
        cachedTuple match {
          case Some(_) => true
          case None    =>
            // cache the next Tuple
            cachedTuple = Option(next)
            cachedTuple.isDefined
        }

      }

      /**
        * Fetch the next row from resultSet, parse it into Texera.Tuple and return.
        * - If resultSet is exhausted, send the next query until no more queries are available.
        * - If no more queries, return null.
        *
        * @return Texera.Tuple
        */
      override def next: Tuple = { // if has the next Tuple in cache, return it and clear the cache

        cachedTuple.foreach(tuple => {
          cachedTuple = None
          return tuple
        })

        // otherwise, send query to fetch for the next Tuple
        try {

          curResultSet match {
            case Some(resultSet) =>
              if (resultSet.next()) {

                // manually skip until the offset position in order to adapt to progressive batches
                curOffset.fold()(offset => {
                  if (offset > 0) {
                    curOffset = Option(offset - 1)
                    return next
                  }
                })

                // construct Texera.Tuple from the next result.
                val tuple = buildTupleFromRow
                // update the limit in order to adapt to progressive batches
                curLimit.fold()(limit => {
                  if (limit > 0) {
                    curLimit = Option(limit - 1)
                  }
                })
                tuple
              } else {
                // close the current resultSet and query
                curResultSet.foreach(resultSet => resultSet.close())
                curResultSet = None
                curQuery.foreach(query => query.close())

                curQuery = getNextQuery
                if (curQuery.isDefined) println(curQuery.get)
                curQuery match {
                  case Some(query) =>
                    curResultSet = Option(query.executeQuery)
                    next
                  case None => null
                }
              }
            case None =>
              curQuery = getNextQuery
              System.out.println(curQuery.toString)
              curQuery match {
                case Some(query) =>
                  curResultSet = Option(query.executeQuery)
                  next
                case None => null
              }
          }
        } catch {
          case e: SQLException =>
            e.printStackTrace()
            throw new RuntimeException(e)
        }
      }
    }

  /**
    * Build a Texera.Tuple from a row of curResultSet
    *
    * @return the new Texera.Tuple
    * @throws SQLException
    */ @throws[SQLException]
  private def buildTupleFromRow: Tuple = {

    val tupleBuilder = Tuple.newBuilder
    import scala.collection.JavaConversions._
    for (attr <- schema.getAttributes) {
      breakable {
        val columnName = attr.getName
        val columnType = attr.getType
        val value = curResultSet.get.getString(columnName)
        if (value == null) {
          tupleBuilder.add(attr, null)
          break
        }
        columnType match {
          case AttributeType.INTEGER =>
            tupleBuilder.add(attr, Integer.valueOf(value))
          case AttributeType.LONG | AttributeType.DOUBLE | AttributeType.STRING =>
            tupleBuilder.add(attr, value)

          case AttributeType.BOOLEAN =>
            tupleBuilder.add(attr, !(value == "0"))

          case AttributeType.TIMESTAMP =>
            tupleBuilder.add(attr, Timestamp.valueOf(value))

          case AttributeType.ANY | _ =>
            throw new RuntimeException(
              this.getClass.getSimpleName + ": unhandled attribute type: " + columnType
            )
        }

      }
    }
    tupleBuilder.build
  }

  @throws[SQLException]
  private def getNextQuery: Option[PreparedStatement] =
    if (hasNextQuery) {
      val nextQuery = generateSqlQuery
      nextQuery match {
        case Some(query) =>
          val preparedStatement = connection.prepareStatement(query)
          var curIndex = 1
          if (column.isDefined && keywords.isDefined) {
            preparedStatement.setString(curIndex, keywords.get)
            curIndex += 1
          }
          curLimit match {
            case Some(limit) =>
              if (limit > 0) preparedStatement.setLong(curIndex, curLimit.get)
            case None =>
          }
          println(preparedStatement)
          Option(preparedStatement)
        case None => None
      }
    } else None

  private def hasNextQuery: Boolean =
    batchByAttribute match {
      case Some(attribute) =>
        attribute.getType match {
          case AttributeType.INTEGER | AttributeType.LONG | AttributeType.TIMESTAMP =>
            curLowerBound.longValue <= upperBound.longValue

          case AttributeType.DOUBLE =>
            curLowerBound.doubleValue <= upperBound.doubleValue
          case AttributeType.STRING | AttributeType.ANY | AttributeType.BOOLEAN | _ =>
            throw new IllegalStateException("Unexpected value: " + attribute.getType)
        }
      case None =>
        val hasNextQuery = !querySent
        querySent = true
        hasNextQuery
    }

  /**
    * generate sql query string using the info provided by user. One of following
    * select * from TableName where 1 = 1 AND MATCH (ColumnName) AGAINST ( ? IN BOOLEAN MODE) LIMIT ?;
    * select * from TableName where 1 = 1 AND MATCH (ColumnName) AGAINST ( ? IN BOOLEAN MODE);
    * select * from TableName where 1 = 1 LIMIT ?;
    * select * from TableName where 1 = 1;
    * <p>
    * with an optional appropriate batchByColumn sliding window,
    * e.g. create_at >= '2017-01-14 03:47:59.0' AND create_at < '2017-01-15 03:47:59.0'
    *
    * @return string of sql query
    */
  private def generateSqlQuery
      : Option[String] = { // in sql prepared statement, table name cannot be inserted using PreparedStatement.setString
    // so it has to be inserted here during sql query generation
    // this.table has to be verified to be existing in the given schema.
    var query = "\n" + "SELECT * FROM " + table + " where 1 = 1"
    // in sql prepared statement, column name cannot be inserted using PreparedStatement.setString either
    if (column.isDefined && keywords.isDefined)
      query += " AND MATCH(" + column + ") AGAINST (? IN BOOLEAN MODE)"
    if (progressive) {
      var nextLowerBound: Number = null
      var isLastBatch = false

      batchByAttribute match {
        case Some(attribute) =>
          attribute.getType match {
            case AttributeType.INTEGER | AttributeType.LONG | AttributeType.TIMESTAMP =>
              nextLowerBound = curLowerBound.longValue + interval
              isLastBatch = nextLowerBound.longValue >= upperBound.longValue

            case AttributeType.DOUBLE =>
              nextLowerBound = curLowerBound.doubleValue + interval
              isLastBatch = nextLowerBound.doubleValue >= upperBound.doubleValue

            case AttributeType.BOOLEAN | AttributeType.STRING | AttributeType.ANY | _ =>
              throw new IllegalStateException("Unexpected value: " + attribute.getType)
          }
          query += " AND " + attribute.getName + " >= '" + batchAttributeToString(
            curLowerBound
          ) + "'" + " AND " + attribute.getName + (if (isLastBatch)
                                                     " <= '" + batchAttributeToString(
                                                       upperBound
                                                     )
                                                   else
                                                     " < '" + batchAttributeToString(
                                                       nextLowerBound
                                                     )) + "'"
        case None => None
      }
      curLowerBound = nextLowerBound
    }

    if (curLimit.isDefined) {
      System.out.println("curLimit: " + curLimit.toString)
      if (curLimit.get > 0) query += " LIMIT ?"
      else return None
    }
    query += ";"
    Option(query)
  }

  /**
    * Convert the Number value to a String to be concatenate to SQL.
    *
    * @param value a Number, contains the value to be converted.
    * @return a String of that value
    * @throws IllegalStateException
    */ @throws[IllegalStateException]
  private def batchAttributeToString(value: Number) = {
    batchByAttribute match {
      case Some(attribute) =>
        attribute.getType match {
          case AttributeType.LONG | AttributeType.INTEGER | AttributeType.DOUBLE =>
            String.valueOf(value)
          case AttributeType.TIMESTAMP =>
            new Timestamp(value.longValue).toString
          case AttributeType.BOOLEAN | AttributeType.STRING | AttributeType.ANY | _ =>
            throw new IllegalStateException("Unexpected value: " + attribute.getType)
        }
      case None =>
    }

  }

  /**
    * Establish a connection to the database server and load statistics for constructing future queries.
    * - tableNames, to check if the input tableName exists on the database server, to prevent SQL injection.
    * - batchColumnBoundaries, to be used to split mini queries, if progressive mode is enabled.
    */
  override def open(): Unit =
    try {
      connection = establishConn
      // load user table names from the given database
      loadTableNames()
      // validates the input table name
      if (!tableNames.contains(table))
        throw new RuntimeException(
          this.getClass.getSimpleName + " can't find the given table `" + table + "`."
        )
      // load for batch column value boundaries used to split mini queries
      if (progressive) loadBatchColumnBoundaries()
    } catch {
      case e: SQLException =>
        e.printStackTrace()
        throw new RuntimeException(
          this.getClass.getSimpleName + " failed to connect to database. " + e.getMessage
        )
    }

  @throws[SQLException]
  private def loadBatchColumnBoundaries(): Unit =
    batchByAttribute match {
      case Some(attribute) =>
        if (attribute.getName.nonEmpty) {
          upperBound = getBatchByBoundary("MAX").getOrElse(0)
          curLowerBound = getBatchByBoundary("MIN").getOrElse(0)
        }
      case None =>
    }

  @throws[SQLException]
  private def getBatchByBoundary(side: String): Option[Number] = {
    batchByAttribute match {
      case Some(attribute) =>
        var result: Number = null
        val preparedStatement = connection.prepareStatement(
          "SELECT " + side + "(" + attribute.getName + ") FROM " + table + ";"
        )
        val resultSet = preparedStatement.executeQuery
        resultSet.next
        schema.getAttribute(attribute.getName).getType match {
          case AttributeType.INTEGER =>
            result = resultSet.getInt(1)

          case AttributeType.LONG =>
            result = resultSet.getLong(1)

          case AttributeType.TIMESTAMP =>
            result = resultSet.getTimestamp(1).getTime

          case AttributeType.DOUBLE =>
            result = resultSet.getDouble(1)

          case AttributeType.BOOLEAN =>
          case AttributeType.STRING  =>
          case AttributeType.ANY     =>
          case _ =>
            throw new IllegalStateException("Unexpected value: " + attribute.getType)
        }
        resultSet.close()
        preparedStatement.close()
        Option(result)
      case None => None
    }
  }

  /**
    * close resultSet, preparedStatement and connection
    */
  override def close(): Unit =
    try {
      curResultSet.foreach(resultSet => resultSet.close())
      curQuery.foreach(query => query.close())

      if (connection != null) connection.close()
    } catch {
      case e: SQLException =>
        throw new RuntimeException(this.getClass.getSimpleName + " fail to close. " + e.getMessage)
    }

  @throws[SQLException]
  protected def establishConn: Connection

  @throws[SQLException]
  protected def loadTableNames(): Unit
}
