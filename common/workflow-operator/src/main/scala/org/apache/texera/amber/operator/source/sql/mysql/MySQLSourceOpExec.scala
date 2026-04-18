/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.operator.source.sql.mysql

import io.vertx.core.Vertx
import io.vertx.sqlclient.{Pool, Row, SqlConnection, Transaction, Tuple => VxTuple}
import org.apache.texera.amber.core.executor.SourceOperatorExecutor
import org.apache.texera.amber.core.tuple.AttributeTypeUtils.{parseField, parseTimestamp}
import org.apache.texera.amber.core.tuple._
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable.ArrayBuffer

/**
  * MySQL source executor backed by the Vert.x MySQL client (Apache 2.0).
  *
  * The Vert.x client is reactive (`Future[RowSet]`), but the operator framework
  * pulls tuples via a synchronous `Iterator[TupleLike]`. We bridge the two with
  * a bounded [[LinkedBlockingQueue]]: the Vert.x event loop pushes rows as
  * they arrive and the iterator thread blocks on `take()`.
  *
  * This class intentionally does not extend [[org.apache.texera.amber.operator.source.sql.SQLSourceOpExec]]
  * because that base class is hard-wired to JDBC (`Connection`, `PreparedStatement`,
  * `ResultSet`). The query-building / keyword-search / progressive-batching logic
  * is re-implemented here against the Vert.x API.
  */
class MySQLSourceOpExec private[mysql] (descString: String) extends SourceOperatorExecutor {

  private val desc: MySQLSourceOpDesc =
    objectMapper.readValue(descString, classOf[MySQLSourceOpDesc])
  private var schema: Schema = _
  private var vertx: Vertx = _
  private var pool: Pool = _

  private val tableNames: ArrayBuffer[String] = ArrayBuffer()
  private var batchByAttribute: Option[Attribute] = None
  private var curLimit: Option[Long] = None
  private var curOffset: Option[Long] = None
  private var curLowerBound: Number = _
  private var upperBound: Number = _
  private var querySent: Boolean = false

  override def open(): Unit = {
    schema = desc.sourceSchema()
    curLimit = desc.limit
    curOffset = desc.offset

    vertx = Vertx.vertx()
    pool = MySQLConnUtil.createPool(
      vertx,
      desc.host,
      desc.port,
      desc.database,
      desc.username,
      desc.password
    )

    batchByAttribute =
      if (desc.progressive.getOrElse(false)) Option(schema.getAttribute(desc.batchByColumn.get))
      else None

    loadTableNames()
    if (!tableNames.contains(desc.table))
      throw new RuntimeException("Can't find the given table `" + desc.table + "`.")

    if (desc.progressive.getOrElse(false)) initBatchColumnBoundaries()
  }

  override def close(): Unit = {
    try if (pool != null) pool.close().toCompletionStage.toCompletableFuture.get()
    finally if (vertx != null) vertx.close().toCompletionStage.toCompletableFuture.get()
  }

  override def produceTuple(): Iterator[TupleLike] =
    new Iterator[TupleLike] {
      private var currentStream: Option[StreamingResult] = None
      private var cached: Option[Tuple] = None

      override def hasNext: Boolean = {
        if (cached.isDefined) return true
        cached = Option(advance())
        cached.isDefined
      }

      override def next(): TupleLike = {
        if (cached.isEmpty) cached = Option(advance())
        val t = cached.orNull
        cached = None
        if (t == null) throw new NoSuchElementException
        t
      }

      @scala.annotation.tailrec
      private def advance(): Tuple = {
        if (currentStream.isEmpty) {
          nextQuery() match {
            case Some((sql, params)) => currentStream = Some(startStream(sql, params))
            case None                => return null
          }
        }
        val stream = currentStream.get
        stream.nextRow() match {
          case Some(row) =>
            if (curOffset.exists(_ > 0)) {
              curOffset = curOffset.map(_ - 1)
              advance()
            } else {
              val tuple = buildTupleFromRow(row)
              curLimit.foreach(limit => if (limit > 0) curLimit = Some(limit - 1))
              tuple
            }
          case None =>
            currentStream = None
            advance()
        }
      }
    }

  // -- async → sync bridge ----------------------------------------------------

  private sealed trait StreamEvent
  private final case class DataRow(row: Row) extends StreamEvent
  private case object EndMarker extends StreamEvent
  private final case class ErrorMarker(err: Throwable) extends StreamEvent

  private final class StreamingResult(queue: LinkedBlockingQueue[StreamEvent]) {
    private var closed = false
    def nextRow(): Option[Row] = {
      if (closed) return None
      queue.take() match {
        case DataRow(r) => Some(r)
        case EndMarker =>
          closed = true
          None
        case ErrorMarker(err) =>
          closed = true
          throw new RuntimeException(err)
      }
    }
  }

  /**
    * Open a Vert.x cursor-backed stream for `sql` and push rows onto a blocking
    * queue. A transaction is required because Vert.x cursors only exist within
    * one.
    */
  private def startStream(sql: String, params: VxTuple): StreamingResult = {
    val queue = new LinkedBlockingQueue[StreamEvent]()

    pool
      .getConnection()
      .onSuccess { (conn: SqlConnection) =>
        conn
          .begin()
          .onSuccess { (tx: Transaction) =>
            conn
              .prepare(sql)
              .onSuccess { ps =>
                val stream = ps.createStream(256, params)
                stream.handler(row => queue.put(DataRow(row)))
                stream.endHandler { _ =>
                  tx.commit().onComplete(_ => {
                    conn.close()
                    queue.put(EndMarker)
                  })
                }
                stream.exceptionHandler { err =>
                  tx.rollback().onComplete(_ => {
                    conn.close()
                    queue.put(ErrorMarker(err))
                  })
                }
              }
              .onFailure { err =>
                tx.rollback().onComplete(_ => {
                  conn.close()
                  queue.put(ErrorMarker(err))
                })
              }
          }
          .onFailure { err =>
            conn.close()
            queue.put(ErrorMarker(err))
          }
      }
      .onFailure(err => queue.put(ErrorMarker(err)))

    new StreamingResult(queue)
  }

  // -- query building --------------------------------------------------------

  private def nextQuery(): Option[(String, VxTuple)] = {
    if (!hasNextQuery) return None
    val sql = new StringBuilder
    val params = VxTuple.tuple()

    sql ++= "SELECT * FROM " + desc.table + " WHERE 1 = 1"

    val keywordCol = desc.keywordSearchByColumn.orNull
    val keywords = desc.keywords.orNull
    if (desc.keywordSearch.getOrElse(false) && keywordCol != null && keywords != null) {
      val columnType = schema.getAttribute(keywordCol).getType
      if (columnType == AttributeType.STRING) {
        sql ++= " AND MATCH(" + keywordCol + ") AGAINST (? IN BOOLEAN MODE)"
        params.addString(keywords)
      } else throw new RuntimeException("Can't do keyword search on type " + columnType.toString)
    }

    if (desc.progressive.getOrElse(false) && desc.batchByColumn.isDefined && desc.interval > 0L)
      addBatchSlidingWindow(sql)

    if (curLimit.isDefined) {
      if (curLimit.get > 0) {
        sql ++= " LIMIT ?"
        params.addLong(curLimit.get)
      } else return None
    }

    if (!desc.progressive.getOrElse(false) && curOffset.isDefined) {
      sql ++= " OFFSET ?"
      params.addLong(curOffset.get)
    }

    sql ++= ";"
    Some((sql.result(), params))
  }

  private def hasNextQuery: Boolean =
    batchByAttribute match {
      case Some(attribute) =>
        attribute.getType match {
          case AttributeType.INTEGER | AttributeType.LONG | AttributeType.TIMESTAMP =>
            curLowerBound.longValue <= upperBound.longValue
          case AttributeType.DOUBLE =>
            curLowerBound.doubleValue <= upperBound.doubleValue
          case _ =>
            throw new IllegalArgumentException("Unexpected type: " + attribute.getType)
        }
      case None =>
        val has = !querySent
        querySent = true
        has
    }

  private def addBatchSlidingWindow(sql: StringBuilder): Unit = {
    var nextLowerBound: Number = null
    var isLast = false
    batchByAttribute match {
      case Some(attribute) =>
        attribute.getType match {
          case AttributeType.INTEGER | AttributeType.LONG | AttributeType.TIMESTAMP =>
            nextLowerBound = curLowerBound.longValue + desc.interval
            isLast = nextLowerBound.longValue >= upperBound.longValue
          case AttributeType.DOUBLE =>
            nextLowerBound = curLowerBound.doubleValue + desc.interval
            isLast = nextLowerBound.doubleValue >= upperBound.doubleValue
          case _ =>
            throw new IllegalArgumentException("Unexpected type: " + attribute.getType)
        }
        sql ++= " AND " + attribute.getName + " >= " + batchAttrToString(curLowerBound) +
          " AND " + attribute.getName +
          (if (isLast) " <= " + batchAttrToString(upperBound)
           else " < " + batchAttrToString(nextLowerBound))
      case None =>
        throw new IllegalArgumentException(
          "no valid batchByColumn to iterate: " + desc.batchByColumn.getOrElse("")
        )
    }
    curLowerBound = nextLowerBound
  }

  private def batchAttrToString(value: Number): String =
    batchByAttribute match {
      case Some(attribute) =>
        attribute.getType match {
          case AttributeType.LONG | AttributeType.INTEGER | AttributeType.DOUBLE =>
            String.valueOf(value)
          case AttributeType.TIMESTAMP =>
            "'" + new Timestamp(value.longValue).toString + "'"
          case _ =>
            throw new IllegalArgumentException("Unexpected type: " + attribute.getType)
        }
      case None =>
        throw new IllegalArgumentException(
          "No valid batchByColumn to iterate: " + desc.batchByColumn.getOrElse("")
        )
    }

  // -- bookkeeping queries ---------------------------------------------------

  private def loadTableNames(): Unit = {
    val rowSet = pool
      .preparedQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = ?;")
      .execute(VxTuple.of(desc.database))
      .toCompletionStage
      .toCompletableFuture
      .get()
    val it = rowSet.iterator()
    while (it.hasNext) tableNames += it.next().getString(0)
  }

  private def initBatchColumnBoundaries(): Unit = {
    if (batchByAttribute.isDefined && desc.min.isDefined && desc.max.isDefined) {
      if (desc.min.get.equalsIgnoreCase("auto")) curLowerBound = fetchBoundary("MIN")
      else
        batchByAttribute.get.getType match {
          case AttributeType.TIMESTAMP => curLowerBound = parseTimestamp(desc.min.get).getTime
          case AttributeType.LONG      => curLowerBound = desc.min.get.toLong
          case _ =>
            throw new IllegalArgumentException(
              s"Unsupported type ${batchByAttribute.get.getType}"
            )
        }

      if (desc.max.get.equalsIgnoreCase("auto")) upperBound = fetchBoundary("MAX")
      else
        batchByAttribute.get.getType match {
          case AttributeType.TIMESTAMP => upperBound = parseTimestamp(desc.max.get).getTime
          case AttributeType.LONG      => upperBound = desc.max.get.toLong
          case _ =>
            throw new IllegalArgumentException(
              s"Unsupported type ${batchByAttribute.get.getType}"
            )
        }
    } else {
      throw new IllegalArgumentException(
        s"Missing required progressive configuration, $batchByAttribute, $desc.min or $desc.max."
      )
    }
  }

  private def fetchBoundary(side: String): Number =
    batchByAttribute match {
      case Some(attribute) =>
        val rowSet = pool
          .query("SELECT " + side + "(" + attribute.getName + ") FROM " + desc.table + ";")
          .execute()
          .toCompletionStage
          .toCompletableFuture
          .get()
        val it = rowSet.iterator()
        if (!it.hasNext) {
          val zero: Number = 0
          zero
        } else {
          val row = it.next()
          attribute.getType match {
            case AttributeType.INTEGER =>
              val n: Number = row.getInteger(0); n
            case AttributeType.LONG =>
              val n: Number = row.getLong(0); n
            case AttributeType.TIMESTAMP =>
              val ldt = row.getLocalDateTime(0)
              val n: Number = java.lang.Long.valueOf(Timestamp.valueOf(ldt).getTime); n
            case AttributeType.DOUBLE =>
              val n: Number = row.getDouble(0); n
            case _ =>
              throw new IllegalStateException("Unexpected value: " + attribute.getType)
          }
        }
      case None =>
        val zero: Number = 0
        zero
    }

  // -- row → Tuple -----------------------------------------------------------

  private def buildTupleFromRow(row: Row): Tuple = {
    val builder = Tuple.builder(schema)
    schema.getAttributes.iterator.zipWithIndex.foreach {
      case (attr, i) =>
        val raw = row.getValue(i)
        if (raw == null) builder.add(attr, null)
        else builder.add(attr, parseField(normalizeJavaTime(raw), attr.getType))
    }
    builder.build()
  }

  /**
    * Vert.x returns `LocalDateTime` / `LocalDate` / `LocalTime` for MySQL
    * temporal columns. Normalize to types that [[parseField]] understands.
    */
  private def normalizeJavaTime(value: Any): Any =
    value match {
      case ldt: LocalDateTime => Timestamp.valueOf(ldt)
      case ld: LocalDate      => ld.toString
      case lt: LocalTime      => lt.toString
      case other              => other
    }
}
