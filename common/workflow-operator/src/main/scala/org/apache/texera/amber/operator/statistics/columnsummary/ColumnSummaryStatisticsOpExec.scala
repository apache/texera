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

package org.apache.texera.amber.operator.statistics.columnsummary

import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Tuple, TupleLike}

import scala.collection.mutable

class ColumnSummaryStatisticsOpExec(descString: String) extends OperatorExecutor {

  private case class ColumnStats(
      columnName: String,
      dataType: AttributeType,
      var rowCount: Int = 0,
      var nullCount: Int = 0,
      var nonNullCount: Int = 0,
      var minValue: Option[Double] = None,
      var maxValue: Option[Double] = None,
      var sum: Double = 0.0,
      var numericCount: Int = 0
  )

  private val statsByColumn = mutable.LinkedHashMap[String, ColumnStats]()

  override def open(): Unit = {
    statsByColumn.clear()
  }

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    initializeStatsIfNeeded(tuple)

    tuple.getSchema.getAttributes.foreach { attribute =>
      val columnName = attribute.getName
      val stats = statsByColumn(columnName)
      val value = tuple.getField[Any](columnName)

      stats.rowCount += 1

      if (value == null) {
        stats.nullCount += 1
      } else {
        stats.nonNullCount += 1

        toDoubleIfNumeric(value).foreach { numericValue =>
          stats.numericCount += 1
          stats.sum += numericValue
          stats.minValue = Some(stats.minValue.fold(numericValue)(Math.min(_, numericValue)))
          stats.maxValue = Some(stats.maxValue.fold(numericValue)(Math.max(_, numericValue)))
        }
      }
    }

    Iterator.empty
  }

  override def onFinish(port: Int): Iterator[TupleLike] = {
    statsByColumn.valuesIterator.map { stats =>
      val meanValue =
        if (stats.numericCount > 0) {
          Double.box(stats.sum / stats.numericCount)
        } else {
          null
        }

      TupleLike(
        "columnName" -> stats.columnName,
        "dataType" -> stats.dataType.name(),
        "rowCount" -> Int.box(stats.rowCount),
        "nullCount" -> Int.box(stats.nullCount),
        "nonNullCount" -> Int.box(stats.nonNullCount),
        "minValue" -> stats.minValue.map(_.toString).orNull,
        "maxValue" -> stats.maxValue.map(_.toString).orNull,
        "meanValue" -> meanValue
      )
    }
  }

  override def close(): Unit = {
    statsByColumn.clear()
  }

  private def initializeStatsIfNeeded(tuple: Tuple): Unit = {
    if (statsByColumn.nonEmpty) {
      return
    }

    tuple.getSchema.getAttributes.foreach { attribute: Attribute =>
      statsByColumn.put(
        attribute.getName,
        ColumnStats(
          columnName = attribute.getName,
          dataType = attribute.getType
        )
      )
    }
  }

  private def toDoubleIfNumeric(value: Any): Option[Double] = {
    value match {
      case v: java.lang.Byte    => Some(v.doubleValue())
      case v: java.lang.Short   => Some(v.doubleValue())
      case v: java.lang.Integer => Some(v.doubleValue())
      case v: java.lang.Long    => Some(v.doubleValue())
      case v: java.lang.Float   => Some(v.doubleValue())
      case v: java.lang.Double  => Some(v.doubleValue())
      case v: Byte              => Some(v.toDouble)
      case v: Short             => Some(v.toDouble)
      case v: Int               => Some(v.toDouble)
      case v: Long              => Some(v.toDouble)
      case v: Float             => Some(v.toDouble)
      case v: Double            => Some(v)
      case _                    => None
    }
  }
}