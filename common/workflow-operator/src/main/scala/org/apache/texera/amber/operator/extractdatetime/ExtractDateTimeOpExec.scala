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

package org.apache.texera.amber.operator.extractdatetime

import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}
import org.apache.texera.amber.operator.map.MapOpExec
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.temporal.IsoFields

class ExtractDateTimeOpExec(descString: String) extends MapOpExec {

  private val desc: ExtractDateTimeOpDesc =
    objectMapper.readValue(descString, classOf[ExtractDateTimeOpDesc])

  this.setMapFunc(extract)

  private def extract(tuple: Tuple): TupleLike = {
    val moment = Option(tuple.getField[Timestamp](desc.attribute)).map(_.toLocalDateTime)
    // A null timestamp has no fields, so every column this operator adds is empty
    // for that row rather than the row being dropped: the operator adds columns and
    // says nothing about which rows belong.
    val added = Option(desc.fields)
      .getOrElse(List.empty)
      .filter(_ != null)
      .distinct
      .map(field => moment.map(m => Int.box(read(m, field))).orNull)
    TupleLike(tuple.getFields ++ added)
  }

  /** One field of a moment, in the ISO reading the exported Python also states. */
  private def read(moment: LocalDateTime, field: DateTimeField): Int =
    field match {
      case DateTimeField.YEAR         => moment.getYear
      case DateTimeField.QUARTER      => moment.get(IsoFields.QUARTER_OF_YEAR)
      case DateTimeField.MONTH        => moment.getMonthValue
      case DateTimeField.DAY          => moment.getDayOfMonth
      case DateTimeField.DAY_OF_WEEK  => moment.getDayOfWeek.getValue
      case DateTimeField.DAY_OF_YEAR  => moment.getDayOfYear
      case DateTimeField.WEEK_OF_YEAR => moment.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)
      case DateTimeField.HOUR         => moment.getHour
      case DateTimeField.MINUTE       => moment.getMinute
      case DateTimeField.SECOND       => moment.getSecond
    }
}
