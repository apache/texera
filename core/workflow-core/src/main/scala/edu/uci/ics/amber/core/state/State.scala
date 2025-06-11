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

package edu.uci.ics.amber.core.state

import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import scala.collection.mutable

final case class State(tuple: Option[Tuple] = None, passToAllDownstream: Boolean = false) {
  val data: mutable.Map[String, (AttributeType, Any)] = mutable.LinkedHashMap()
  add("passToAllDownstream", passToAllDownstream, AttributeType.BOOLEAN)
  if (tuple.isDefined) {
    tuple.get.getSchema.getAttributes.foreach { attribute =>
      add(attribute.getName, tuple.get.getField(attribute.getName), attribute.getType)
    }
  }

  def add(key: String, value: Any, valueType: AttributeType): Unit =
    data.put(key, (valueType, value))

  def get(key: String): Any = data(key)._2

  def isPassToAllDownstream: Boolean = get("passToAllDownstream").asInstanceOf[Boolean]

  def apply(key: String): Any = get(key)

  def toTuple: Tuple =
    Tuple
      .builder(
        Schema(data.map {
          case (name, (attrType, _)) =>
            new Attribute(name, attrType)
        }.toList)
      )
      .addSequentially(data.values.map(_._2).toArray)
      .build()

  override def toString: String =
    data.map { case (key, (_, value)) => s"$key: $value" }.mkString(", ")
}
