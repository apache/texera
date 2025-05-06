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

package edu.uci.ics.amber.engine.common

import akka.serialization.{Serialization, Serializers}

object SerializedState {

  val CP_STATE_KEY = "Amber_CPState"
  val DP_STATE_KEY = "Amber_DPState"
  val IN_FLIGHT_MSG_KEY = "Amber_Inflight_Messages"
  val DP_QUEUED_MSG_KEY = "Amber_DP_Queued_Messages"
  val OUTPUT_MSG_KEY = "Amber_Output_Messages"

  def fromObject[T <: AnyRef](obj: T, serialization: Serialization): SerializedState = {
    val bytes = serialization.serialize(obj).get
    val ser = serialization.findSerializerFor(obj)
    val manifest = Serializers.manifestFor(ser, obj)
    SerializedState(bytes, ser.identifier, manifest)
  }
}

case class SerializedState(bytes: Array[Byte], serializerId: Int, manifest: String) {

  def toObject[T <: AnyRef](serialization: Serialization): T = {
    serialization.deserialize(bytes, serializerId, manifest).get.asInstanceOf[T]
  }

  def size(): Long = {
    bytes.length
  }
}
