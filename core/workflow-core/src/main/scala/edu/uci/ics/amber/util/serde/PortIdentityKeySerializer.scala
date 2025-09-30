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

package edu.uci.ics.amber.util.serde

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.{JsonSerializer, SerializerProvider}
import edu.uci.ics.amber.core.workflow.PortIdentity
import edu.uci.ics.amber.util.serde.PortIdentityKeySerializer.portIdToString

case object PortIdentityKeySerializer {
  def portIdToString(portId: PortIdentity): String = {
    s"${portId.id}_${portId.internal}"
  }
}

class PortIdentityKeySerializer extends JsonSerializer[PortIdentity] {
  override def serialize(
      key: PortIdentity,
      gen: JsonGenerator,
      serializers: SerializerProvider
  ): Unit = {
    // Serialize PortIdentity as a string "id_internal"
    gen.writeFieldName(portIdToString(key))
  }
}
