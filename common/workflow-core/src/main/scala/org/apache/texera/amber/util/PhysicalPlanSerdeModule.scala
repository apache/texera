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

package org.apache.texera.amber.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.workflow.OutputPort.OutputMode
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.util.serde.{
  OpExecInitInfoDeserializer,
  OpExecInitInfoSerializer,
  OutputModeDeserializer,
  OutputModeSerializer,
  PortIdentityKeyDeserializer,
  PortIdentityKeySerializer
}

/**
  * Reusable Jackson module that teaches an [[ObjectMapper]] how to serialize and deserialize a
  * [[org.apache.texera.amber.core.workflow.PhysicalPlan]] / [[org.apache.texera.amber.core.workflow.PhysicalOp]].
  *
  * `PhysicalOp` carries a few values that Jackson cannot handle out of the box:
  *   - `PortIdentity` used as a map key (serialized as a string "id_internal"),
  *   - the scalapb sealed-oneof `OpExecInitInfo` (tagged by a `kind` discriminator),
  *   - the scalapb enum `OutputPort.OutputMode` (serialized as its integer wire value).
  *
  * The registration lives here (rather than inline in [[JSONUtils]]) so that any other process which
  * needs to round-trip a `PhysicalPlan` over JSON — notably the workflow-compiling-service's
  * Dropwizard object mapper, which only ships `DefaultScalaModule` — can register the exact same
  * serializers and stay byte-for-byte compatible with [[JSONUtils.objectMapper]].
  */
object PhysicalPlanSerdeModule {

  /** Builds a fresh [[SimpleModule]] with the PhysicalPlan serializers/deserializers. */
  def physicalPlanModule: SimpleModule =
    new SimpleModule()
      .addKeySerializer(classOf[PortIdentity], new PortIdentityKeySerializer())
      .addKeyDeserializer(classOf[PortIdentity], new PortIdentityKeyDeserializer())
      .addSerializer(classOf[OpExecInitInfo], new OpExecInitInfoSerializer())
      .addDeserializer(classOf[OpExecInitInfo], new OpExecInitInfoDeserializer())
      .addSerializer(classOf[OutputMode], new OutputModeSerializer())
      .addDeserializer(classOf[OutputMode], new OutputModeDeserializer())

  /**
    * Registers [[physicalPlanModule]] on the given mapper and returns it (same instance), so calls
    * can be chained alongside other `registerModule` invocations.
    */
  def register(mapper: ObjectMapper): mapper.type = {
    mapper.registerModule(physicalPlanModule)
    mapper
  }
}
