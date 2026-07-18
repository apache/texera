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

package org.apache.texera.amber.operator.visualization.bulletChart

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString

/**
  * Defines a step range used for qualitative segments in the Bullet Chart.
  */

class BulletChartStepDefinition {
  @JsonProperty("start")
  @JsonSchemaTitle("Start")
  var start: EncodableString = ""

  @JsonProperty("end")
  @JsonSchemaTitle("End")
  var end: EncodableString = ""

  // @JsonCreator on the two-arg constructor (params carry @JsonProperty) so
  // Jackson deserializes step objects via it, while the no-arg primary ctor is
  // kept for callers that build-then-set (e.g. BulletChartOpDescSpec). Upstream
  // merged a BulletChartStepDefinitionSpec that asserts this @JsonCreator ctor
  // exists; our fork's class predated it without the annotation.
  @JsonCreator
  def this(
      @JsonProperty("start") start: EncodableString,
      @JsonProperty("end") end: EncodableString
  ) = {
    this()
    this.start = start
    this.end = end
  }
}
