/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.texera.amber.operator.visualization.gaugeChart

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle

/**
  * A step's bounds are numbers, not text: the operator only ever uses them as
  * `float(...)`, and a non-numeric bound used to be swallowed — the gauge rendered
  * with no steps at all and no explanation. Declaring them numeric lets the form
  * reject a typed-in word before the run.
  *
  * `contentAs` is required: Scala erases `Option[Double]`'s element type, so
  * without it Jackson leaves the raw JSON value inside the Option and the first
  * use throws ClassCastException. It names the boxed class deliberately — the
  * primitive would coerce a blank to 0 instead of None.
  */
class GaugeChartSteps {
  @JsonProperty("start")
  @JsonSchemaTitle("Start")
  @JsonDeserialize(contentAs = classOf[java.lang.Double])
  var start: Option[Double] = None

  @JsonProperty("end")
  @JsonSchemaTitle("End")
  @JsonDeserialize(contentAs = classOf[java.lang.Double])
  var end: Option[Double] = None
}
