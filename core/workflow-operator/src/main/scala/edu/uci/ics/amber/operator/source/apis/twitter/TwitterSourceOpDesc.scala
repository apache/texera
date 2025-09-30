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

package edu.uci.ics.amber.operator.source.apis.twitter

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaDescription, JsonSchemaTitle}
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.source.SourceOperatorDescriptor
import edu.uci.ics.amber.core.workflow.OutputPort

abstract class TwitterSourceOpDesc extends SourceOperatorDescriptor {

  @JsonIgnore
  val APIName: Option[String] = None

  @JsonProperty(required = true)
  @JsonSchemaTitle("API Key")
  var apiKey: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("API Secret Key")
  var apiSecretKey: String = _

  @JsonProperty(required = true, defaultValue = "false")
  @JsonSchemaTitle("Stop Upon Rate Limit")
  @JsonSchemaDescription("Stop when hitting rate limit?")
  var stopWhenRateLimited: Boolean = false

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      userFriendlyName = s"Twitter ${APIName.get} API",
      operatorDescription = s"Retrieve data from Twitter ${APIName.get} API",
      OperatorGroupConstants.API_GROUP,
      inputPorts = List.empty,
      outputPorts = List(OutputPort())
    )
  }

}
