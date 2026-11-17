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

package org.apache.texera.amber.operator.source.apis.twitter

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaDescription, JsonSchemaTitle}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{OutputPort, PhysicalOp, SchemaPropagationFunc}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.source.SourceOperatorDescriptor
import org.apache.texera.amber.util.JSONUtils.objectMapper

@deprecated("Twitter source operator is no longer executable.", "1.1.0-incubating")
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

  protected def tweetSourcePhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      executorClassName: String
  ): PhysicalOp =
    // TODO: use multiple workers
    PhysicalOp
      .sourcePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(executorClassName, objectMapper.writeValueAsString(this))
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> sourceSchema()))
      )

  override def sourceSchema(): Schema = TweetSchema.create()
}

private[twitter] object TweetSchema {
  def create(): Schema = {
    Schema()
      .add("id", AttributeType.STRING)
      .add("text", AttributeType.STRING)
      .add("created_at", AttributeType.TIMESTAMP)
      .add("lang", AttributeType.STRING)
      .add("tweet_type", AttributeType.STRING)
      .add("place_id", AttributeType.STRING)
      .add("place_coordinate", AttributeType.STRING)
      .add("in_reply_to_status_id", AttributeType.STRING)
      .add("in_reply_to_user_id", AttributeType.STRING)
      .add("like_count", AttributeType.LONG)
      .add("quote_count", AttributeType.LONG)
      .add("reply_count", AttributeType.LONG)
      .add("retweet_count", AttributeType.LONG)
      .add("hashtags", AttributeType.STRING)
      .add("symbols", AttributeType.STRING)
      .add("urls", AttributeType.STRING)
      .add("mentions", AttributeType.STRING)
      .add("user_id", AttributeType.STRING)
      .add("user_created_at", AttributeType.TIMESTAMP)
      .add("user_name", AttributeType.STRING)
      .add("user_display_name", AttributeType.STRING)
      .add("user_lang", AttributeType.STRING)
      .add("user_description", AttributeType.STRING)
      .add("user_followers_count", AttributeType.LONG)
      .add("user_following_count", AttributeType.LONG)
      .add("user_tweet_count", AttributeType.LONG)
      .add("user_listed_count", AttributeType.LONG)
      .add("user_location", AttributeType.STRING)
      .add("user_url", AttributeType.STRING)
      .add("user_profile_image_url", AttributeType.STRING)
      .add("user_pinned_tweet_id", AttributeType.STRING)
      .add("user_protected", AttributeType.BOOLEAN)
      .add("user_verified", AttributeType.BOOLEAN)
  }
}
