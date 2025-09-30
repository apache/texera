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

import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import io.github.redouane59.twitter.TwitterClient
import io.github.redouane59.twitter.signature.TwitterCredentials

abstract class TwitterSourceOpExec(
    descString: String
) extends SourceOperatorExecutor {
  private val desc: TwitterSourceOpDesc =
    objectMapper.readValue(descString, classOf[TwitterSourceOpDesc])
  // batch size for each API request defined by Twitter
  //  500 is the maximum tweets for each request
  val TWITTER_API_BATCH_SIZE_MAX = 500

  //  10 is the minimal tweets for each request
  // val TWITTER_API_BATCH_SIZE_MIN = 10

  //  however, when using batch size < 100, could cause using different
  //  twitter endpoints which has different rate limit.
  //  (related to redouane59/twitteredV2.5)
  //  thus, in practice, we use 100 as the min batch size.
  val TWITTER_API_BATCH_SIZE_MIN = 100

  var twitterClient: TwitterClient = _

  override def open(): Unit = {
    twitterClient = new TwitterClient(
      TwitterCredentials
        .builder()
        .apiKey(desc.apiKey)
        .apiSecretKey(desc.apiSecretKey)
        .build()
    )
    twitterClient.setAutomaticRetry(!desc.stopWhenRateLimited)
  }

  override def close(): Unit = {}
}
