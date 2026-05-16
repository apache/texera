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

package org.apache.texera.amber.operator.aiagent

import org.scalatest.flatspec.AnyFlatSpec

class AIAgentResponseCacheSpec extends AnyFlatSpec {

  "AIAgentResponseCache.key" should "include the API key signature" in {
    val baseArgs = (
      "openai/gpt-4o-mini",
      0.7,
      "system",
      "prompt",
      "read_url",
      "text"
    )

    val keyA = AIAgentResponseCache.key(
      baseArgs._1,
      baseArgs._2,
      AIAgentResponseCache.sha256("key-a"),
      baseArgs._3,
      baseArgs._4,
      baseArgs._5,
      baseArgs._6
    )
    val keyB = AIAgentResponseCache.key(
      baseArgs._1,
      baseArgs._2,
      AIAgentResponseCache.sha256("key-b"),
      baseArgs._3,
      baseArgs._4,
      baseArgs._5,
      baseArgs._6
    )

    assert(keyA != keyB)
  }

  it should "be instance-local" in {
    val cacheA = new AIAgentResponseCache()
    val cacheB = new AIAgentResponseCache()
    cacheA.put("k", "v")

    assert(cacheA.get("k").contains("v"))
    assert(cacheB.get("k").isEmpty)
  }
}
