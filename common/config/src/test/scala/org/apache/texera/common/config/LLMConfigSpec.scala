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

package org.apache.texera.common.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Spec for [[LLMConfig]]. Reading each value forces resolution from llm.conf, so a renamed or
  * mistyped key surfaces here as a ConfigException. Both values carry a `${?ENV}` override, so
  * exact-value assertions are guarded.
  */
class LLMConfigSpec extends AnyFlatSpec with Matchers {

  private def ifUnset(name: String)(assertion: => Any): Unit =
    if (!sys.env.contains(name) && !sys.props.contains(name)) assertion

  "LLMConfig" should "resolve the LiteLLM base URL and master key from llm.conf" in {
    ifUnset("LITELLM_BASE_URL")(LLMConfig.baseUrl shouldBe "http://0.0.0.0:4000")
    ifUnset("LITELLM_MASTER_KEY")(LLMConfig.masterKey shouldBe "")
  }
}
