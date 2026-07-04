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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ObjectMapperUtilsSpec extends AnyFlatSpec with Matchers {

  private def findWarmupThread(): Option[Thread] = {
    val threads = new Array[Thread](Thread.activeCount() * 2 + 16)
    val count = Thread.enumerate(threads)
    threads.take(count).find(t => t != null && t.getName == "ObjectMapperWarmupForOperatorsThread")
  }

  "warmupObjectMapperForOperatorsSerde" should "spawn the named warmup thread and complete" in {
    noException should be thrownBy ObjectMapperUtils.warmupObjectMapperForOperatorsSerde()
    // the warmup runs a full operator-metadata scan (seconds), so the thread is observable
    // right after start(); assert it was actually spawned, then join so its body runs
    val thread = findWarmupThread()
    thread shouldBe defined
    thread.foreach { t =>
      t.join(60000)
      t.isAlive shouldBe false
    }
  }
}
