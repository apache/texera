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

  "warmupObjectMapperForOperatorsSerde" should "spawn the named warmup thread and complete" in {
    // The method returns the started thread, so we can observe and join it deterministically
    // instead of racing to find it via Thread.enumerate().
    val thread = ObjectMapperUtils.warmupObjectMapperForOperatorsSerde()
    thread.getName shouldBe "ObjectMapperWarmupForOperatorsThread"
    // The warmup runs a full operator-metadata scan (~4-5s cold), so a 1-3s bound would
    // false-fail; 20s still surfaces a genuine hang far faster than the previous 60s.
    thread.join(20000)
    thread.isAlive shouldBe false
  }
}
