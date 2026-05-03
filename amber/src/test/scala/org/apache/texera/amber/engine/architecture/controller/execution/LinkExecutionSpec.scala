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

package org.apache.texera.amber.engine.architecture.controller.execution

import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.scalatest.flatspec.AnyFlatSpec

class LinkExecutionSpec extends AnyFlatSpec {

  private def channelId(from: String, to: String, isControl: Boolean = false): ChannelIdentity =
    ChannelIdentity(ActorVirtualIdentity(from), ActorVirtualIdentity(to), isControl)

  "LinkExecution" should "have no channel executions when freshly constructed" in {
    val link = LinkExecution()
    assert(link.getAllChannelExecutions.isEmpty)
  }

  "LinkExecution.initChannelExecution" should "register a new ChannelExecution for the given channel id" in {
    val link = LinkExecution()
    val cid = channelId("a", "b")
    link.initChannelExecution(cid)

    val all = link.getAllChannelExecutions.toMap
    assert(all.contains(cid))
    assert(all(cid) == ChannelExecution())
  }

  it should "throw an AssertionError if called twice for the same channel id" in {
    val link = LinkExecution()
    val cid = channelId("a", "b")
    link.initChannelExecution(cid)
    assertThrows[AssertionError] {
      link.initChannelExecution(cid)
    }
  }

  it should "track multiple distinct channel executions" in {
    val link = LinkExecution()
    val c1 = channelId("a", "b")
    val c2 = channelId("a", "b", isControl = true)
    val c3 = channelId("a", "c")

    link.initChannelExecution(c1)
    link.initChannelExecution(c2)
    link.initChannelExecution(c3)

    assert(link.getAllChannelExecutions.toMap.keySet == Set(c1, c2, c3))
  }
}
