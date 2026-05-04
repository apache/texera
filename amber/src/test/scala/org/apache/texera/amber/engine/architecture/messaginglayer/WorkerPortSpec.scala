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

package org.apache.texera.amber.engine.architecture.messaginglayer

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable

class WorkerPortSpec extends AnyFlatSpec {

  private val schema: Schema = Schema().add(new Attribute("v", AttributeType.INTEGER))

  "WorkerPort" should "default to an empty channel set and not-completed state" in {
    val p = WorkerPort(schema)
    assert(p.schema == schema)
    assert(p.channels.isEmpty)
    assert(!p.completed)
  }

  it should "carry the channel set provided at construction" in {
    val cid =
      ChannelIdentity(ActorVirtualIdentity("a"), ActorVirtualIdentity("b"), isControl = false)
    val p = WorkerPort(schema, mutable.Set(cid))
    assert(p.channels == mutable.Set(cid))
  }

  it should "allow `completed` to be flipped to true" in {
    val p = WorkerPort(schema)
    p.completed = true
    assert(p.completed)
  }

  it should "allow channels to be appended after construction" in {
    val p = WorkerPort(schema)
    val cid =
      ChannelIdentity(ActorVirtualIdentity("a"), ActorVirtualIdentity("b"), isControl = false)
    p.channels += cid
    assert(p.channels.contains(cid))
  }

  it should "treat distinct instances with the same fields as case-class equal" in {
    val cid =
      ChannelIdentity(ActorVirtualIdentity("a"), ActorVirtualIdentity("b"), isControl = false)
    val a = WorkerPort(schema, mutable.Set(cid), completed = true)
    val b = WorkerPort(schema, mutable.Set(cid), completed = true)
    assert(a == b)
  }
}
