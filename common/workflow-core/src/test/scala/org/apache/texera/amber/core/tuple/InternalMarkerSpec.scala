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

package org.apache.texera.amber.core.tuple

import org.apache.texera.amber.core.workflow.PortIdentity
import org.scalatest.flatspec.AnyFlatSpec

class InternalMarkerSpec extends AnyFlatSpec {

  "FinalizePort" should "carry the configured portId and direction" in {
    val marker = FinalizePort(PortIdentity(3), input = false)
    assert(marker.portId == PortIdentity(3))
    assert(!marker.input)
  }

  it should "be a TupleLike with empty getFields and zero inMemSize" in {
    val marker = FinalizePort(PortIdentity(0), input = true)
    assert(marker.isInstanceOf[TupleLike])
    assert(marker.getFields.isEmpty)
    assert(marker.inMemSize == 0L)
  }

  it should "respect case-class equality on the constructor arguments" in {
    assert(
      FinalizePort(PortIdentity(0), input = true) == FinalizePort(PortIdentity(0), input = true)
    )
    assert(
      FinalizePort(PortIdentity(0), input = true) != FinalizePort(PortIdentity(0), input = false)
    )
    assert(
      FinalizePort(PortIdentity(0), input = true) != FinalizePort(PortIdentity(1), input = true)
    )
  }

  "FinalizeExecutor" should "be a TupleLike with empty getFields and zero inMemSize" in {
    val marker = FinalizeExecutor()
    assert(marker.isInstanceOf[TupleLike])
    assert(marker.getFields.isEmpty)
    assert(marker.inMemSize == 0L)
  }

  it should "be equal to another FinalizeExecutor instance (no-field case class)" in {
    assert(FinalizeExecutor() == FinalizeExecutor())
  }

  "InternalMarker" should "be distinguishable via pattern matching" in {
    val markers: List[TupleLike] = List(
      FinalizePort(PortIdentity(0), input = true),
      FinalizeExecutor()
    )
    val classified = markers.map {
      case FinalizePort(_, _)    => "port"
      case FinalizeExecutor()    => "executor"
      case other: InternalMarker => s"other-marker:$other"
      case other                 => s"non-marker:$other"
    }
    assert(classified == List("port", "executor"))
  }
}
