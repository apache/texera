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

package org.apache.texera.amber.core.storage

import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{GlobalPortIdentity, PortIdentity}
import org.scalatest.flatspec.AnyFlatSpec

import java.net.URI

class VFSURIFactorySpec extends AnyFlatSpec {

  private val workflowId = WorkflowIdentity(7L)
  private val executionId = ExecutionIdentity(11L)
  private val operatorId = OperatorIdentity("opA")
  private val portId =
    GlobalPortIdentity(
      PhysicalOpIdentity(operatorId, "main"),
      PortIdentity(0),
      input = true
    )

  "VFSURIFactory.createResultURI" should "include workflow, execution, port, and the result resource type" in {
    val uri = VFSURIFactory.createResultURI(workflowId, executionId, portId)
    assert(uri.getScheme == VFSURIFactory.VFS_FILE_URI_SCHEME)
    val path = uri.getPath
    assert(path.contains("/wid/7"))
    assert(path.contains("/eid/11"))
    assert(path.contains("/globalportid/"))
    assert(path.endsWith("/result"))
  }

  it should "round-trip through decodeURI" in {
    val uri = VFSURIFactory.createResultURI(workflowId, executionId, portId)
    val (wid, eid, globalPortIdOpt, resourceType) = VFSURIFactory.decodeURI(uri)
    assert(wid == workflowId)
    assert(eid == executionId)
    assert(globalPortIdOpt.contains(portId))
    assert(resourceType == VFSResourceType.RESULT)
  }

  "VFSURIFactory.createRuntimeStatisticsURI" should "produce a runtimeStatistics URI without an opid segment" in {
    val uri = VFSURIFactory.createRuntimeStatisticsURI(workflowId, executionId)
    val path = uri.getPath
    assert(path.endsWith("/runtimestatistics"))
    assert(!path.contains("/opid/"))

    val (wid, eid, globalPortIdOpt, resourceType) = VFSURIFactory.decodeURI(uri)
    assert(wid == workflowId)
    assert(eid == executionId)
    assert(globalPortIdOpt.isEmpty)
    assert(resourceType == VFSResourceType.RUNTIME_STATISTICS)
  }

  "VFSURIFactory.createConsoleMessagesURI" should "embed the operator id and the consoleMessages resource type" in {
    val uri = VFSURIFactory.createConsoleMessagesURI(workflowId, executionId, operatorId)
    val path = uri.getPath
    assert(path.contains(s"/opid/${operatorId.id}"))
    assert(path.endsWith("/consolemessages"))

    // The current `decodeURI` does not extract the operator id (it has no
    // "opid" branch), so we only round-trip wid/eid/resourceType here.
    val (wid, eid, globalPortIdOpt, resourceType) = VFSURIFactory.decodeURI(uri)
    assert(wid == workflowId)
    assert(eid == executionId)
    assert(globalPortIdOpt.isEmpty)
    assert(resourceType == VFSResourceType.CONSOLE_MESSAGES)
  }

  "VFSURIFactory.decodeURI" should "reject URIs with a non-vfs scheme" in {
    assertThrows[IllegalArgumentException] {
      VFSURIFactory.decodeURI(new URI("http:///wid/1/eid/1/result"))
    }
  }

  it should "reject URIs missing required segments" in {
    assertThrows[IllegalArgumentException] {
      VFSURIFactory.decodeURI(new URI("vfs:///wid/1/result"))
    }
  }

  it should "reject URIs whose final segment is not a known resource type" in {
    assertThrows[IllegalArgumentException] {
      VFSURIFactory.decodeURI(new URI("vfs:///wid/1/eid/2/notarealresource"))
    }
  }
}
