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

  "VFSURIFactory.createPortBaseURI" should "include workflow, execution, and port segments without a resource type" in {
    val baseURI = VFSURIFactory.createPortBaseURI(workflowId, executionId, portId)
    assert(baseURI.getScheme == VFSURIFactory.VFS_FILE_URI_SCHEME)
    val path = baseURI.getPath
    assert(path.contains("/wid/7"))
    assert(path.contains("/eid/11"))
    assert(path.contains("/globalportid/"))
    assert(!path.endsWith("/result"))
    assert(!path.endsWith("/state"))
  }

  "VFSURIFactory.resultURI / stateURI" should "append the resource segment and round-trip through decodeURI" in {
    val baseURI = VFSURIFactory.createPortBaseURI(workflowId, executionId, portId)
    val resultURI = VFSURIFactory.resultURI(baseURI)
    val stateURI = VFSURIFactory.stateURI(baseURI)
    assert(resultURI.getPath.endsWith("/result"))
    assert(stateURI.getPath.endsWith("/state"))

    val VFSUriComponents(wid, eid, globalPortIdOpt, resourceType, _) =
      VFSURIFactory.decodeURI(resultURI)
    assert(wid == workflowId)
    assert(eid == executionId)
    assert(globalPortIdOpt.contains(portId))
    assert(resourceType == VFSResourceType.RESULT)
    assert(VFSURIFactory.decodeURI(stateURI).resourceType == VFSResourceType.STATE)
  }

  "VFSURIFactory.createRuntimeStatisticsURI" should "produce a runtimeStatistics URI without an opid segment" in {
    val uri = VFSURIFactory.createRuntimeStatisticsURI(workflowId, executionId)
    val path = uri.getPath
    assert(path.endsWith("/runtimestatistics"))
    assert(!path.contains("/opid/"))

    val VFSUriComponents(wid, eid, globalPortIdOpt, resourceType, _) = VFSURIFactory.decodeURI(uri)
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
    val VFSUriComponents(wid, eid, globalPortIdOpt, resourceType, _) = VFSURIFactory.decodeURI(uri)
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

  it should "reject a URI where a required key is the final segment with no value" in {
    // "wid" is present but is the last segment (index + 1 >= segments.length)
    assertThrows[IllegalArgumentException] {
      VFSURIFactory.decodeURI(new URI("vfs:///eid/2/wid"))
    }
  }

  "decodeURI" should "report the warehouse from a leading /wh/<name> segment" in {
    val uri =
      VFSURIFactory.createPortBaseURI(
        workflowId,
        executionId,
        portId,
        warehouse = Some("user-2-foo")
      )
    assert(uri.getPath.startsWith("/wh/user-2-foo/"))
    // A base URI has no resource segment, so decode the derived result URI.
    assert(
      VFSURIFactory.decodeURI(VFSURIFactory.resultURI(uri)).warehouse.contains("user-2-foo")
    )
  }

  it should "return None when no /wh/ segment is present (non-BYO URIs are unchanged)" in {
    val uri = VFSURIFactory.createPortBaseURI(workflowId, executionId, portId)
    assert(!uri.getPath.contains("/wh/"))
    assert(VFSURIFactory.decodeURI(VFSURIFactory.resultURI(uri)).warehouse.isEmpty)
  }

  it should "only honour a LEADING wh segment, never one deeper in the path" in {
    // A `wh` appearing later -- e.g. inside an operator id -- must not select a
    // warehouse: it would disagree with DocumentFactory, which strips only a
    // leading `wh/<name>/`, and would route the write to another user's warehouse.
    assert(
      VFSURIFactory
        .decodeURI(new URI("vfs:///wid/1/eid/2/opid/a/wh/victim/b/consolemessages"))
        .warehouse
        .isEmpty
    )
    // An operator literally named `wh` is likewise not a warehouse.
    assert(
      VFSURIFactory
        .decodeURI(new URI("vfs:///wid/1/eid/2/opid/wh/consolemessages"))
        .warehouse
        .isEmpty
    )
  }

  it should "not let a percent-encoded slash in the name forge extra segments" in {
    // Decoded before splitting, this path would read as /wh/a/wid/999/... -- handing
    // back "a" as the warehouse and shifting which `wid` the parser sees. Splitting
    // the raw path keeps `%2F` inside its own segment, and the name is then rejected
    // as illegal, so the URI resolves to no warehouse rather than to a wrong one.
    assert(
      VFSURIFactory
        .decodeURI(new URI("vfs:///wh/a%2Fwid%2F999/wid/1/eid/2/result"))
        .warehouse
        .isEmpty
    )
    assert(
      VFSURIFactory
        .decodeURI(new URI("vfs:///wh/user-2%2Dfoo/wid/7/eid/3/result"))
        .warehouse
        .isEmpty
    )
  }

  it should "locate wid/eid by raw segment, so an encoded slash cannot shift them" in {
    // Decoded before splitting, this path reads as /wh/a/wid/999/wid/1/... and the
    // key search finds the injected `wid` first -- resolving to execution 999 and
    // landing this execution's data under another's storage key. Python's
    // decode_uri splits the raw path, so decoding here would also make the two
    // languages disagree about which execution a URI belongs to.
    val components =
      VFSURIFactory.decodeURI(new URI("vfs:///wh/a%2Fwid%2F999/wid/1/eid/2/result"))
    assert(components.workflowId == WorkflowIdentity(1))
    assert(components.executionId == ExecutionIdentity(2))
  }

  "VFSURIFactory" should "reject an operatorId containing '/' rather than let it forge URI segments" in {
    assertThrows[IllegalArgumentException] {
      VFSURIFactory.createConsoleMessagesURI(
        workflowId,
        executionId,
        OperatorIdentity("a/wh/victim/b")
      )
    }
  }

  it should "reject a warehouse name that is not safe as a URI path segment" in {
    Seq("a/b", "a%2Fb", "", "-lead", "sp ace").foreach { bad =>
      withClue(s"warehouse name '$bad' should be rejected: ") {
        assertThrows[IllegalArgumentException] {
          VFSURIFactory.createPortBaseURI(workflowId, executionId, portId, Some(bad))
        }
      }
    }
  }

  "A warehouse-scoped URI" should
    "still round-trip through decodeURI (wid/eid/port/resource resolved despite the /wh/ prefix)" in {
    val base =
      VFSURIFactory.createPortBaseURI(
        workflowId,
        executionId,
        portId,
        warehouse = Some("user-2-foo")
      )
    val resultURI = VFSURIFactory.resultURI(base)
    assert(VFSURIFactory.decodeURI(resultURI).warehouse.contains("user-2-foo"))

    val components = VFSURIFactory.decodeURI(resultURI)
    assert(components.workflowId == workflowId)
    assert(components.executionId == executionId)
    assert(components.globalPortId.contains(portId))
    assert(components.resourceType == VFSResourceType.RESULT)
  }
}
