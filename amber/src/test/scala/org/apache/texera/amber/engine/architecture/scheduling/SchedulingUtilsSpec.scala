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

package org.apache.texera.amber.engine.architecture.scheduling

import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.PhysicalOp
import org.jgrapht.graph.DirectedAcyclicGraph
import org.scalatest.flatspec.AnyFlatSpec

import scala.jdk.CollectionConverters.CollectionHasAsScala

class SchedulingUtilsSpec extends AnyFlatSpec {

  private def region(regionId: Long, opId: String): Region = {
    val physicalOp = PhysicalOp(
      PhysicalOpIdentity(OperatorIdentity(opId), "main"),
      WorkflowIdentity(0),
      ExecutionIdentity(0),
      OpExecInitInfo.Empty
    )
    Region(RegionIdentity(regionId), Set(physicalOp), Set.empty)
  }

  private def newGraph(): DirectedAcyclicGraph[Region, RegionLink] =
    new DirectedAcyclicGraph[Region, RegionLink](classOf[RegionLink])

  "SchedulingUtils.replaceVertex" should "replace an isolated vertex with no incident edges" in {
    val graph = newGraph()
    val oldVertex = region(1, "a")
    val newVertex = region(1, "a-prime")
    graph.addVertex(oldVertex)

    SchedulingUtils.replaceVertex(graph, oldVertex, newVertex)

    assert(!graph.containsVertex(oldVertex))
    assert(graph.containsVertex(newVertex))
    assert(graph.edgeSet().isEmpty)
  }

  it should "rewrite outgoing edges to originate from the new vertex" in {
    val graph = newGraph()
    val oldVertex = region(1, "a")
    val downstream = region(2, "b")
    val newVertex = region(1, "a-prime")
    graph.addVertex(oldVertex)
    graph.addVertex(downstream)
    graph.addEdge(oldVertex, downstream, RegionLink(oldVertex.id, downstream.id))

    SchedulingUtils.replaceVertex(graph, oldVertex, newVertex)

    assert(!graph.containsVertex(oldVertex))
    assert(graph.containsVertex(newVertex))
    val outgoing = graph.outgoingEdgesOf(newVertex).asScala.toList
    assert(outgoing.size == 1)
    assert(graph.getEdgeTarget(outgoing.head) == downstream)
    assert(outgoing.head == RegionLink(newVertex.id, downstream.id))
  }

  it should "rewrite incoming edges to terminate at the new vertex" in {
    val graph = newGraph()
    val upstream = region(0, "u")
    val oldVertex = region(1, "a")
    val newVertex = region(1, "a-prime")
    graph.addVertex(upstream)
    graph.addVertex(oldVertex)
    graph.addEdge(upstream, oldVertex, RegionLink(upstream.id, oldVertex.id))

    SchedulingUtils.replaceVertex(graph, oldVertex, newVertex)

    assert(!graph.containsVertex(oldVertex))
    val incoming = graph.incomingEdgesOf(newVertex).asScala.toList
    assert(incoming.size == 1)
    assert(graph.getEdgeSource(incoming.head) == upstream)
    assert(incoming.head == RegionLink(upstream.id, newVertex.id))
  }

  it should "preserve both upstream and downstream edges in a chain" in {
    val graph = newGraph()
    val upstream = region(0, "u")
    val oldVertex = region(1, "a")
    val downstream = region(2, "d")
    val newVertex = region(1, "a-prime")
    graph.addVertex(upstream)
    graph.addVertex(oldVertex)
    graph.addVertex(downstream)
    graph.addEdge(upstream, oldVertex, RegionLink(upstream.id, oldVertex.id))
    graph.addEdge(oldVertex, downstream, RegionLink(oldVertex.id, downstream.id))

    SchedulingUtils.replaceVertex(graph, oldVertex, newVertex)

    assert(graph.vertexSet().asScala.toSet == Set(upstream, newVertex, downstream))
    assert(
      graph.edgeSet().asScala.toSet ==
        Set(
          RegionLink(upstream.id, newVertex.id),
          RegionLink(newVertex.id, downstream.id)
        )
    )
  }

  it should "leave the graph unchanged when old and new vertices are equal" in {
    val graph = newGraph()
    val upstream = region(0, "u")
    val vertex = region(1, "a")
    val downstream = region(2, "d")
    graph.addVertex(upstream)
    graph.addVertex(vertex)
    graph.addVertex(downstream)
    graph.addEdge(upstream, vertex, RegionLink(upstream.id, vertex.id))
    graph.addEdge(vertex, downstream, RegionLink(vertex.id, downstream.id))

    SchedulingUtils.replaceVertex(graph, vertex, vertex)

    assert(graph.vertexSet().asScala.toSet == Set(upstream, vertex, downstream))
    assert(graph.edgeSet().size == 2)
  }
}
