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

package org.apache.texera.service.util

import io.fabric8.kubernetes.api.model.{Container, Node, Pod, PodSpec, Quantity}
import io.fabric8.kubernetes.client.readiness.Readiness

import scala.jdk.CollectionConverters._

/** CPU is in millicores, memory in bytes and GPU in devices. `available` is never below zero. */
case class ResourceCapacity(total: Long, allocated: Long, available: Long)

case class ClusterCapacity(cpu: ResourceCapacity, memory: ResourceCapacity, gpu: ResourceCapacity)

object ClusterCapacity {

  // Quantities in base units (cores, bytes, devices), keyed by Kubernetes resource name.
  private type Amounts = Map[String, BigDecimal]

  private val Cpu = "cpu"
  private val Memory = "memory"
  private val MillicoresPerCore = 1000

  // Pods in these phases hold nothing on their node.
  private val FinishedPhases = Set("Succeeded", "Failed")

  /**
    * Sums what the nodes offer against the requests the scheduler has handed out, the "Allocated
    * resources" of `kubectl describe node`, so `available` bounds what a new unit can claim. Live
    * usage is not used: it reads below what is committed and has no GPU metric.
    *
    * Not modelled, so `available` is an upper bound: per-node fit, the pool's ResourceQuota, node
    * placement (taints, selectors, runtime classes), pod-level `spec.resources`, and native
    * sidecars, which the scheduler adds to the app containers, so a pod using one is undercharged.
    */
  def fromNodesAndPods(
      nodes: Iterable[Node],
      pods: Iterable[Pod],
      gpuResourceKey: String
  ): ClusterCapacity = {
    val schedulable = nodes.filter(isSchedulable)
    val nodeNames = schedulable.map(_.getMetadata.getName).toSet

    val total = sum(schedulable.map(node => amounts(node.getStatus.getAllocatable)))
    val allocated = sum(pods.flatMap(pod => occupiedRequests(pod, nodeNames)))

    def capacity(resource: String, unitsPerAmount: Int): ResourceCapacity = {
      val totalUnits = toUnits(total, resource, unitsPerAmount)
      val allocatedUnits = toUnits(allocated, resource, unitsPerAmount)
      ResourceCapacity(totalUnits, allocatedUnits, (totalUnits - allocatedUnits).max(0))
    }
    ClusterCapacity(
      cpu = capacity(Cpu, MillicoresPerCore),
      memory = capacity(Memory, 1),
      gpu = capacity(gpuResourceKey, 1)
    )
  }

  // A cordoned or not Ready node takes no new pods, so it cannot hold a new unit.
  private def isSchedulable(node: Node): Boolean = {
    val cordoned =
      Option(node.getSpec).flatMap(spec => Option(spec.getUnschedulable)).exists(_.booleanValue)
    !cordoned && Readiness.isNodeReady(node)
  }

  // As the scheduler does, a bound pod holds its requests until it finishes, an unbound one none.
  private def occupiedRequests(pod: Pod, nodeNames: Set[String]): Option[Amounts] =
    for {
      spec <- Option(pod.getSpec)
      nodeName <- Option(spec.getNodeName) if nodeNames.contains(nodeName)
      if !Option(pod.getStatus).flatMap(s => Option(s.getPhase)).exists(FinishedPhases.contains)
    } yield podRequests(spec)

  // Init containers run one at a time, so a pod needs the larger of its containers' sum and its
  // biggest init container, plus overhead.
  private def podRequests(spec: PodSpec): Amounts = {
    val appContainers = sum(containers(spec.getContainers).map(requests))
    val peakInit = peak(containers(spec.getInitContainers).map(requests))
    sum(Seq(peak(Seq(appContainers, peakInit)), amounts(spec.getOverhead)))
  }

  private def containers(list: java.util.List[Container]): Seq[Container] =
    Option(list).map(_.asScala.toSeq).getOrElse(Seq.empty)

  private def requests(container: Container): Amounts =
    Option(container.getResources)
      .map(resources => amounts(resources.getRequests))
      .getOrElse(Map.empty)

  private def amounts(quantities: java.util.Map[String, Quantity]): Amounts =
    Option(quantities)
      .map(_.asScala.map {
        case (resource, quantity) =>
          resource -> BigDecimal(quantity.getNumericalAmount)
      }.toMap)
      .getOrElse(Map.empty)

  private def sum(all: Iterable[Amounts]): Amounts =
    all.flatten.groupMapReduce(_._1)(_._2)(_ + _)

  private def peak(all: Iterable[Amounts]): Amounts =
    all.flatten.groupMapReduce(_._1)(_._2)(_ max _)

  private def toUnits(all: Amounts, resource: String, unitsPerAmount: Int): Long =
    (all.getOrElse(resource, BigDecimal(0)) * unitsPerAmount)
      .setScale(0, BigDecimal.RoundingMode.HALF_UP)
      .toLong
}
