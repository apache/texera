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

/**
  * One resource across the cluster's schedulable nodes. CPU is in millicores, memory in bytes
  * and GPU in devices, so a consumer can compute a percentage or pick a display unit without
  * parsing Kubernetes quantity strings. `available` is `total - allocated`, never below zero.
  */
case class ResourceCapacity(total: Long, allocated: Long, available: Long)

case class ClusterCapacity(cpu: ResourceCapacity, memory: ResourceCapacity, gpu: ResourceCapacity)

object ClusterCapacity {

  // Quantities in base units (cores, bytes, devices), keyed by Kubernetes resource name.
  private type Amounts = Map[String, BigDecimal]

  private val Cpu = "cpu"
  private val Memory = "memory"
  private val MillicoresPerCore = 1000

  // A pod in either phase has stopped and no longer holds anything on its node.
  private val FinishedPhases = Set("Succeeded", "Failed")

  /**
    * Aggregates what the nodes offer against what the scheduler has already handed out.
    *
    * `total` is the allocatable of every Ready, uncordoned node, and `allocated` is the requests
    * of the pods occupying those nodes. These are the figures `kubectl describe node`
    * reports as "Allocated resources", and they are what kube-scheduler fits a new pod against,
    * so `available` is what bounds a new computing unit. Live usage is deliberately not used: it
    * reads well below what is committed, and metrics-server reports no GPU at all.
    *
    * Not modelled, so `available` is an upper bound on what a unit can claim:
    *   - per-node fit: the figures are sums across nodes, so capacity spread thinly over many
    *     nodes can still refuse a unit that needs one node's worth;
    *   - the pool namespace's ResourceQuota, which refuses a unit once it is used up however
    *     much the nodes have free;
    *   - which nodes a unit may land on (taints, node selectors, runtime-class scheduling), so
    *     a node it cannot use still counts toward `total`;
    *   - pod-level `spec.resources`, which newer clusters let a pod set in place of
    *     per-container requests;
    *   - native sidecars (init containers with restartPolicy Always), which the scheduler adds
    *     to the app containers instead of taking the larger of the two, so a pod using one is
    *     charged less than it holds.
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

  // A cordoned node takes no new pods, and neither does one that is not Ready (the node
  // controller taints it for exactly that reason), so neither can hold a new computing unit.
  private def isSchedulable(node: Node): Boolean = {
    val cordoned =
      Option(node.getSpec).flatMap(spec => Option(spec.getUnschedulable)).exists(_.booleanValue)
    !cordoned && Readiness.isNodeReady(node)
  }

  // The scheduler's accounting: a pod bound to a node counts until it finishes, whether or not its
  // containers have started, and a pod it has not bound yet counts nowhere.
  private def occupiedRequests(pod: Pod, nodeNames: Set[String]): Option[Amounts] =
    for {
      spec <- Option(pod.getSpec)
      nodeName <- Option(spec.getNodeName) if nodeNames.contains(nodeName)
      if !Option(pod.getStatus).flatMap(s => Option(s.getPhase)).exists(FinishedPhases.contains)
    } yield podRequests(spec)

  // Init containers run one at a time before the app containers, so a pod needs the larger of
  // the app containers' sum and its biggest init container, plus the runtime-class overhead.
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

  // Both treat a resource absent from an amount as zero. Requests are never negative, so zero is
  // also the identity for `peak`'s max.
  private def sum(all: Iterable[Amounts]): Amounts =
    all.flatten.groupMapReduce(_._1)(_._2)(_ + _)

  private def peak(all: Iterable[Amounts]): Amounts =
    all.flatten.groupMapReduce(_._1)(_._2)(_ max _)

  private def toUnits(all: Amounts, resource: String, unitsPerAmount: Int): Long =
    (all.getOrElse(resource, BigDecimal(0)) * unitsPerAmount)
      .setScale(0, BigDecimal.RoundingMode.HALF_UP)
      .toLong
}
