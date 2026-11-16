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

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.fabric8.kubernetes.api.model.{
  ContainerBuilder,
  Node,
  NodeBuilder,
  Pod,
  PodBuilder,
  Quantity,
  ResourceRequirements,
  ResourceRequirementsBuilder
}
import io.dropwizard.jackson.Jackson
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

// A pure function over fabric8 objects, so cases use builders with no client. The rules are the
// ones kube-scheduler applies when it decides whether a new pod fits.
class ClusterCapacitySpec extends AnyFlatSpec with Matchers {

  import ClusterCapacitySpec._

  private val Gi: Long = 1024L * 1024L * 1024L

  private def capacity(nodes: Seq[Node], pods: Seq[Pod]): ClusterCapacity =
    ClusterCapacity.fromNodesAndPods(nodes, pods, gpuKey)

  "fromNodesAndPods" should "report all zeros for an empty cluster" in {
    val result = capacity(Seq.empty, Seq.empty)
    val zero = ResourceCapacity(total = 0, allocated = 0, available = 0)
    result shouldBe ClusterCapacity(cpu = zero, memory = zero, gpu = zero)
  }

  it should "parse decimal-suffixed and fractional quantities exactly" in {
    val result = capacity(
      Seq(node("a", cpu = "1.5", memory = "512M")),
      Seq(pod("p", Some("a"), Seq(Map("cpu" -> "250m", "memory" -> "128M"))))
    )
    result.cpu shouldBe ResourceCapacity(total = 1500, allocated = 250, available = 1250)
    result.memory shouldBe
      ResourceCapacity(total = 512000000L, allocated = 128000000L, available = 384000000L)
  }

  it should "allocate the requests of pods bound to a node and report the remainder as available" in {
    val result = capacity(
      Seq(node("a", "4", "16Gi", Some("2")), node("b", "8", "32Gi")),
      Seq(
        pod("p1", Some("a"), Seq(Map("cpu" -> "1", "memory" -> "2Gi", gpuKey -> "1"))),
        pod("p2", Some("b"), Seq(Map("cpu" -> "500m", "memory" -> "1Gi")))
      )
    )
    result.cpu shouldBe ResourceCapacity(total = 12000, allocated = 1500, available = 10500)
    result.memory shouldBe ResourceCapacity(
      total = 48 * Gi,
      allocated = 3 * Gi,
      available = 45 * Gi
    )
    result.gpu shouldBe ResourceCapacity(total = 2, allocated = 1, available = 1)
  }

  it should "sum the requests of every container in a pod" in {
    val result = capacity(
      Seq(node("a", "4", "16Gi")),
      Seq(pod("p", Some("a"), Seq(Map("cpu" -> "1"), Map("cpu" -> "250m", "memory" -> "1Gi"))))
    )
    result.cpu.allocated shouldBe 1250
    result.memory.allocated shouldBe Gi
  }

  it should "count a container that declares no resources, or empty ones, as zero" in {
    // A container with no `resources` block (null) must not take the endpoint down.
    val bare = new PodBuilder()
      .withNewMetadata()
      .withName("bare")
      .endMetadata()
      .withNewSpec()
      .withNodeName("a")
      .addToContainers(new ContainerBuilder().withName("no-resources").build())
      .addToContainers(
        new ContainerBuilder()
          .withName("no-requests")
          .withResources(new ResourceRequirements())
          .build()
      )
      .endSpec()
      .build()
    capacity(Seq(node("a", "4", "16Gi")), Seq(bare)).cpu.allocated shouldBe 0
  }

  it should "charge a pod the larger of its containers and its biggest init container" in {
    val result = capacity(
      Seq(node("a", "8", "16Gi")),
      Seq(
        // The init container wins on CPU, the app containers on memory: the pod needs the larger.
        pod(
          "p",
          Some("a"),
          containers = Seq(Map("cpu" -> "1", "memory" -> "4Gi")),
          initContainers = Seq(Map("cpu" -> "3", "memory" -> "1Gi"), Map("cpu" -> "2"))
        )
      )
    )
    result.cpu.allocated shouldBe 3000
    result.memory.allocated shouldBe 4 * Gi
  }

  it should "add a pod's runtime-class overhead to what it is charged" in {
    val result = capacity(
      Seq(node("a", "4", "16Gi")),
      Seq(
        pod(
          "p",
          Some("a"),
          containers = Seq(Map("cpu" -> "1", "memory" -> "1Gi")),
          overhead = Map("cpu" -> "250m", "memory" -> "120Mi")
        )
      )
    )
    result.cpu.allocated shouldBe 1250
    result.memory.allocated shouldBe Gi + 120L * 1024 * 1024
  }

  it should "leave out a cordoned node together with the pods running on it" in {
    val result = capacity(
      Seq(node("a", "4", "16Gi"), node("cordoned", "8", "32Gi", Some("4"), cordoned = true)),
      Seq(
        pod("p1", Some("a"), Seq(Map("cpu" -> "1"))),
        pod("p2", Some("cordoned"), Seq(Map("cpu" -> "6", gpuKey -> "4")))
      )
    )
    result.cpu shouldBe ResourceCapacity(total = 4000, allocated = 1000, available = 3000)
    result.gpu shouldBe ResourceCapacity(total = 0, allocated = 0, available = 0)
  }

  it should "leave out a node that is not Ready together with the pods running on it" in {
    val result = capacity(
      Seq(node("a", "4", "16Gi"), node("down", "8", "32Gi", ready = false)),
      Seq(pod("p", Some("down"), Seq(Map("cpu" -> "6"))))
    )
    result.cpu shouldBe ResourceCapacity(total = 4000, allocated = 0, available = 4000)
  }

  it should "leave out a node that reports no Ready condition at all" in {
    val noConditions = new NodeBuilder(node("blank", "8", "32Gi"))
      .editStatus()
      .withConditions()
      .endStatus()
      .build()
    capacity(Seq(noConditions), Seq.empty).cpu.total shouldBe 0
  }

  it should "leave out a node with no status at all" in {
    val statusless = new NodeBuilder().withNewMetadata().withName("new").endMetadata().build()
    capacity(Seq(statusless, node("a", "4", "16Gi")), Seq.empty).cpu.total shouldBe 4000
  }

  it should "read GPUs under the configured resource key, whatever the vendor" in {
    val nodes = Seq(node("a", "4", "16Gi", Some("2")))
    ClusterCapacity.fromNodesAndPods(nodes, Seq.empty, gpuKey).gpu.total shouldBe 2
    ClusterCapacity.fromNodesAndPods(nodes, Seq.empty, "amd.com/gpu").gpu.total shouldBe 0
  }

  it should "ignore pods the scheduler has not bound to a node" in {
    val result = capacity(
      Seq(node("a", "4", "16Gi")),
      Seq(
        pod("pending", None, Seq(Map("cpu" -> "2")), phase = "Pending"),
        pod("blank", Some(""), Seq(Map("cpu" -> "2")), phase = "Pending")
      )
    )
    result.cpu.allocated shouldBe 0
  }

  it should "count Pending, Running and Unknown pods but not finished ones" in {
    val result = capacity(
      Seq(node("a", "16", "64Gi")),
      Seq(
        pod("pending", Some("a"), Seq(Map("cpu" -> "1")), phase = "Pending"),
        pod("running", Some("a"), Seq(Map("cpu" -> "2")), phase = "Running"),
        pod("unknown", Some("a"), Seq(Map("cpu" -> "4")), phase = "Unknown"),
        pod("done", Some("a"), Seq(Map("cpu" -> "8")), phase = "Succeeded"),
        pod("crashed", Some("a"), Seq(Map("cpu" -> "16")), phase = "Failed")
      )
    )
    result.cpu.allocated shouldBe 7000
  }

  it should "count a pod whose status has not been populated yet" in {
    val statusless =
      new PodBuilder(pod("p", Some("a"), Seq(Map("cpu" -> "2")))).withStatus(null).build()
    capacity(Seq(node("a", "4", "16Gi")), Seq(statusless)).cpu.allocated shouldBe 2000
  }

  it should "count a pod with no spec as zero" in {
    val specless = new PodBuilder().withNewMetadata().withName("p").endMetadata().build()
    capacity(Seq(node("a", "4", "16Gi")), Seq(specless)).cpu.allocated shouldBe 0
  }

  it should "floor available at zero when requests exceed what the nodes advertise" in {
    // Allocatable can shrink under running pods (a kubelet reservation raised after they landed).
    val result = capacity(
      Seq(node("a", "2", "4Gi")),
      Seq(pod("p", Some("a"), Seq(Map("cpu" -> "3", "memory" -> "6Gi"))))
    )
    result.cpu shouldBe ResourceCapacity(total = 2000, allocated = 3000, available = 0)
    result.memory shouldBe ResourceCapacity(total = 4 * Gi, allocated = 6 * Gi, available = 0)
  }

  // Consumers read these field names, so a rename breaks them. Mapper set up as the service does.
  "ClusterCapacity" should "serialise as cpu/memory/gpu objects of total, allocated and available" in {
    val mapper = Jackson.newObjectMapper().registerModule(DefaultScalaModule)
    val serialised = mapper.writeValueAsString(
      ClusterCapacity(
        cpu = ResourceCapacity(total = 4000, allocated = 1000, available = 3000),
        memory = ResourceCapacity(total = 8 * Gi, allocated = Gi, available = 7 * Gi),
        gpu = ResourceCapacity(total = 2, allocated = 1, available = 1)
      )
    )
    mapper.readTree(serialised) shouldBe mapper.readTree("""{
      "cpu": {"total": 4000, "allocated": 1000, "available": 3000},
      "memory": {"total": 8589934592, "allocated": 1073741824, "available": 7516192768},
      "gpu": {"total": 2, "allocated": 1, "available": 1}
    }""")
  }
}

object ClusterCapacitySpec {

  private val gpuKey = "nvidia.com/gpu"

  private def quantities(values: Map[String, String]): java.util.Map[String, Quantity] =
    values.map { case (resource, amount) => resource -> new Quantity(amount) }.asJava

  // `unschedulable` is left unset (null, as the API server omits it) unless the node is cordoned.
  private[util] def node(
      name: String,
      cpu: String,
      memory: String,
      gpu: Option[String] = None,
      ready: Boolean = true,
      cordoned: Boolean = false
  ): Node = {
    val allocatable = Map("cpu" -> cpu, "memory" -> memory) ++ gpu.map(gpuKey -> _)
    val builder = new NodeBuilder()
      .withNewMetadata()
      .withName(name)
      .endMetadata()
      .withNewSpec()
      .endSpec()
      .withNewStatus()
      .withAllocatable(quantities(allocatable))
      .addNewCondition()
      .withType("Ready")
      .withStatus(if (ready) "True" else "False")
      .endCondition()
      .endStatus()
    if (cordoned) builder.editSpec().withUnschedulable(true).endSpec()
    builder.build()
  }

  // A pod with one app container per entry of `containers`, each asking for the given requests.
  private[util] def pod(
      name: String,
      nodeName: Option[String],
      containers: Seq[Map[String, String]],
      phase: String = "Running",
      initContainers: Seq[Map[String, String]] = Seq.empty,
      overhead: Map[String, String] = Map.empty
  ): Pod = {
    def container(requested: Map[String, String]) =
      new ContainerBuilder()
        .withName("c")
        .withResources(
          new ResourceRequirementsBuilder().withRequests(quantities(requested)).build()
        )
        .build()

    new PodBuilder()
      .withNewMetadata()
      .withName(name)
      .endMetadata()
      .withNewSpec()
      .withNodeName(nodeName.orNull)
      .withContainers(containers.map(container).asJava)
      .withInitContainers(initContainers.map(container).asJava)
      .withOverhead(quantities(overhead))
      .endSpec()
      .withNewStatus()
      .withPhase(phase)
      .endStatus()
      .build()
  }
}
