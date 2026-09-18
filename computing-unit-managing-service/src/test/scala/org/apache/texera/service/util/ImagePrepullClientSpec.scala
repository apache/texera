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

import io.fabric8.kubernetes.api.model.apps.DaemonSetBuilder
import org.apache.texera.common.config.CuratedImageConfig
import org.scalatest.OptionValues._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class ImagePrepullClientSpec extends AnyFlatSpec with Matchers {

  import ImagePrepullClient.{ImageLabel, OwnerLabel, imageIdOf, prepullDaemonSet}

  private val PinnedRef = "tagandhi19/texera-cu-sklearn@sha256:" + "b" * 64

  "prepullDaemonSet" should "pull the pinned reference and nothing else" in {
    val spec = prepullDaemonSet(7, PinnedRef).getSpec.getTemplate.getSpec
    val initContainers = spec.getInitContainers.asScala.toList
    initContainers should have size 1
    val init = initContainers.head

    // The whole point: the image is named as the init container, so scheduling the pod is
    // what pulls it. The command is a no-op -- nothing in the image is run.
    init.getImage shouldBe PinnedRef
    init.getCommand.asScala.toList shouldBe List("sh", "-c", "true")

    // A digest cannot resolve to different bytes later, so re-checking the registry every
    // time the pod restarts would buy nothing.
    init.getImagePullPolicy shouldBe "IfNotPresent"

    // Only the pause container keeps running. If the curated image were left running here
    // it would be a computing unit nobody asked for, on every node.
    val containers = spec.getContainers.asScala.toList
    containers.map(_.getName) shouldBe List("pause")
    containers.head.getImage shouldBe CuratedImageConfig.prepullPauseImage
  }

  // The default rolls one node at a time, each waiting for a full pull -- hours on a large
  // cluster, for a pod with no availability to protect.
  it should "repoint every node at once" in {
    val rolling = prepullDaemonSet(7, PinnedRef).getSpec.getUpdateStrategy.getRollingUpdate
    rolling.getMaxUnavailable.getStrVal shouldBe "100%"
  }

  // A computing-unit pod declares no tolerations, so a tainted node is one no unit can
  // land on. Tolerating everything put multi-gigabyte images on control-plane nodes.
  it should "schedule exactly where a computing unit can, and no wider" in {
    val podSpec = prepullDaemonSet(7, PinnedRef).getSpec.getTemplate.getSpec
    Option(podSpec.getTolerations).map(_.asScala.toList).getOrElse(Nil) shouldBe Nil
  }

  // The pool namespace's ResourceQuota refuses a pod whose init container omits requests,
  // while still creating the DaemonSet -- so this failed silently.
  it should "declare the requests a quota would demand" in {
    val podSpec = prepullDaemonSet(7, PinnedRef).getSpec.getTemplate.getSpec
    val everyContainer =
      podSpec.getInitContainers.asScala.toList ++ podSpec.getContainers.asScala.toList
    everyContainer.foreach { container =>
      val requests = Option(container.getResources).map(_.getRequests.asScala).getOrElse(Map.empty)
      withClue(s"${container.getName} must request cpu and memory: ") {
        requests.keySet should contain allOf ("cpu", "memory")
      }
    }
  }

  // A limit is per container and never maxed, so a shared one capped the image's own
  // shell at 8Mi and OOMKilled it.
  it should "cap the pause container only, never the image's own shell" in {
    val podSpec = prepullDaemonSet(7, PinnedRef).getSpec.getTemplate.getSpec

    val prepuller = podSpec.getInitContainers.asScala.head
    Option(prepuller.getResources).map(_.getLimits.asScala).getOrElse(Map.empty) shouldBe empty

    val pause = podSpec.getContainers.asScala.head
    pause.getResources.getLimits.asScala.keySet should contain allOf ("cpu", "memory")
  }

  // Reconciling runs on every read of the image list, so a failure that will not clear
  // was retried on every page load, for every ready image.
  "isCoolingDown" should "hold back a reference that just failed" in {
    val now = 1_000_000_000L
    val cooldownMillis = CuratedImageConfig.prepullRetryCooldownSeconds * 1000L

    ImagePrepullClient.isCoolingDown(Some((PinnedRef, now)), PinnedRef, now) shouldBe true
    ImagePrepullClient.isCoolingDown(
      Some((PinnedRef, now - cooldownMillis + 1)),
      PinnedRef,
      now
    ) shouldBe true
  }

  it should "try again once the cooldown has passed" in {
    val now = 1_000_000_000L
    val cooldownMillis = CuratedImageConfig.prepullRetryCooldownSeconds * 1000L
    ImagePrepullClient.isCoolingDown(
      Some((PinnedRef, now - cooldownMillis)),
      PinnedRef,
      now
    ) shouldBe false
  }

  // Otherwise a refresh that resolved a new digest would leave nodes on the old image
  // until the cooldown expired.
  it should "not hold back a different digest" in {
    val now = 1_000_000_000L
    val other = "owner/name@sha256:" + "f" * 64
    ImagePrepullClient.isCoolingDown(Some((PinnedRef, now)), other, now) shouldBe false
  }

  it should "not hold back an image that has never failed" in {
    ImagePrepullClient.isCoolingDown(None, PinnedRef, 1_000_000_000L) shouldBe false
  }

  // How a stale pre-pull is spotted: comparing only ids would miss a failed repoint.
  "prepulledRefOf" should "read back what a pre-pull actually pulls" in {
    ImagePrepullClient.prepulledRefOf(prepullDaemonSet(7, PinnedRef)).value shouldBe PinnedRef
  }

  it should "be empty for a DaemonSet with no init container" in {
    val strayObject = new DaemonSetBuilder().withNewMetadata().withName("x").endMetadata().build()
    ImagePrepullClient.prepulledRefOf(strayObject) shouldBe None
  }

  // Same name at any digest, so a refresh does not leave a second pre-pull behind.
  it should "name itself after the image, so a refresh replaces rather than adds" in {
    prepullDaemonSet(7, PinnedRef).getMetadata.getName shouldBe "cu-image-prepull-7"
    prepullDaemonSet(7, "owner/name@sha256:" + "c" * 64).getMetadata.getName shouldBe
      "cu-image-prepull-7"
    prepullDaemonSet(8, PinnedRef).getMetadata.getName shouldBe "cu-image-prepull-8"
  }

  // A selector is immutable once created, so anything mutable in it would make every
  // later repoint fail.
  it should "select on a label it will never want to change" in {
    val daemonSet = prepullDaemonSet(7, PinnedRef)
    daemonSet.getSpec.getSelector.getMatchLabels.asScala shouldBe
      Map("app" -> "cu-image-prepull-7")

    // The pod template must still match it, or the DaemonSet is rejected outright.
    val templateLabels = daemonSet.getSpec.getTemplate.getMetadata.getLabels.asScala
    templateLabels("app") shouldBe "cu-image-prepull-7"
  }

  it should "label the image it belongs to, so one can be removed without the others" in {
    val labels = prepullDaemonSet(7, PinnedRef).getMetadata.getLabels.asScala
    labels(OwnerLabel) shouldBe "true"
    labels(ImageLabel) shouldBe "7"
  }

  "imageIdOf" should "read the image back from the label rather than the name" in {
    imageIdOf(prepullDaemonSet(7, PinnedRef)).value shouldBe 7
  }

  // A stray object must not be read as an image id.
  it should "ignore a DaemonSet that is not one of ours" in {
    val unlabelled = new DaemonSetBuilder().withNewMetadata().withName("something").endMetadata()
    imageIdOf(unlabelled.build()) shouldBe None

    val notANumber = new DaemonSetBuilder()
      .withNewMetadata()
      .withName("something")
      .addToLabels(ImageLabel, "not-a-number")
      .endMetadata()
    imageIdOf(notANumber.build()) shouldBe None
  }
}
