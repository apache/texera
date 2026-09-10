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

  it should "schedule exactly where a computing unit can, and no wider" in {
    // The regression this guards: an earlier version tolerated everything, which put
    // multi-gigabyte images on control-plane and other reserved nodes. A computing-unit
    // pod declares no tolerations, so a tainted node is one no unit can ever land on --
    // pre-pulling there buys nothing and costs disk on the nodes that can least spare it.
    val podSpec = prepullDaemonSet(7, PinnedRef).getSpec.getTemplate.getSpec
    Option(podSpec.getTolerations).map(_.asScala.toList).getOrElse(Nil) shouldBe Nil
  }

  // The regression this guards: the init container declared no requests, and the pool
  // namespace has a ResourceQuota on requests.cpu/requests.memory. Quota admission checks
  // init containers, so every pre-pull pod was refused -- while the DaemonSet itself was
  // created, so the service logged success and pre-pulled nothing, anywhere, ever.
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

  // The regression this guards: requests and limits were built once and shared, which put
  // an 8Mi cap on the init container. A limit is enforced per container and never maxed
  // across them, so the shell of an arbitrary image -- bash, on the Python bases these are
  // built from -- was OOMKilled, the pod crash-looped, pause never ran, and the image went
  // back to being reclaimable, all while the DaemonSet reported itself created.
  it should "cap the pause container only, never the image's own shell" in {
    val podSpec = prepullDaemonSet(7, PinnedRef).getSpec.getTemplate.getSpec

    val prepuller = podSpec.getInitContainers.asScala.head
    Option(prepuller.getResources).map(_.getLimits.asScala).getOrElse(Map.empty) shouldBe empty

    val pause = podSpec.getContainers.asScala.head
    pause.getResources.getLimits.asScala.keySet should contain allOf ("cpu", "memory")
  }

  // The regression this guards: a persistent failure -- the Role not reapplied after an
  // upgrade -- was retried on every read of the image list, by every user, for every ready
  // image, logging a stack trace each time.
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

  it should "not hold back a different digest" in {
    // A refresh that resolved a new digest is a new question. Making it wait out a cooldown
    // the previous reference earned would leave nodes on the superseded image for as long
    // as the cooldown lasts.
    val now = 1_000_000_000L
    val other = "owner/name@sha256:" + "f" * 64
    ImagePrepullClient.isCoolingDown(Some((PinnedRef, now)), other, now) shouldBe false
  }

  it should "not hold back an image that has never failed" in {
    ImagePrepullClient.isCoolingDown(None, PinnedRef, 1_000_000_000L) shouldBe false
  }

  "prepulledRefOf" should "read back what a pre-pull actually pulls" in {
    // What lets a stale pre-pull be spotted: a refresh whose repoint failed leaves one
    // holding the previous digest, and comparing only ids would never notice.
    ImagePrepullClient.prepulledRefOf(prepullDaemonSet(7, PinnedRef)).value shouldBe PinnedRef
  }

  it should "be empty for a DaemonSet with no init container" in {
    val strayObject = new DaemonSetBuilder().withNewMetadata().withName("x").endMetadata().build()
    ImagePrepullClient.prepulledRefOf(strayObject) shouldBe None
  }

  it should "name itself after the image, so a refresh replaces rather than adds" in {
    // Same name for the same image at any digest: a refreshed image must not leave a
    // second pre-pull behind holding bytes nothing runs any more.
    prepullDaemonSet(7, PinnedRef).getMetadata.getName shouldBe "cu-image-prepull-7"
    prepullDaemonSet(7, "owner/name@sha256:" + "c" * 64).getMetadata.getName shouldBe
      "cu-image-prepull-7"
    prepullDaemonSet(8, PinnedRef).getMetadata.getName shouldBe "cu-image-prepull-8"
  }

  it should "select on a label it will never want to change" in {
    // A DaemonSet's selector is immutable once created. Selecting on the image id too
    // would be harmless, but selecting on anything mutable would make the update in
    // ensurePrepull fail for good, so this pins the selector to "app" alone.
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

  it should "ignore a DaemonSet that is not one of ours" in {
    // prepulledRefs lists by label, but a cluster can hold anything. A stray object
    // must not be read as an image id and leave a real image without its pre-pull.
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
