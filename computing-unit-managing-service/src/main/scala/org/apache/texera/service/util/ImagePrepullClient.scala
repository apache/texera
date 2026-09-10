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

import com.typesafe.scalalogging.LazyLogging
import io.fabric8.kubernetes.api.model.{Quantity, ResourceRequirementsBuilder}
import io.fabric8.kubernetes.api.model.apps.{DaemonSet, DaemonSetBuilder}
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import org.apache.texera.common.config.CuratedImageConfig

import scala.jdk.CollectionConverters._

/**
  * Puts a ready curated image on every node before anyone starts a unit from it.
  *
  * Without this the first unit on a node waits for the whole image -- about 80 seconds for
  * a 3 GB one -- while every later unit there starts at once, so the same action takes
  * seconds or minutes depending only on where it landed.
  *
  * The mechanism is the one the chart already uses for the deployment's own image: a
  * DaemonSet whose init container is the image and whose command does nothing, then a pause
  * container to hold the pod open so the node does not reclaim what was just pulled. The
  * chart cannot express these, because a curated image is registered while the cluster is
  * running, so they are built here instead.
  *
  * Every call is best-effort. A pre-pull that cannot be created is logged and ignored: the
  * image still works, and the first unit on each node just pays for the pull.
  */
object ImagePrepullClient extends LazyLogging {

  private val client: io.fabric8.kubernetes.client.KubernetesClient =
    new KubernetesClientBuilder().build()

  private def namespace: String = CuratedImageConfig.prepullNamespace

  /** Marks every pre-pull this service owns, so they are found by label and not by name. */
  private[service] val OwnerLabel = "texera-cu-image-prepull"

  /** The image a pre-pull belongs to, so one image's can be removed without the others. */
  private[service] val ImageLabel = "texera-cu-image"

  /**
    * Creates the pre-pull for an image, or points an existing one at a new reference. Called
    * whenever a row reaches READY, which covers a refresh that resolved a moved tag to a
    * different digest.
    */
  def ensurePrepull(iid: Int, pinnedRef: String): Unit = {
    if (!CuratedImageConfig.prepullEnabled) return
    try {
      val daemonSet = prepullDaemonSet(iid, pinnedRef)
      client
        .apps()
        .daemonSets()
        .inNamespace(namespace)
        .resource(daemonSet)
        .createOr(existing => existing.update())
      logger.info(s"Pre-pulling curated image $iid ($pinnedRef) onto every node.")
    } catch {
      case e: Throwable =>
        // The image is still usable; only the head start is lost.
        logger.warn(
          s"Could not pre-pull curated image $iid ($pinnedRef). The first unit on each " +
            "node will wait for the pull instead.",
          e
        )
    }
  }

  /**
    * Removes an image's pre-pull. Safe to call when there is none -- a deployment that had
    * pre-pulling turned off has nothing to delete, and neither has an image that never
    * reached READY.
    */
  def deletePrepull(iid: Int): Unit = {
    try {
      client
        .apps()
        .daemonSets()
        .inNamespace(namespace)
        .withName(CuratedImageConfig.prepullName(iid))
        .delete()
    } catch {
      case e: Throwable =>
        // Left behind it would go on holding the image on every node, so it is worth
        // naming in the log rather than passing over.
        logger.warn(s"Could not remove the pre-pull for curated image $iid.", e)
    }
  }

  /**
    * What each image's pre-pull currently pulls, keyed by image. Read so that a row which
    * reached READY without one -- registered before this shipped, or finished while the
    * cluster was unreachable -- is given theirs, and so that one still pointing at a
    * superseded digest is corrected.
    *
    * None means the question could not be answered, which is not the same as "none exist":
    * an empty map would have the caller create a pre-pull for every ready image against a
    * cluster that has just refused to talk to it.
    */
  def prepulledRefs(): Option[Map[Int, String]] = {
    try {
      val entries = client
        .apps()
        .daemonSets()
        .inNamespace(namespace)
        .withLabel(OwnerLabel, "true")
        .list()
        .getItems
        .asScala
        .flatMap(daemonSet => imageIdOf(daemonSet).map(_ -> prepulledRefOf(daemonSet).orNull))
      Some(entries.toMap)
    } catch {
      case e: Throwable =>
        logger.warn("Could not list the curated-image pre-pulls; leaving them as they are.", e)
        None
    }
  }

  /** Every pre-pull this service owns, removed. How turning pre-pulling off frees the disk. */
  def deleteAllPrepulls(): Unit = {
    try {
      client
        .apps()
        .daemonSets()
        .inNamespace(namespace)
        .withLabel(OwnerLabel, "true")
        .delete()
    } catch {
      case e: Throwable =>
        logger.warn("Could not remove the curated-image pre-pulls.", e)
    }
  }

  /** The reference a pre-pull pulls, which is its init container's image. */
  private[service] def prepulledRefOf(daemonSet: DaemonSet): Option[String] =
    Option(daemonSet.getSpec)
      .flatMap(spec => Option(spec.getTemplate))
      .flatMap(template => Option(template.getSpec))
      .flatMap(podSpec => Option(podSpec.getInitContainers))
      .flatMap(_.asScala.headOption)
      .flatMap(container => Option(container.getImage))

  /** The image a pre-pull was created for, from its label rather than its name. */
  private[service] def imageIdOf(daemonSet: DaemonSet): Option[Int] =
    Option(daemonSet.getMetadata)
      .flatMap(m => Option(m.getLabels))
      .flatMap(labels => Option(labels.get(ImageLabel)))
      .flatMap(value => scala.util.Try(value.toInt).toOption)

  private[service] def prepullDaemonSet(iid: Int, pinnedRef: String): DaemonSet = {
    val name = CuratedImageConfig.prepullName(iid)
    val labels = Map(
      "app" -> name,
      OwnerLabel -> "true",
      ImageLabel -> iid.toString
    ).asJava

    // Stated on both containers, not just the one that keeps running. A namespace with a
    // ResourceQuota on requests.cpu/requests.memory refuses a pod whose init container
    // leaves them out -- quota admission checks init containers too -- and the refusal is
    // invisible, because the DaemonSet is still created. Costs nothing: a pod's request is
    // the larger of its init containers and the sum of its others, so the same 1m/8Mi.
    val resources = new ResourceRequirementsBuilder()
      .addToRequests("cpu", new Quantity(CuratedImageConfig.prepullCpu))
      .addToRequests("memory", new Quantity(CuratedImageConfig.prepullMemory))
      .addToLimits("cpu", new Quantity(CuratedImageConfig.prepullCpu))
      .addToLimits("memory", new Quantity(CuratedImageConfig.prepullMemory))
      .build()

    new DaemonSetBuilder()
      .withNewMetadata()
      .withName(name)
      .withNamespace(namespace)
      .withLabels(labels)
      .endMetadata()
      .withNewSpec()
      .withNewSelector()
      // Only "app". A DaemonSet's selector cannot be changed after it is created, so it
      // must not carry anything this code might later want to alter.
      .withMatchLabels(Map("app" -> name).asJava)
      .endSelector()
      .withNewTemplate()
      .withNewMetadata()
      .withLabels(labels)
      .endMetadata()
      .withNewSpec()
      // No tolerations, deliberately: a computing-unit pod declares none either, so a
      // tainted node is one no unit can ever be scheduled onto. Tolerating everything
      // would put multi-gigabyte images on control-plane and other reserved nodes that
      // will never run a unit.
      .withInitContainers(
        new io.fabric8.kubernetes.api.model.ContainerBuilder()
          .withName("prepuller")
          .withImage(pinnedRef)
          // The reference names a digest, so what is already on the node cannot differ
          // from what the registry would serve. Always would re-check it for nothing.
          .withImagePullPolicy("IfNotPresent")
          .withCommand("sh", "-c", "true")
          .withResources(resources)
          .build()
      )
      .addNewContainer()
      .withName("pause")
      .withImage(CuratedImageConfig.prepullPauseImage)
      .withResources(resources)
      .endContainer()
      .endSpec()
      .endTemplate()
      .endSpec()
      .build()
  }
}
