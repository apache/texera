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
    * The images that already have a pre-pull. Read so that rows which reached READY without
    * one -- registered before this shipped, or created while the cluster was unreachable --
    * are given theirs, rather than never being pre-pulled at all.
    */
  def prepulledImageIds(): Set[Int] = {
    if (!CuratedImageConfig.prepullEnabled) return Set.empty
    try {
      client
        .apps()
        .daemonSets()
        .inNamespace(namespace)
        .withLabel(OwnerLabel, "true")
        .list()
        .getItems
        .asScala
        .flatMap(imageIdOf)
        .toSet
    } catch {
      case e: Throwable =>
        // Returning "none are pre-pulled" would ask for every one of them again on a
        // cluster that cannot answer. An empty answer here means "nothing to add".
        logger.warn("Could not list the curated-image pre-pulls; leaving them as they are.", e)
        Set.empty
    }
  }

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

    // The pause container is the whole running cost of a pre-pull, and it does nothing but
    // exist, so it is held to the smallest request the chart's own pre-puller uses.
    val pauseResources = new ResourceRequirementsBuilder()
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
      // A node the deployment tolerates is a node a unit can land on, so it is one the
      // image has to reach.
      .addNewToleration()
      .withOperator("Exists")
      .endToleration()
      .withInitContainers(
        new io.fabric8.kubernetes.api.model.ContainerBuilder()
          .withName("prepuller")
          .withImage(pinnedRef)
          // The reference names a digest, so what is already on the node cannot differ
          // from what the registry would serve. Always would re-check it for nothing.
          .withImagePullPolicy("IfNotPresent")
          .withCommand("sh", "-c", "true")
          .build()
      )
      .addNewContainer()
      .withName("pause")
      .withImage(CuratedImageConfig.prepullPauseImage)
      .withResources(pauseResources)
      .endContainer()
      .endSpec()
      .endTemplate()
      .endSpec()
      .build()
  }
}
