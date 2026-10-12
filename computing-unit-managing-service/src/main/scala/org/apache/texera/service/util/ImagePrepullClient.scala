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
import io.fabric8.kubernetes.api.model.apps.{DaemonSet, DaemonSetBuilder}
import io.fabric8.kubernetes.api.model.{DeletionPropagation, Quantity, ResourceRequirementsBuilder}
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import org.apache.texera.common.config.CuratedImageConfig

import scala.jdk.CollectionConverters._

/**
  * Puts a ready curated image on every node before anyone starts a unit from it, so the
  * first unit there does not wait for the pull.
  *
  * One DaemonSet per image, the same shape the chart uses for the deployment's own image:
  * an init container that is the image and does nothing, then a pause container to hold
  * the pod open so the node does not reclaim what was pulled. Built here rather than in
  * the chart because curated images are registered while the cluster is running.
  *
  * Every call is best-effort: a failure is logged, and the image still works.
  */
object ImagePrepullClient extends LazyLogging {

  private val client: io.fabric8.kubernetes.client.KubernetesClient =
    new KubernetesClientBuilder().build()

  private def namespace: String = CuratedImageConfig.prepullNamespace

  /** Marks the pre-pulls this service owns, so they are found by label, not by name. */
  private[service] val OwnerLabel = "texera-cu-image-prepull"

  /** Which image a pre-pull is for, so one can be removed without the others. */
  private[service] val ImageLabel = "texera-cu-image"

  /** Reference and time of each image's last failed create. See [[isCoolingDown]]. */
  private val lastFailure = new java.util.concurrent.ConcurrentHashMap[Int, (String, Long)]()

  private def failedRecently(iid: Int, pinnedRef: String): Boolean =
    isCoolingDown(Option(lastFailure.get(iid)), pinnedRef, System.currentTimeMillis())

  /**
    * Whether to hold a create back. Reconciling runs on every read of the image list, so
    * without this a failure that will not clear is retried on every page load. Keyed by
    * reference, so a new digest is tried at once.
    *
    * Pure, so the rule can be tested without a cluster.
    */
  private[service] def isCoolingDown(
      recorded: Option[(String, Long)],
      pinnedRef: String,
      now: Long
  ): Boolean =
    recorded.exists {
      case (failedRef, at) =>
        failedRef == pinnedRef &&
          now - at < CuratedImageConfig.prepullRetryCooldownSeconds * 1000L
    }

  /** Forgets an image's last failure, so the next create is attempted immediately. */
  def clearFailure(iid: Int): Unit = lastFailure.remove(iid)

  /** Creates an image's pre-pull, or points an existing one at a new reference. */
  def ensurePrepull(iid: Int, pinnedRef: String): Unit = {
    if (!CuratedImageConfig.prepullEnabled) return
    if (failedRecently(iid, pinnedRef)) return
    try {
      client
        .apps()
        .daemonSets()
        .inNamespace(namespace)
        .resource(prepullDaemonSet(iid, pinnedRef))
        .createOr(existing => existing.update())
      lastFailure.remove(iid)
      logger.info(s"Pre-pulling curated image $iid ($pinnedRef) onto every node.")
    } catch {
      case e: Throwable =>
        lastFailure.put(iid, (pinnedRef, System.currentTimeMillis()))
        logger.warn(
          s"Could not pre-pull curated image $iid ($pinnedRef). The first unit on each " +
            "node will wait for the pull instead.",
          e
        )
    }
  }

  /** Removes an image's pre-pull. Safe to call when there is none. */
  def deletePrepull(iid: Int): Unit = {
    try {
      client
        .apps()
        .daemonSets()
        .inNamespace(namespace)
        .withName(CuratedImageConfig.prepullName(iid))
        // Stated, not left to a default: orphaned pods would hold the image on every node,
        // and the reconcile pass lists DaemonSets, so it would never find them.
        .withPropagationPolicy(DeletionPropagation.BACKGROUND)
        .delete()
    } catch {
      case e: Throwable =>
        logger.warn(s"Could not remove the pre-pull for curated image $iid.", e)
    }
    lastFailure.remove(iid)
  }

  /**
    * What each image's pre-pull currently pulls, keyed by image.
    *
    * None means the cluster could not be asked, which is not "none exist": an empty map
    * would have the caller create a pre-pull for every ready image against a cluster that
    * has just refused to talk to it.
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
    val labels = Map("app" -> name, OwnerLabel -> "true", ImageLabel -> iid.toString).asJava

    // Requests on both containers: a namespace with a ResourceQuota on requests.cpu or
    // requests.memory refuses a pod whose init container omits them, and the refusal is
    // invisible because the DaemonSet is still created. Costs nothing -- a pod's request
    // is the larger of its init containers and the sum of the rest.
    val prepullerResources = new ResourceRequirementsBuilder()
      .addToRequests("cpu", new Quantity(CuratedImageConfig.prepullCpu))
      .addToRequests("memory", new Quantity(CuratedImageConfig.prepullMemory))
      .build()

    // Limits only here. A limit is per container and never maxed, so capping the init
    // container would OOMKill an arbitrary image's shell.
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
      // "app" only: a selector cannot be changed once created.
      .withMatchLabels(Map("app" -> name).asJava)
      .endSelector()
      // Every node at once. The default rolls one at a time, each waiting for a full pull,
      // which is hours on a large cluster -- and there is no availability to protect here.
      .withNewUpdateStrategy()
      .withType("RollingUpdate")
      .withNewRollingUpdate()
      .withMaxUnavailable(new io.fabric8.kubernetes.api.model.IntOrString("100%"))
      .endRollingUpdate()
      .endUpdateStrategy()
      .withNewTemplate()
      .withNewMetadata()
      .withLabels(labels)
      .endMetadata()
      .withNewSpec()
      // No tolerations: computing-unit pods declare none, so a tainted node is one no unit
      // can be scheduled onto.
      .withInitContainers(
        new io.fabric8.kubernetes.api.model.ContainerBuilder()
          .withName("prepuller")
          .withImage(pinnedRef)
          // The reference names a digest, so what is on the node cannot differ.
          .withImagePullPolicy("IfNotPresent")
          .withCommand("sh", "-c", "true")
          .withResources(prepullerResources)
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
