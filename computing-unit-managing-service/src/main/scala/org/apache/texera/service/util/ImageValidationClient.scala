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
import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.api.model.batch.v1.{Job, JobBuilder}
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import org.apache.texera.common.config.{CuratedImageConfig, KubernetesConfig}

import scala.jdk.CollectionConverters._

/**
  * Checks that a registered image looks like one a computing unit can start from, and
  * resolves the digest behind the reference an administrator gave.
  *
  * "Looks like" is the honest word: the check is that the start command names
  * `computing-unit-master`, which an unrelated image could also do. It catches the ordinary
  * mistake -- the wrong image pasted in -- rather than proving provenance. Registration is
  * admin-only, so that is the bar it needs to clear.
  *
  * The digest is resolved first and everything else is checked against it, so the image
  * that was approved is exactly the one a unit is pinned to -- a tag moved midway through
  * cannot slip a different image past the check.
  *
  * Only manifests and config blobs are read, a few kilobytes, never the layers. A reference
  * that is misspelled, private, or not a computing-unit image is refused in seconds, in
  * front of the administrator who typed it rather than the user whose unit would not
  * start.
  *
  * skopeo rather than a pull: it reads the registry directly, with no Docker socket and no
  * privileged pod.
  */
object ImageValidationClient extends LazyLogging {

  private val client: io.fabric8.kubernetes.client.KubernetesClient =
    new KubernetesClientBuilder().build()

  private def namespace: String = CuratedImageConfig.validationNamespace

  /** Printed by the job and read back out of its log, since a Job cannot return a value. */
  private val DigestMarker = "TEXERA_SOURCE_DIGEST="

  /**
    * Looks at Cmd and Entrypoint rather than the whole config: a match anywhere in the
    * config would also be satisfied by an environment variable that merely mentions the
    * name.
    */
  private def validationScript: String = {
    // The image's own user, checked only where the deployment forces a non-root pod: an
    // image that expects root would be admitted and then fail to start the user's first
    // unit, which is far too late to find out.
    val rootCheck =
      if (!KubernetesConfig.computingUnitRunAsNonRoot) ""
      else
        """
          ||IMAGE_USER=$(skopeo inspect --config --format '{{.Config.User}}' "docker://$PINNED")
          ||echo "Runs as: ${IMAGE_USER:-root}"
          ||case "${IMAGE_USER:-root}" in
          ||  root|0|"")
          ||    echo ""
          ||    echo "ERROR: this image runs as root."
          ||    echo "Computing units are started as a non-root user here, so it would be"
          ||    echo "admitted now and then fail to start. Rebuild it with a USER line."
          ||    exit 1
          ||    ;;
          ||esac
          |""".stripMargin.replace("||", "|")

    s"""set -eu
       |
       |echo "Inspecting $$SOURCE_REF"
       |
       |# The digest first, so everything after this is checked against the exact image that
       |# will be pinned. Resolving it last would leave room for the tag to move in between,
       |# and what was approved would not be what a unit runs.
       |if ! DIGEST=$$(skopeo inspect --format '{{.Digest}}' "docker://$$SOURCE_REF" 2>&1); then
       |  echo ""
       |  echo "ERROR: could not read $$SOURCE_REF from its registry."
       |  echo "$$DIGEST"
       |  echo ""
       |  echo "If that says the manifest is unknown, the tag does not exist. A Docker Hub"
       |  echo "page address carries no tag, so ':latest' was assumed -- and many images do"
       |  echo "not publish one. Register the reference with the tag you want, for example"
       |  echo "'owner/name:1.0'."
       |  echo "If it mentions authorisation, the image is private; only public images can"
       |  echo "be used."
       |  exit 1
       |fi
       |
       |# The repository without its tag, matching what the service derives from the same
       |# reference. A colon after the last slash is a tag; one before it is a registry port.
       |case "$$SOURCE_REF" in
       |  *@sha256:*) REPO="$${SOURCE_REF%@*}" ;;
       |  *) case "$${SOURCE_REF##*/}" in
       |       *:*) REPO="$${SOURCE_REF%:*}" ;;
       |       *)   REPO="$$SOURCE_REF" ;;
       |     esac ;;
       |esac
       |PINNED="$$REPO@$$DIGEST"
       |echo "Pinned to: $$PINNED"
       |
       |if ! START_CMD=$$(skopeo inspect --config \\
       |  --format '{{.Config.Cmd}} {{.Config.Entrypoint}}' \\
       |  "docker://$$PINNED" 2>&1); then
       |  echo ""
       |  echo "ERROR: could not read the image at $$PINNED."
       |  echo "$$START_CMD"
       |  exit 1
       |fi
       |echo "Start command: $$START_CMD"
       |
       |if ! echo "$$START_CMD" | grep -qF '${CuratedImageConfig.requiredCommand}'; then
       |  echo ""
       |  echo "ERROR: $$SOURCE_REF does not look like a Texera computing-unit image."
       |  echo "Its start command is: $$START_CMD"
       |  echo "A computing-unit image starts '${CuratedImageConfig.requiredCommand}'."
       |  exit 1
       |fi
       |$rootCheck
       |echo "$DigestMarker$$DIGEST"
       |""".stripMargin
  }

  def startValidation(iid: Int, attempt: Int, sourceRef: String): Unit = {
    val jobName = CuratedImageConfig.validationJobName(iid, attempt)

    // Only attempts below this one. Two refreshes claim their numbers atomically but reach
    // the cluster in any order, so sweeping every job of this image would let the older of
    // the two delete the newer one's job and strand the row waiting for a job that is gone.
    deleteSupersededValidations(iid, attempt)

    val job = validationJob(jobName, iid, attempt, sourceRef)
    client.batch().v1().jobs().inNamespace(namespace).resource(job).create()
    logger.info(s"Started validation $jobName for $sourceRef")
  }

  private def validationJob(jobName: String, iid: Int, attempt: Int, sourceRef: String): Job = {
    val resources = new ResourceRequirementsBuilder()
      .addToRequests("cpu", new Quantity(CuratedImageConfig.validationCpuRequest))
      .addToRequests("memory", new Quantity(CuratedImageConfig.validationMemoryRequest))
      .addToLimits("cpu", new Quantity(CuratedImageConfig.validationCpuLimit))
      .addToLimits("memory", new Quantity(CuratedImageConfig.validationMemoryLimit))
      .build()

    new JobBuilder()
      .withNewMetadata()
      .withName(jobName)
      .withNamespace(namespace)
      .addToLabels("texera-cu-image", iid.toString)
      .addToLabels("texera-cu-image-attempt", attempt.toString)
      .endMetadata()
      .withNewSpec()
      // A rejected image is rejected deterministically, and a network failure is better
      // retried by an administrator who can see why.
      .withBackoffLimit(0)
      .withActiveDeadlineSeconds(CuratedImageConfig.validationTimeoutSeconds.toLong)
      .withNewTemplate()
      .withNewMetadata()
      .addToLabels("texera-cu-image", iid.toString)
      .addToLabels("texera-cu-image-attempt", attempt.toString)
      .endMetadata()
      .withNewSpec()
      .withRestartPolicy("Never")
      .addNewContainer()
      .withName("skopeo")
      .withImage(CuratedImageConfig.validationImage)
      .withCommand("/bin/sh", "-c")
      .withArgs(validationScript)
      // Passed as a value, not spliced into the script: the shell expands it but never
      // parses it, so a reference cannot carry commands of its own.
      .addNewEnv()
      .withName("SOURCE_REF")
      .withValue(sourceRef)
      .endEnv()
      .withResources(resources)
      .endContainer()
      .endSpec()
      .endTemplate()
      .endSpec()
      .build()
  }

  sealed trait ValidationState
  object ValidationState {
    case object Running extends ValidationState
    case object Succeeded extends ValidationState
    case object Failed extends ValidationState

    /** The job is gone -- cleaned up, or never created. */
    case object Absent extends ValidationState
  }

  def validationState(iid: Int, attempt: Int): ValidationState = {
    val job = Option(
      client
        .batch()
        .v1()
        .jobs()
        .inNamespace(namespace)
        .withName(CuratedImageConfig.validationJobName(iid, attempt))
        .get()
    )

    stateOf(job)
  }

  /**
    * What a Job's status says about its validation. A job stopped by its deadline counts as
    * failed here, because the Job controller records that as a failure.
    */
  private[service] def stateOf(job: Option[Job]): ValidationState =
    job match {
      case None => ValidationState.Absent
      case Some(j) =>
        val status = Option(j.getStatus)
        val succeeded = status.flatMap(s => Option(s.getSucceeded)).exists(_ > 0)
        val failed = status.flatMap(s => Option(s.getFailed)).exists(_ > 0)
        if (succeeded) ValidationState.Succeeded
        else if (failed) ValidationState.Failed
        else ValidationState.Running
    }

  /** The job's output, readable while it runs and after it finishes. */
  def validationLog(iid: Int, attempt: Int): Option[String] = {
    val pods = client
      .pods()
      .inNamespace(namespace)
      .withLabel("job-name", CuratedImageConfig.validationJobName(iid, attempt))
      .list()
      .getItems
      .asScala
      .toList

    pods.headOption.flatMap { pod =>
      try {
        Option(client.pods().inNamespace(namespace).withName(pod.getMetadata.getName).getLog(true))
      } catch {
        // A container that has not started has no log -- ordinary, not an error.
        case e: Throwable =>
          logger.debug(s"No log yet for validation $iid/$attempt: ${e.getMessage}")
          None
      }
    }
  }

  /**
    * Names the API address in the error. A client with no usable kube config falls back to
    * http://localhost:8080, and whatever answers there fails opaquely -- which reads as a
    * Texera bug rather than a missing cluster.
    */
  def describeStartFailure(e: Throwable): String = {
    val cause = Option(e.getCause).filter(_ ne e)
    val detail = Option(e.getMessage)
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse("no message")
    val master =
      try Option(client.getMasterUrl).map(_.toString).getOrElse("unknown")
      catch { case _: Throwable => "unknown" }

    s"""Could not start the validation job.
       |
       |  ${e.getClass.getSimpleName}: $detail${cause
      .map(c => s"\n  caused by ${c.getClass.getSimpleName}: ${Option(c.getMessage).getOrElse("")}")
      .getOrElse("")}
       |
       |Kubernetes API address: $master
       |Namespace:              $namespace
       |
       |Validation runs as a Kubernetes Job, so this needs a reachable cluster. If the
       |address above is http://localhost:8080 then no kube context is set and the client
       |fell back to that default -- check `kubectl config current-context`, and that the
       |namespace above exists.""".stripMargin
  }

  /**
    * Why a job failed, taken from the Job itself. The pods of a job stopped by its deadline
    * are removed by the Job controller, so its condition is all that is left to explain it.
    */
  def failureReason(iid: Int, attempt: Int): Option[String] =
    try {
      Option(
        client
          .batch()
          .v1()
          .jobs()
          .inNamespace(namespace)
          .withName(CuratedImageConfig.validationJobName(iid, attempt))
          .get()
      ).flatMap(failureReasonOf)
    } catch {
      case e: Throwable =>
        logger.debug(s"Could not read why validation $iid/$attempt failed: ${e.getMessage}")
        None
    }

  /**
    * Why a Job says it failed. A deadline is spelled out, because the pods are gone by then
    * and this is the only account the administrator will get.
    */
  private[service] def failureReasonOf(job: Job): Option[String] =
    Option(job.getStatus)
      .flatMap(s => Option(s.getConditions))
      .map(_.asScala.toList)
      .getOrElse(Nil)
      .find(c => c.getType == "Failed")
      .flatMap { c =>
        if (c.getReason == "DeadlineExceeded")
          Some(
            s"The validation gave up after ${CuratedImageConfig.validationTimeoutSeconds} " +
              "seconds. The registry did not answer in time; try again, and check the " +
              "reference is one this cluster can reach."
          )
        else
          Option(c.getMessage).filter(_.nonEmpty).orElse(Option(c.getReason).filter(_.nonEmpty))
      }

  /** The digest the source tag resolved to, as printed by a successful job. */
  def sourceDigestFrom(log: String): Option[String] =
    log.linesIterator
      .map(_.trim)
      .find(_.startsWith(DigestMarker))
      .map(_.drop(DigestMarker.length).trim)
      .filter(_.nonEmpty)

  /**
    * The reference a unit starts from: the administrator's repository at the resolved
    * digest. A tag can be moved by its owner, so pinning keeps the unit on the bytes that
    * were approved. A reference that already names a digest is returned unchanged.
    */
  private[service] def pinnedRef(sourceRef: String, digest: String): String = {
    val reference = Option(sourceRef).map(_.trim).getOrElse("")
    if (reference.contains("@sha256:")) reference
    else {
      // Strip the tag if there is one. A colon after the last slash is a tag; one before
      // it belongs to a registry's port.
      val lastSlash = reference.lastIndexOf('/')
      val colon = reference.indexOf(':', lastSlash + 1)
      val repository = if (colon >= 0) reference.substring(0, colon) else reference
      s"$repository@$digest"
    }
  }

  /**
    * Removes one validation's job once its outcome has been recorded. Safe to call when it
    * does not exist. Not a TTL on the Job: outcomes are read when someone looks at the list,
    * so a job reaped on a timer could vanish before it was ever read.
    */
  def deleteValidation(iid: Int, attempt: Int): Unit = {
    val jobName = CuratedImageConfig.validationJobName(iid, attempt)
    try {
      client
        .batch()
        .v1()
        .jobs()
        .inNamespace(namespace)
        .withName(jobName)
        .withPropagationPolicy(DeletionPropagation.BACKGROUND)
        .delete()
    } catch {
      case e: Throwable => logger.warn(s"Could not clean up validation $jobName: ${e.getMessage}")
    }
  }

  /** Which of an image's jobs belong to an attempt this one has replaced. */
  private[service] def supersededJobs(jobs: List[Job], attempt: Int): List[String] =
    jobs.filter(j => attemptOf(j).exists(_ < attempt)).flatMap(j => Option(j.getMetadata.getName))

  /**
    * The attempt a job belongs to, from its label, falling back to the trailing number of
    * its name for a job created before the label existed.
    */
  private[service] def attemptOf(job: Job): Option[Int] = {
    val labelled = Option(job.getMetadata)
      .flatMap(m => Option(m.getLabels))
      .flatMap(l => Option(l.get("texera-cu-image-attempt")))
    val named = Option(job.getMetadata).flatMap(m => Option(m.getName)).map(_.split('-').last)
    labelled.orElse(named).flatMap(v => scala.util.Try(v.toInt).toOption)
  }

  /** Removes the jobs of attempts this one has replaced. */
  def deleteSupersededValidations(iid: Int, attempt: Int): Unit = {
    try {
      val jobs = client
        .batch()
        .v1()
        .jobs()
        .inNamespace(namespace)
        .withLabel("texera-cu-image", iid.toString)
        .list()
        .getItems
        .asScala
        .toList
      supersededJobs(jobs, attempt).foreach { name =>
        client
          .batch()
          .v1()
          .jobs()
          .inNamespace(namespace)
          .withName(name)
          .withPropagationPolicy(DeletionPropagation.BACKGROUND)
          .delete()
      }
    } catch {
      case e: Throwable =>
        logger.warn(s"Could not clean up superseded validations for image $iid: ${e.getMessage}")
    }
  }

  /** Removes every validation job belonging to an image, superseded or not. */
  def deleteAllValidations(iid: Int): Unit = {
    try {
      client
        .batch()
        .v1()
        .jobs()
        .inNamespace(namespace)
        .withLabel("texera-cu-image", iid.toString)
        .withPropagationPolicy(DeletionPropagation.BACKGROUND)
        .delete()
    } catch {
      case e: Throwable =>
        logger.warn(s"Could not clean up validation jobs for image $iid: ${e.getMessage}")
    }
  }
}
