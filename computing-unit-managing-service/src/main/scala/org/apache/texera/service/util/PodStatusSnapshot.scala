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

import io.fabric8.kubernetes.api.model.Pod

import scala.jdk.CollectionConverters._

/** The slice of one container's status that the state mapping needs. */
case class ContainerStatusSnapshot(
    waitingReason: Option[String],
    lastTerminatedReason: Option[String],
    restartCount: Int
)

/**
  * The slice of a pod's status that computing-unit state resolution needs, extracted once per
  * pod so the bulk listing keeps its single namespace-wide call. Kept as a plain case class
  * (not the fabric8 Pod) so the (snapshot -> state) mapping in [[ComputingUnitHelpers]] is a
  * pure, builder-testable function and so vanish reconciliation can keep testing key presence
  * on the snapshot map exactly as it did on the phase map.
  *
  * Container-level fields matter because our pods run with restartPolicy Always: an OOM-killed
  * container restarts in place and the pod phase stays "Running", so OOM kills and crash loops
  * are only visible through `containerStatuses[].lastState/state`.
  */
case class PodStatusSnapshot(
    phase: Option[String],
    terminating: Boolean,
    podReason: Option[String],
    podMessage: Option[String],
    unschedulable: Boolean,
    containers: Seq[ContainerStatusSnapshot]
)

object PodStatusSnapshot {

  /** The snapshot of a pod whose status has not been populated yet (getStatus == null). */
  val empty: PodStatusSnapshot =
    PodStatusSnapshot(
      phase = None,
      terminating = false,
      podReason = None,
      podMessage = None,
      unschedulable = false,
      containers = Seq.empty
    )

  /** Pure fabric8 -> snapshot extraction; every nested status object is null-guarded. */
  def fromPod(pod: Pod): PodStatusSnapshot = {
    val terminating =
      Option(pod.getMetadata).flatMap(m => Option(m.getDeletionTimestamp)).isDefined
    Option(pod.getStatus) match {
      case None => empty.copy(terminating = terminating)
      case Some(status) =>
        val unschedulable = Option(status.getConditions)
          .map(_.asScala)
          .getOrElse(Seq.empty)
          .exists(condition =>
            condition.getType == "PodScheduled" &&
              condition.getStatus == "False" &&
              condition.getReason == "Unschedulable"
          )
        val containers = Option(status.getContainerStatuses)
          .map(_.asScala.toSeq)
          .getOrElse(Seq.empty)
          .map(containerStatus =>
            ContainerStatusSnapshot(
              waitingReason = Option(containerStatus.getState)
                .flatMap(state => Option(state.getWaiting))
                .flatMap(waiting => Option(waiting.getReason)),
              lastTerminatedReason = Option(containerStatus.getLastState)
                .flatMap(state => Option(state.getTerminated))
                .flatMap(terminated => Option(terminated.getReason)),
              restartCount = Option(containerStatus.getRestartCount).map(_.intValue()).getOrElse(0)
            )
          )
        PodStatusSnapshot(
          phase = Option(status.getPhase),
          terminating = terminating,
          podReason = Option(status.getReason),
          podMessage = Option(status.getMessage),
          unschedulable = unschedulable,
          containers = containers
        )
    }
  }
}
