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
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.{KubernetesClientBuilder, KubernetesClientException}
import org.apache.texera.common.config.KubernetesConfig

/**
  * Finds the node a computing unit's pod runs on, so a mount can be sent to that node's
  * mounter.
  *
  * Resolved here rather than supplied by the caller: letting a caller name the node would
  * hand anything that can reach this service the ability to aim requests at any node's
  * privileged mounter.
  */
class ComputingUnitNodeLocator(fetchPod: String => Option[Pod]) extends LazyLogging {

  def nodeIpOf(cuid: Int): Option[String] = {
    val podName = s"${KubernetesConfig.computeUnitPodNamePrefix}-$cuid"
    // Every field is nullable until the pod is scheduled, which reads as "no node yet".
    fetchPod(podName)
      .flatMap(pod => Option(pod.getStatus))
      .flatMap(status => Option(status.getHostIP))
      .filter(_.nonEmpty)
  }
}

object ComputingUnitNodeLocator extends ComputingUnitNodeLocator(InClusterKubernetesApi.getPod)

private[util] object InClusterKubernetesApi {

  // In-cluster config: the client reads the API address, the cluster CA and the projected
  // service-account token itself, and refreshes that token as the kubelet rotates it.
  private lazy val client = new KubernetesClientBuilder().build()

  def getPod(podName: String): Option[Pod] = {
    val namespace = KubernetesConfig.computeUnitPoolNamespace
    try Option(client.pods().inNamespace(namespace).withName(podName).get())
    catch {
      case e: KubernetesClientException =>
        // Distinguished from a missing pod, which reads as None: a missing RBAC rule or an
        // unreachable API server must not read as "the computing unit is not running".
        throw new IllegalStateException(
          s"cannot read pod $podName in namespace $namespace: ${e.getMessage}",
          e
        )
    }
  }
}
