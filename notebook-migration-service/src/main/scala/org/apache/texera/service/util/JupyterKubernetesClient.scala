// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.texera.service.util

import io.fabric8.kubernetes.api.model.{
  EnvVarBuilder,
  Pod,
  PodBuilder,
  Quantity,
  ResourceRequirementsBuilder
}
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import org.apache.texera.common.config.KubernetesConfig

/**
  * Thin wrapper over the fabric8 client for per-user JupyterLab pods, mirroring the computing
  * unit's KubernetesClient. The fabric8 client is a constructor parameter rather than a global
  * so tests can exercise the naming and addressing without a live cluster.
  */
class JupyterKubernetesClient(client: io.fabric8.kubernetes.client.KubernetesClient) {

  private val namespace: String = KubernetesConfig.jupyterNamespace
  private val podNamePrefix = "jupyter"

  def generatePodName(uid: Int): String = s"$podNamePrefix-$uid"

  /** The in-cluster address of a user's pod, resolvable via the headless service. */
  def generatePodURI(uid: Int): String =
    s"${generatePodName(uid)}.${KubernetesConfig.jupyterServiceName}.$namespace.svc.cluster.local:${KubernetesConfig.jupyterPortNumber}"

  def podExists(uid: Int): Boolean = getPodByName(generatePodName(uid)).isDefined

  def getPodByName(podName: String): Option[Pod] =
    Option(client.pods().inNamespace(namespace).withName(podName).get())

  /**
    * Starts a user's JupyterLab. The token is passed as JUPYTER_TOKEN, which is what the image's
    * start-texera-jupyter.sh reads, so each pod ends up with its owner's token and no other.
    * Hostname and subdomain are what make generatePodURI resolve.
    */
  def createPod(uid: Int, token: String): Pod = {
    val podName = generatePodName(uid)

    val resources = new ResourceRequirementsBuilder()
      .addToLimits("cpu", new Quantity(KubernetesConfig.jupyterCpuLimit))
      .addToLimits("memory", new Quantity(KubernetesConfig.jupyterMemoryLimit))
      .build()

    val pod = new PodBuilder()
      .withNewMetadata()
      .withName(podName)
      .withNamespace(namespace)
      .addToLabels("type", "jupyter")
      .addToLabels("uid", uid.toString)
      .addToLabels("name", podName)
      .endMetadata()
      .withNewSpec()
      .addNewContainer()
      .withName("jupyter")
      .withImage(KubernetesConfig.jupyterImageName)
      .withImagePullPolicy(KubernetesConfig.computingUnitImagePullPolicy)
      .addNewPort()
      .withContainerPort(KubernetesConfig.jupyterPortNumber)
      .endPort()
      .withEnv(
        new EnvVarBuilder().withName("JUPYTER_TOKEN").withValue(token).build(),
        // Drives the pod's iframe CSP frame-ancestors and its postMessage origin check.
        // Empty is ignored by the image, which then keeps its local-development default.
        new EnvVarBuilder()
          .withName("TEXERA_ORIGIN")
          .withValue(KubernetesConfig.jupyterTexeraOrigin)
          .build()
      )
      .withResources(resources)
      .endContainer()
      .withHostname(podName)
      .withSubdomain(KubernetesConfig.jupyterServiceName)
      .endSpec()
      .build()

    client.resource(pod).inNamespace(namespace).create()
  }

  // Grace period 0: a discarded pod holds nothing worth flushing, and a rebuild cannot claim
  // the name until the old pod is really gone.
  def deletePod(uid: Int): Unit =
    client.pods().inNamespace(namespace).withName(generatePodName(uid)).withGracePeriod(0).delete()
}

object JupyterKubernetesClient {

  /**
    * Built on demand rather than at object initialisation: the single-node and local-dev
    * deployments have no cluster to build a client against, and never provision.
    */
  def inCluster: JupyterKubernetesClient =
    new JupyterKubernetesClient(new KubernetesClientBuilder().build())
}
