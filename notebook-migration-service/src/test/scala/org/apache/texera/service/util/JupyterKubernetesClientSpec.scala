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

import io.fabric8.kubernetes.api.model.{Pod, PodBuilder, PodList}
import io.fabric8.kubernetes.client.dsl.{
  Deletable,
  MixedOperation,
  NamespaceableResource,
  NonNamespaceOperation,
  PodResource,
  Resource
}
import io.fabric8.kubernetes.client.{
  KubernetesClient => Fabric8Client,
  PropagationPolicyConfigurable
}
import org.apache.texera.common.config.KubernetesConfig
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{doReturn, mock, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

/**
  * Two layers: the pure naming and addressing, which is what the registry stores and the
  * service dials, and the thin fabric8 wrappers driven through a Mockito-stubbed client so
  * the pod spec can be asserted without a live cluster.
  */
class JupyterKubernetesClientSpec extends AnyFlatSpec with Matchers {

  private val namespace = KubernetesConfig.jupyterNamespace

  private val bare = new JupyterKubernetesClient(null)

  // fabric8's fluent API returns type variables, so RETURNS_DEEP_STUBS cannot be used and
  // each step of the chain is mocked explicitly. Mirrors the computing unit's spec.
  // withGracePeriod hands back a further step in the chain, so delete() lands on that object
  // rather than on the PodResource; it is returned so the spec can verify it.
  private def stubbedPods(
      existing: Pod
  ): (Fabric8Client, PodResource, PropagationPolicyConfigurable[Deletable]) = {
    val client = mock(classOf[Fabric8Client])
    val mixed = mock(classOf[MixedOperation[_, _, _]])
      .asInstanceOf[MixedOperation[Pod, PodList, PodResource]]
    val inNamespace = mock(classOf[NonNamespaceOperation[_, _, _]])
      .asInstanceOf[NonNamespaceOperation[Pod, PodList, PodResource]]
    val podResource = mock(classOf[PodResource])
    when(client.pods()).thenReturn(mixed)
    when(mixed.inNamespace(namespace)).thenReturn(inNamespace)
    when(inNamespace.withName(org.mockito.ArgumentMatchers.anyString())).thenReturn(podResource)
    val afterGracePeriod = mock(classOf[PropagationPolicyConfigurable[_]])
      .asInstanceOf[PropagationPolicyConfigurable[Deletable]]
    when(podResource.get()).thenReturn(existing)
    // doReturn, not when: withGracePeriod's return type is an existential that thenReturn
    // cannot be given a name for.
    doReturn(afterGracePeriod, Nil: _*).when(podResource).withGracePeriod(0L)
    (client, podResource, afterGracePeriod)
  }

  // -- naming and addressing --------------------------------------------------

  "generatePodName" should "namespace the pod by uid" in {
    bare.generatePodName(7) shouldBe "jupyter-7"
  }

  it should "give every user a distinct pod name" in {
    (1 to 50).map(bare.generatePodName).distinct.size shouldBe 50
  }

  "generatePodURI" should "address the pod through the headless service" in {
    // Must match the pod's hostname.subdomain, or the name does not resolve in-cluster.
    bare.generatePodURI(7) shouldBe
      s"jupyter-7.${KubernetesConfig.jupyterServiceName}.$namespace" +
        s".svc.cluster.local:${KubernetesConfig.jupyterPortNumber}"
  }

  it should "carry the configured port" in {
    bare.generatePodURI(7) should endWith(s":${KubernetesConfig.jupyterPortNumber}")
  }

  // -- lookups ---------------------------------------------------------------

  "getPodByName" should "return the pod when one exists" in {
    val pod = new PodBuilder().withNewMetadata().withName("jupyter-7").endMetadata().build()
    val (client, _, _) = stubbedPods(pod)
    new JupyterKubernetesClient(client).getPodByName("jupyter-7") shouldBe Some(pod)
  }

  it should "return None when the pod is absent" in {
    val (client, _, _) = stubbedPods(null)
    new JupyterKubernetesClient(client).getPodByName("jupyter-7") shouldBe None
  }

  "podExists" should "report true for a live pod and false for a missing one" in {
    val pod = new PodBuilder().withNewMetadata().withName("jupyter-7").endMetadata().build()
    new JupyterKubernetesClient(stubbedPods(pod)._1).podExists(7) shouldBe true
    new JupyterKubernetesClient(stubbedPods(null)._1).podExists(7) shouldBe false
  }

  "deletePod" should "delete the user's own pod by name" in {
    val (client, podResource, afterGracePeriod) = stubbedPods(null)
    new JupyterKubernetesClient(client).deletePod(7)
    verify(client.pods().inNamespace(namespace)).withName("jupyter-7")
    verify(podResource).withGracePeriod(0L)
    verify(afterGracePeriod).delete()
  }

  // -- pod spec --------------------------------------------------------------

  // Captures the pod handed to fabric8, so every field the deployment depends on is asserted.
  private def createdPod(uid: Int, token: String): Pod = {
    val client = mock(classOf[Fabric8Client])
    val namespaceable = mock(classOf[NamespaceableResource[_]])
      .asInstanceOf[NamespaceableResource[Pod]]
    val resource = mock(classOf[Resource[_]]).asInstanceOf[Resource[Pod]]
    val captor = ArgumentCaptor.forClass(classOf[Pod])
    when(client.resource(captor.capture())).thenReturn(namespaceable)
    when(namespaceable.inNamespace(namespace)).thenReturn(resource)
    when(resource.create()).thenReturn(null)
    new JupyterKubernetesClient(client).createPod(uid, token)
    captor.getValue
  }

  "createPod" should "name and namespace the pod for its owner" in {
    val pod = createdPod(7, "tok")
    pod.getMetadata.getName shouldBe "jupyter-7"
    pod.getMetadata.getNamespace shouldBe namespace
  }

  it should "label the pod so the headless service and the owner are identifiable" in {
    val labels = createdPod(7, "tok").getMetadata.getLabels.asScala
    labels("type") shouldBe "jupyter"
    labels("uid") shouldBe "7"
    labels("name") shouldBe "jupyter-7"
  }

  it should "pass the owner's token as JUPYTER_TOKEN" in {
    // The image's start-texera-jupyter.sh reads this, so it is what isolates one user's
    // Jupyter from another's.
    val env = createdPod(7, "derived-token").getSpec.getContainers.asScala.head.getEnv.asScala
    env.map(_.getName) should contain("JUPYTER_TOKEN")
    env.find(_.getName == "JUPYTER_TOKEN").map(_.getValue) shouldBe Some("derived-token")
  }

  it should "tell the pod which Texera origin may frame it" in {
    // Without this the pod keeps the image's local-development default, and its CSP
    // frame-ancestors then blocks the real deployment from embedding it.
    val env = createdPod(7, "tok").getSpec.getContainers.asScala.head.getEnv.asScala
    env.find(_.getName == "TEXERA_ORIGIN").map(_.getValue) shouldBe
      Some(KubernetesConfig.jupyterTexeraOrigin)
  }

  it should "carry the configured image, pull policy and port" in {
    val container = createdPod(7, "tok").getSpec.getContainers.asScala.head
    container.getImage shouldBe KubernetesConfig.jupyterImageName
    container.getImagePullPolicy shouldBe KubernetesConfig.computingUnitImagePullPolicy
    container.getPorts.asScala.map(_.getContainerPort.intValue()) should contain(
      KubernetesConfig.jupyterPortNumber
    )
  }

  it should "carry the configured cpu and memory limits" in {
    val limits = createdPod(7, "tok").getSpec.getContainers.asScala.head.getResources.getLimits
    limits.get("cpu").toString shouldBe KubernetesConfig.jupyterCpuLimit
    limits.get("memory").toString shouldBe KubernetesConfig.jupyterMemoryLimit
  }

  it should "set hostname and subdomain so generatePodURI resolves" in {
    // The pair is what makes <pod>.<service>.<namespace>.svc.cluster.local addressable.
    val spec = createdPod(7, "tok").getSpec
    spec.getHostname shouldBe "jupyter-7"
    spec.getSubdomain shouldBe KubernetesConfig.jupyterServiceName
  }

  "inCluster" should "build a client lazily without requiring a reachable cluster" in {
    // The companion is only touched when a provision happens, but building the client must
    // not itself need a cluster: single-node and local dev have none.
    val client = JupyterKubernetesClient.inCluster
    client.generatePodName(7) shouldBe "jupyter-7"
  }

  it should "create the pod in the Jupyter namespace" in {
    val client = mock(classOf[Fabric8Client])
    val namespaceable = mock(classOf[NamespaceableResource[_]])
      .asInstanceOf[NamespaceableResource[Pod]]
    val resource = mock(classOf[Resource[_]]).asInstanceOf[Resource[Pod]]
    when(client.resource(org.mockito.ArgumentMatchers.any(classOf[Pod]))).thenReturn(namespaceable)
    when(namespaceable.inNamespace(namespace)).thenReturn(resource)
    new JupyterKubernetesClient(client).createPod(7, "tok")
    verify(namespaceable).inNamespace(namespace)
    verify(resource).create()
  }
}
