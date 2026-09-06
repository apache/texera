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

package org.apache.texera.common.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Spec for [[KubernetesConfig]]. Reading each value forces resolution from kubernetes.conf, so a
  * renamed or mistyped key surfaces here as a ConfigException. Every value except the port numbers
  * carries a `${?ENV}` override, so exact-value assertions are guarded on the env var being unset.
  */
class KubernetesConfigSpec extends AnyFlatSpec with Matchers {

  // `${?VAR}` in HOCON can be satisfied by an OS env var or a JVM system property,
  // so treat either as an override.
  private def ifUnset(name: String)(assertion: => Any): Unit =
    if (!sys.env.contains(name) && !sys.props.contains(name)) assertion

  "KubernetesConfig.computeUnitPortNumber" should "load the fixed port (no env override)" in {
    KubernetesConfig.computeUnitPortNumber shouldBe 8085
  }

  "KubernetesConfig string settings" should "resolve to their kubernetes.conf defaults" in {
    ifUnset("KUBERNETES_COMPUTE_UNIT_SERVICE_NAME")(
      KubernetesConfig.computeUnitServiceName shouldBe "workflow-computing-unit-svc"
    )
    ifUnset("KUBERNETES_COMPUTE_UNIT_POOL_NAME")(
      KubernetesConfig.computeUnitPoolName shouldBe "texera-workflow-computing-unit"
    )
    ifUnset("KUBERNETES_COMPUTE_UNIT_POOL_NAMESPACE")(
      KubernetesConfig.computeUnitPoolNamespace shouldBe "texera-workflow-computing-unit-pool"
    )
    ifUnset("KUBERNETES_IMAGE_NAME")(
      KubernetesConfig.computeUnitImageName shouldBe "bobbai/texera-workflow-computing-unit:dev"
    )
    ifUnset("KUBERNETES_IMAGE_PULL_POLICY")(
      KubernetesConfig.computingUnitImagePullPolicy shouldBe "Always"
    )
    ifUnset("KUBERNETES_COMPUTING_UNIT_GPU_RESOURCE_KEY")(
      KubernetesConfig.gpuResourceKey shouldBe "nvidia.com/gpu"
    )
  }

  "KubernetesConfig numeric and boolean settings" should "resolve to their kubernetes.conf defaults" in {
    ifUnset("KUBERNETES_COMPUTING_UNIT_ENABLED")(
      KubernetesConfig.kubernetesComputingUnitEnabled shouldBe false
    )
    ifUnset("MAX_NUM_OF_RUNNING_COMPUTING_UNITS_PER_USER")(
      KubernetesConfig.maxNumOfRunningComputingUnitsPerUser shouldBe 10
    )
    // an override may legitimately set 0 (to disable), so only require non-negative
    KubernetesConfig.maxNumOfRunningComputingUnitsPerUser should be >= 0
  }

  "KubernetesConfig jupyter settings" should "resolve to their kubernetes.conf defaults" in {
    KubernetesConfig.jupyterPortNumber shouldBe 8888
    // Off by default and keyed separately from kubernetes.enabled, so enabling computing
    // units on Kubernetes never silently enables per-user Jupyter.
    ifUnset("KUBERNETES_JUPYTER_ENABLED")(KubernetesConfig.jupyterEnabled shouldBe false)
    ifUnset("KUBERNETES_JUPYTER_NAMESPACE")(
      KubernetesConfig.jupyterNamespace shouldBe "texera-jupyter-pool"
    )
    ifUnset("KUBERNETES_JUPYTER_SERVICE_NAME")(
      KubernetesConfig.jupyterServiceName shouldBe "jupyter-svc"
    )
    ifUnset("KUBERNETES_JUPYTER_IMAGE_NAME")(
      KubernetesConfig.jupyterImageName shouldBe "ghcr.io/apache/texera-jupyter:latest"
    )
    // Empty by default: only a real deployment knows its own origin.
    ifUnset("KUBERNETES_JUPYTER_TEXERA_ORIGIN")(KubernetesConfig.jupyterTexeraOrigin shouldBe "")
    ifUnset("KUBERNETES_JUPYTER_CPU_LIMIT")(KubernetesConfig.jupyterCpuLimit shouldBe "1")
    ifUnset("KUBERNETES_JUPYTER_MEMORY_LIMIT")(
      KubernetesConfig.jupyterMemoryLimit shouldBe "2Gi"
    )
    // Empty means the browser is handed the in-network address; a deployment that
    // publishes Jupyter overrides it.
    ifUnset("KUBERNETES_JUPYTER_PUBLIC_URL_TEMPLATE")(
      KubernetesConfig.jupyterPublicUrlTemplate shouldBe ""
    )
  }

  "KubernetesConfig mounter settings" should "resolve to their kubernetes.conf defaults" in {
    // Off by default: the mount gives each CU pod a hostPath volume, which the `baseline`
    // and `restricted` Pod Security Standards forbid, so a deployment opts in.
    ifUnset("KUBERNETES_MOUNTER_ENABLED")(KubernetesConfig.mounterEnabled shouldBe false)
    ifUnset("KUBERNETES_MOUNTER_HOST_ROOT")(
      KubernetesConfig.mounterHostRoot shouldBe "/var/lib/texera-mounts"
    )
  }

  "KubernetesConfig limit options" should "parse into trimmed, non-empty lists" in {
    ifUnset("KUBERNETES_COMPUTING_UNIT_CPU_LIMIT_OPTIONS")(
      KubernetesConfig.cpuLimitOptions shouldBe List("1", "2", "4")
    )
    ifUnset("KUBERNETES_COMPUTING_UNIT_MEMORY_LIMIT_OPTIONS")(
      KubernetesConfig.memoryLimitOptions shouldBe List("1Gi", "2Gi", "4Gi")
    )
    ifUnset("KUBERNETES_COMPUTING_UNIT_GPU_LIMIT_OPTIONS")(
      KubernetesConfig.gpuLimitOptions shouldBe List("0", "1", "2")
    )
    // the parser trims and drops blanks; assert that invariant without requiring a
    // non-empty result, since a blank/whitespace override would legitimately parse to
    // an empty list.
    for (
      options <- Seq(
        KubernetesConfig.cpuLimitOptions,
        KubernetesConfig.memoryLimitOptions,
        KubernetesConfig.gpuLimitOptions
      )
    ) {
      options.forall(s => s == s.trim && s.nonEmpty) shouldBe true
    }
  }
}
