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

import io.fabric8.kubernetes.client.KubernetesClientException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ComputingUnitManagingServiceExceptionSpec extends AnyFlatSpec with Matchers {

  private def k8sError(message: String): KubernetesClientException =
    new KubernetesClientException(message)

  "InsufficientComputingResource" should "render a resource-specific message" in {
    InsufficientComputingResource("CPU").getMessage shouldBe
      "Insufficient CPU available in the server. Please decrease the requested amount or try again later."
  }

  "InsufficientComputingUnitQuota" should "render the quota message" in {
    InsufficientComputingUnitQuota(3).getMessage shouldBe
      "You may only have 3 computing-unit(s) running at the same time"
  }

  "InternalError" should "default to a generic message" in {
    InternalError().getMessage shouldBe
      "The server encountered an internal error while processing your request. Please try again later."
  }

  "fromKubernetes" should "classify an exceeded-quota cpu error as CPU" in {
    ComputingUnitManagingServiceException.fromKubernetes(k8sError("exceeded quota: cpu")) shouldBe
      InsufficientComputingResource("CPU")
  }

  it should "classify a memory quota error as memory" in {
    ComputingUnitManagingServiceException.fromKubernetes(
      k8sError("exceeded quota: memory")
    ) shouldBe
      InsufficientComputingResource("memory")
  }

  it should "classify a gpu quota error as GPU" in {
    ComputingUnitManagingServiceException.fromKubernetes(k8sError("exceeded quota: gpu")) shouldBe
      InsufficientComputingResource("GPU")
  }

  it should "match case-insensitively" in {
    ComputingUnitManagingServiceException.fromKubernetes(
      k8sError("Exceeded Quota: CPU limit")
    ) shouldBe
      InsufficientComputingResource("CPU")
  }

  it should "fall back to InternalError for an exceeded quota with no known resource" in {
    val e = k8sError("exceeded quota: pods")
    ComputingUnitManagingServiceException.fromKubernetes(e) shouldBe InternalError(e.getMessage)
  }

  it should "fall back to InternalError for a non-quota error" in {
    val e = k8sError("some other error")
    ComputingUnitManagingServiceException.fromKubernetes(e) shouldBe InternalError(e.getMessage)
  }

  it should "fall back to InternalError for a null message" in {
    ComputingUnitManagingServiceException.fromKubernetes(k8sError(null)) shouldBe a[InternalError]
  }
}
