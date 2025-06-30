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

package edu.uci.ics.texera.service.util

import io.fabric8.kubernetes.client.KubernetesClientException
import jakarta.ws.rs.WebApplicationException

/**
  * Parent type for every error the CU-managing service can raise.
  */
sealed abstract class ComputingUnitManagingServiceException(msg: String)
    extends WebApplicationException(msg)

// Not enough cluster resources for this CU request (CPU / memory / GPU)
final case class InsufficientComputingResource(resourceType: String)
    extends ComputingUnitManagingServiceException(
      s"Insufficient $resourceType available in the server. Please decrease the requested amount or try again later."
    )

// User has reached the per-user CU-quota (number of running units)
final case class InsufficientComputingUnitQuota(maxNumberOfComputingUnit: Int)
    extends ComputingUnitManagingServiceException(
      s"You may only have $maxNumberOfComputingUnit computing-unit(s) running at the same time"
    )

// default exception fallback
final case class InternalError(
    override val getMessage: String =
      "The server encountered an internal error while processing your request. " +
        "Please try again later."
) extends ComputingUnitManagingServiceException(getMessage)

/**
  * Companion object with helpers.
  */
object ComputingUnitManagingServiceException {

  /**
    * Translate a KubernetesClientException to one of the service-level exceptions
    */
  def fromKubernetes(e: KubernetesClientException): ComputingUnitManagingServiceException = {
    val message = Option(e.getMessage).map(_.toLowerCase).getOrElse("")

    if (message.contains("exceeded quota")) {
      if (message.contains("cpu")) InsufficientComputingResource("CPU")
      else if (message.contains("memory")) InsufficientComputingResource("memory")
      else if (message.contains("gpu")) InsufficientComputingResource("GPU")
      else InternalError(e.getMessage)
    } else {
      InternalError(e.getMessage)
    }
  }
}
