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

import org.apache.texera.common.config.StorageConfig

/**
  * The Jupyter server a request targets. internalUrl is what the service calls, publicUrl is
  * what the browser loads; they differ once Jupyter is containerized, since the in-network
  * name does not resolve from the browser.
  */
final case class JupyterEndpoints(internalUrl: String, publicUrl: String, token: String)

object JupyterEndpoints {

  // The single Jupyter from static config, used while per-user provisioning is off.
  val configured: JupyterEndpoints = JupyterEndpoints(
    StorageConfig.jupyterInternalURL,
    StorageConfig.jupyterPublicURL,
    StorageConfig.jupyterToken
  )
}
