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

import org.apache.texera.common.config.{KubernetesConfig, StorageConfig}
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.tables.daos.UserJupyterDao

/**
  * Maps a user to the Jupyter their requests should reach.
  *
  * The uid always comes from the authenticated session, never from a request body, so one user
  * can never address another's Jupyter.
  */
object JupyterEndpointResolver {

  /**
    * Endpoints for the user's Jupyter, or None when they have none.
    *
    * With per-user Jupyter off, every user resolves to the statically configured server: that
    * is how the single-node and local-dev deployments run one shared JupyterLab. With it on, a
    * user with no registry row has nothing provisioned yet, and falling back to the shared
    * server would hand them somebody else's notebooks.
    */
  def resolve(
      uid: Int,
      jupyterEnabled: Boolean = KubernetesConfig.jupyterEnabled,
      fallback: JupyterEndpoints = JupyterEndpoints.configured,
      tokenSecret: String = StorageConfig.jupyterTokenSecret
  ): Option[JupyterEndpoints] =
    if (!jupyterEnabled) Some(fallback)
    else
      registrationOf(uid).map(row =>
        // The token is derived rather than stored, so it is rebuilt here from the uid.
        JupyterEndpoints(
          row.getInternalUrl,
          row.getPublicUrl,
          JupyterTokenDeriver.derive(uid, tokenSecret)
        )
      )

  private def registrationOf(uid: Int) = {
    val dao = new UserJupyterDao(SqlServer.getInstance().createDSLContext().configuration())
    Option(dao.fetchOneByUid(uid))
  }
}
