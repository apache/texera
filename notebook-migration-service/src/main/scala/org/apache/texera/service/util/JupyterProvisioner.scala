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

import com.typesafe.scalalogging.LazyLogging
import io.fabric8.kubernetes.client.KubernetesClientException
import org.apache.texera.common.config.{KubernetesConfig, StorageConfig}
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.tables.daos.UserJupyterDao
import org.apache.texera.dao.jooq.generated.tables.pojos.UserJupyter
import org.jooq.exception.DataAccessException

import java.util.concurrent.Semaphore
import scala.util.control.NonFatal

/**
  * Brings a user's JupyterLab into existence and registers where it lives.
  *
  * Dependencies are constructor parameters so the provisioning logic can be tested without a
  * cluster; the companion object binds the production ones.
  */
class JupyterProvisioner(
    kubernetesClient: => JupyterKubernetesClient,
    accepts: (String, String) => Boolean,
    publicUrlTemplate: String,
    readinessTimeoutMillis: Long,
    readinessPollMillis: Long,
    maxConcurrentProvisions: Int
) extends LazyLogging {

  // By-name above, forced once here, so no client is built unless a provision happens.
  private lazy val kubernetes = kubernetesClient

  // Provisioning parks the calling request thread for up to readinessTimeoutMillis, so a burst
  // of first-time users could otherwise drain the HTTP worker pool. Requests past the cap are
  // told to retry rather than queued behind a full readiness wait.
  private val provisionPermits = new Semaphore(maxConcurrentProvisions)

  /**
    * The user's Jupyter, starting one if they have none. None means it could not be made
    * ready, which callers report the same as an unreachable server.
    *
    * A registered pod that no longer answers is discarded and rebuilt: the row would otherwise
    * outlive the pod and point every later request at nothing.
    */
  def ensure(
      uid: Int,
      jupyterEnabled: Boolean = KubernetesConfig.jupyterEnabled,
      fallback: JupyterEndpoints = JupyterEndpoints.configured,
      tokenSecret: String = StorageConfig.jupyterTokenSecret
  ): Option[JupyterEndpoints] = {
    if (!jupyterEnabled) return Some(fallback)

    val token = JupyterTokenDeriver.derive(uid, tokenSecret)
    JupyterEndpointResolver.resolve(uid, jupyterEnabled = true, tokenSecret = tokenSecret) match {
      case Some(endpoints) if accepts(endpoints.internalUrl, token) => Some(endpoints)
      case Some(endpoints)                                          =>
        // Either the pod is gone, or it predates a token-secret rotation and no longer
        // accepts the derived token. Both are unusable and both are fixed by rebuilding.
        logger.warn(
          s"Jupyter for user $uid is registered at ${endpoints.internalUrl} but does not "
            + "accept its current token; rebuilding it"
        )
        withPermit(uid) {
          if (discard(uid)) provision(uid, token)
          else {
            // The name is still taken, so a create would be skipped and the rebuild would poll
            // the dying pod to the readiness deadline. Fail now and say why.
            logger.error(s"Stale Jupyter pod for user $uid did not terminate; not rebuilding")
            None
          }
        }
      case None => withPermit(uid)(provision(uid, token))
    }
  }

  // None means the cap is reached, which callers report the same as an unreachable server; the
  // fast path never gets here, so a user with a working pod is unaffected by the limit.
  private def withPermit(uid: Int)(work: => Option[JupyterEndpoints]): Option[JupyterEndpoints] =
    if (!provisionPermits.tryAcquire()) {
      logger.warn(
        s"$maxConcurrentProvisions Jupyter provisions already in flight; "
          + s"refusing user $uid for now"
      )
      None
    } else {
      try work
      finally provisionPermits.release()
    }

  private def provision(uid: Int, token: String): Option[JupyterEndpoints] = {
    // Jupyter serves every endpoint under its base path, /api included, so the recorded
    // address has to carry it or each later call lands on a 404.
    val internalUrl = s"http://${kubernetes.generatePodURI(uid)}${kubernetes.basePathFor(uid)}"
    val endpoints = JupyterEndpoints(internalUrl, publicUrlFor(uid, internalUrl), token)
    try {
      createIfAbsent(uid, token)
      if (!waitUntilAccepting(internalUrl, token)) {
        logger.error(s"Jupyter for user $uid did not become ready; removing the pod")
        kubernetes.deletePod(uid)
        None
      } else {
        register(endpoints, uid)
        Some(endpoints)
      }
    } catch {
      case NonFatal(e) =>
        logger.error(s"Failed to provision Jupyter for user $uid", e)
        None
    }
  }

  // Two concurrent first requests can both find the pod absent and both create it. The
  // loser's create returns 409, and the winner's pod is the one it wanted anyway, since both
  // derive the same token from the same uid, so the conflict is a success. Mirrors
  // register()'s handling of a duplicate row.
  private def createIfAbsent(uid: Int, token: String): Unit = {
    if (kubernetes.podExists(uid)) return
    try kubernetes.createPod(uid, token)
    catch {
      case e: KubernetesClientException if e.getCode == 409 =>
        logger.info(s"Jupyter pod for user $uid was created concurrently; using that one")
    }
  }

  /** Browser-facing address; the in-cluster name does not resolve from the browser. */
  private def publicUrlFor(uid: Int, internalUrl: String): String =
    if (publicUrlTemplate.isEmpty) internalUrl
    else publicUrlTemplate.replace("{uid}", uid.toString)

  // Waits for the pod to accept its own token rather than merely to answer, so a pod that
  // starts with the wrong token never gets registered.
  private def waitUntilAccepting(internalUrl: String, token: String): Boolean = {
    val deadline = System.currentTimeMillis() + readinessTimeoutMillis
    var ready = accepts(internalUrl, token)
    while (!ready && System.currentTimeMillis() < deadline) {
      Thread.sleep(readinessPollMillis)
      ready = accepts(internalUrl, token)
    }
    ready
  }

  private def register(endpoints: JupyterEndpoints, uid: Int): Unit = {
    val row = new UserJupyter
    row.setUid(uid)
    row.setInternalUrl(endpoints.internalUrl)
    row.setPublicUrl(endpoints.publicUrl)
    try dao().insert(row)
    catch {
      // Two concurrent first requests can both provision. uid is the primary key, so the
      // loser trips 23505; the winner's row holds the same uid-derived addresses, so leaving
      // it in place is correct.
      case e: DataAccessException if e.sqlState == "23505" =>
        logger.info(s"Jupyter for user $uid was registered concurrently; keeping that row")
    }
  }

  /** Removes the pod and its row. False means the pod outlived the wait, so no rebuild. */
  private def discard(uid: Int): Boolean = {
    // A pod that is already gone throws here, which is the state the delete wanted; only the
    // wait below decides whether the name is free.
    try kubernetes.deletePod(uid)
    catch { case NonFatal(e) => logger.warn(s"Could not delete stale Jupyter pod for $uid", e) }
    val gone = waitUntilGone(uid)
    dao().deleteById(uid)
    gone
  }

  // Deletion is asynchronous even at grace period 0, and podExists counts a Terminating pod as
  // present, so returning early would make the rebuild skip its create.
  private def waitUntilGone(uid: Int): Boolean = {
    val deadline = System.currentTimeMillis() + readinessTimeoutMillis
    var exists = kubernetes.podExists(uid)
    while (exists && System.currentTimeMillis() < deadline) {
      Thread.sleep(readinessPollMillis)
      exists = kubernetes.podExists(uid)
    }
    !exists
  }

  private def dao() = new UserJupyterDao(SqlServer.getInstance().createDSLContext().configuration())
}

// A pod is scheduled, pulled and started before Jupyter answers, so the first request after
// provisioning waits rather than failing.
object JupyterProvisioner
    extends JupyterProvisioner(
      JupyterKubernetesClient.inCluster,
      JupyterProbe.isAuthorized,
      KubernetesConfig.jupyterPublicUrlTemplate,
      readinessTimeoutMillis = 60000,
      readinessPollMillis = 1000,
      // Far below Dropwizard's 1024 worker threads, so a provisioning burst cannot starve the
      // rest of the service, while still admitting a classroom's worth of concurrent logins.
      maxConcurrentProvisions = 16
    )
