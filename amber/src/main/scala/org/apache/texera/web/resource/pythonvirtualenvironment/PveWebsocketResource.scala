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

package org.apache.texera.web.resource.pythonvirtualenvironment

import org.apache.texera.config.KubernetesConfig

import javax.websocket._
import javax.websocket.server.ServerEndpoint
import java.util.concurrent.LinkedBlockingQueue
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  *  WebSocket endpoint for PVE creation and user package installation that streams
  *  pip installation logs  to the frontend in real time. The environment setup runs
  *  asynchronously, and output is pushed to the client until completion.
  */

@ServerEndpoint("/wsapi/pve")
class PveWebsocketResource {

  /**
    * Reads a single-valued handshake parameter, rejecting an absent key, an empty value list
    * and a blank value alike: all three are malformed handshakes, and reading one straight
    * through would either throw from the reader itself or hand `PveManager` an empty name to
    * resolve into a directory path.
    */
  private def requiredParam(
      params: java.util.Map[String, java.util.List[String]],
      name: String
  ): String = {
    val values = params.get(name)
    if (values == null || values.isEmpty || values.get(0).isBlank) {
      throw new IllegalArgumentException(s"Missing required parameter: $name")
    }
    values.get(0)
  }

  @OnOpen
  def onOpen(session: Session): Unit = {

    val params = session.getRequestParameterMap

    val isLocal = !KubernetesConfig.kubernetesComputingUnitEnabled

    val queue = new LinkedBlockingQueue[String]()

    Future {
      try {
        // These reads belong inside the try, not in the prologue of `onOpen`: up there a
        // malformed handshake throws before either this catch arm or the pump below exists,
        // and the client is left with a socket that closes carrying neither an `[ERR]` line
        // nor the `__DONE__` sentinel it waits for.
        val cuidParam = requiredParam(params, "cuid")
        val cuid = cuidParam.toIntOption.getOrElse(
          throw new IllegalArgumentException(s"Invalid cuid: $cuidParam")
        )
        val pveName = requiredParam(params, "pveName")
        val action = params.getOrDefault("action", java.util.List.of("create")).get(0)

        action match {
          case "create" =>
            PveManager.createNewPve(cuid, queue, pveName, isLocal)

          case "install" =>
            val packages =
              params
                .getOrDefault("packages", java.util.List.of("[]"))
                .get(0)
                .stripPrefix("[")
                .stripSuffix("]")
                .split(",")
                .toList
                .map(_.replace("\"", "").trim)
                .filter(_.nonEmpty)

            PveManager.installUserPackages(packages, cuid, queue, pveName, isLocal)

          case _ =>
            queue.put(s"[ERR] Unknown action: $action")
        }
      } catch {
        case e: Exception =>
          queue.put(s"[ERR] ${e.getMessage}")
      } finally {
        queue.put("__DONE__")
      }
    }

    Future {
      var done = false

      while (!done && session.isOpen) {
        val line = queue.take()
        session.getBasicRemote.sendText(line)

        if (line == "__DONE__") {
          done = true
          session.close()
        }
      }
    }
  }
}
