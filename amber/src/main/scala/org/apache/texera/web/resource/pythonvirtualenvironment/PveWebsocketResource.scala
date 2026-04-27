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

import javax.websocket._
import javax.websocket.server.ServerEndpoint
import java.util.concurrent.LinkedBlockingQueue
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * WebSocket endpoint for PVE creation that streams pip installation logs
  * to the frontend in real time. The environment setup runs asynchronously,
  * and output is pushed to the client until completion.
  */

@ServerEndpoint("/wsapi/pve")
class PveWebsocketResource {

  @OnOpen
  def onOpen(session: Session): Unit = {

    val params = session.getRequestParameterMap

    val cuid = params.get("cuid").get(0).toInt
    val pveName = params.get("pveName").get(0)
    val isLocal = params.get("isLocal").get(0).toBoolean

    val queue = new LinkedBlockingQueue[String]()

    Future {
      try {
        PveManager.createNewPve(cuid, queue, pveName, isLocal)
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
