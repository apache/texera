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

package org.apache.texera.web.model.websocket.event

import org.apache.texera.web.service.ExecutionResultService.WebResultUpdate

/**
  * The result updates of one poll, for every external output port whose result changed. Both maps
  * are keyed by operator id and then by output port index (a JSON string key, e.g.
  * `{"Split-1": {"0": ..., "1": ...}}`). `tableStats` has only the ports whose result is a table.
  */
case class WebResultUpdateEvent(
    updates: Map[String, Map[Int, WebResultUpdate]],
    tableStats: Map[String, Map[Int, Map[String, Map[String, Any]]]]
) extends TexeraWebSocketEvent
