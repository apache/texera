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

package edu.uci.ics.amber.engine.common.statetransition

import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState._
import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity

// The following pattern is a good practice of enum in scala
// We've always used this pattern in the codebase
// https://nrinaudo.github.io/scala-best-practices/definitions/adt.html
// https://nrinaudo.github.io/scala-best-practices/adts/product_with_serializable.html

class WorkerStateManager(actorId: ActorVirtualIdentity, initialState: WorkerState = UNINITIALIZED)
    extends StateManager[WorkerState](
      actorId,
      Map(
        UNINITIALIZED -> Set(READY),
        READY -> Set(PAUSED, RUNNING, COMPLETED),
        RUNNING -> Set(PAUSED, COMPLETED),
        PAUSED -> Set(RUNNING),
        COMPLETED -> Set()
      ),
      initialState
    ) {}
