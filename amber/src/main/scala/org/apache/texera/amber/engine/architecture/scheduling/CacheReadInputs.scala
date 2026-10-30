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

package org.apache.texera.amber.engine.architecture.scheduling

import org.apache.texera.amber.core.virtualidentity.PhysicalOpIdentity
import org.apache.texera.amber.core.workflow.GlobalPortIdentity

import java.net.URI

/**
  * The input ports that read saved results, decided before scheduling, with the locations
  * each one reads. `CostBasedScheduleGenerator` adds these locations to the ones it derives
  * from materialized links, and the resource allocator does not treat the operators that own
  * these ports as sources. The default, empty, leaves scheduling unchanged.
  */
case class CacheReadInputs(
    readerUris: Map[GlobalPortIdentity, List[URI]] = Map.empty
) {
  require(readerUris.keys.forall(_.input), "reader URIs are keyed by input port")

  /** Operators with at least one input port that reads a saved result. */
  def operatorsReadingFromCache: Set[PhysicalOpIdentity] =
    readerUris.keySet.map(_.opId)
}
