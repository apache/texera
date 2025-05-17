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

package edu.uci.ics.amber.engine.architecture.scheduling.config

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.Partitioning
import java.net.URI

/**
  * Super-type for any per-port config.
  * After resource allocation, only OutputPortConfig or InputPortConfig remain.
  */
sealed trait PortConfig {
  def storageURIs: List[URI]
}

/** An output port requires exactly one materialization URI. */
final case class OutputPortConfig(storageURI: URI) extends PortConfig {
  override val storageURIs: List[URI] = List(storageURI)
}

/**
  * This class is needed as we fill the ResouceConfig of a region in two passes (before and after the ResouceAllocator
  * is invoked.) In the first pass, the ScheduleGenerator builds a schedule and assigns materialization URIs to
  * input/output ports. The URI allocation happens in this pass as the ScheduleGenerator can assign URIs as it creates
  * each region, utilizing its global information about materializations across regions. After a Region DAG is
  * finalized by the ScheduleGenerator, the ScheduleGenerator invokes ResouceAllocator, which allocates workers. As
  * Partitioning can only be created after worker allocation, IntermediateInputPortConfig serves as the intermediate result
  * before the ResourceAllocator is invoked. After ResourceAllocator finishes allocating resources, it will be
  * upgraded to an InputPortConfig.
  */
final case class IntermediateInputPortConfig(storageURIs: List[URI]) extends PortConfig

/**
  * Final form after ResourceAllocator is invoked by the ScheduleGenerator.
  * Each URI is associated with its Partitioning.
  */
final case class InputPortConfig(
    storagePairs: List[(URI, Partitioning)]
) extends PortConfig {
  override val storageURIs: List[URI] = storagePairs.map(_._1)
}
