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
  * Unified config for both input and output ports of an operator
  * @param storageURIs An input port could have multiple URIs to read from.
  *                    An output port currently only has one URI to write to.
  * @param partitioningsOpt Currently only an input port can have a partitioning. For the case of multiple workers,
  *                         each port of each worker has a materialization reader thread, and all the threads of the same
  *                         input port (across different workers) will read the same, duplicated content from the same
  *                         URI. Each thread of a worker uses the Partitioning of this input port to filter the tuples
  *                         sent to the worker input queue. The Partitioning of an input port mimics that of a link, and
  *                         uses a virtual Input port Materialization Reader Thread Actor Id as the sender actorId of
  *                         each port of each actor. In an input port materialization reader thread, the thread reads
  *                         the complete materialization storage data, and only sends tuples to the matching actor that
  *                         the thread belongs to.
  */
case class PortConfig(storageURIs: List[URI], partitioningsOpt: Option[List[Partitioning]] = None)
