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

package org.apache.texera.amber.core.workflow

import java.net.URI

/**
  * The saved result of an output port, which the operators that read the port may read
  * instead. The port's own operator, if it runs, still computes the port.
  *
  * @param storageUri the port's base URI, as `VFSURIFactory.createPortBaseURI` builds it; the
  *                   result and state documents hang off it via `resultURI` and `stateURI`
  * @param tupleCount the saved row count when the writer recorded one; a skipped operator
  *                   reports it as the port's output count
  */
case class CachedResult(storageUri: URI, tupleCount: Option[Long])
