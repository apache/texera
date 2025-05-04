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

package edu.uci.ics.amber.operator.source.cache

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.core.storage.model.VirtualDocument
import edu.uci.ics.amber.core.storage.{DocumentFactory, VFSResourceType, VFSURIFactory}
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}

import java.net.URI

class CacheSourceOpExec(storageUri: URI) extends SourceOperatorExecutor with LazyLogging {
  val (_, _, _, resourceType) = VFSURIFactory.decodeURI(storageUri)
  if (resourceType != VFSResourceType.RESULT) {
    throw new RuntimeException("The storage URI must point to a result storage")
  }

  private val storage =
    DocumentFactory.openDocument(storageUri)._1.asInstanceOf[VirtualDocument[Tuple]]

  override def produceTuple(): Iterator[TupleLike] = storage.get()

}
