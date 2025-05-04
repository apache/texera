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

package edu.uci.ics.amber.core

import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity

class WorkflowRuntimeException(
    val message: String,
    val relatedWorkerId: Option[ActorVirtualIdentity] = None
) extends RuntimeException(message)
    with Serializable {

  def this(message: String, cause: Throwable, relatedWorkerId: Option[ActorVirtualIdentity]) = {
    this(message, relatedWorkerId)
    initCause(cause)
  }

  def this(cause: Throwable, relatedWorkerId: Option[ActorVirtualIdentity]) = {
    this(Option(cause).map(_.toString).orNull, cause, relatedWorkerId)
  }

  def this(cause: Throwable) = {
    this(Option(cause).map(_.toString).orNull, cause, None)
  }

  def this() = {
    this(null: String)
  }

  override def toString: String = message

}
