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

package edu.uci.ics.amber.engine.common.ambermessage

import akka.actor.{ActorRef, Address}
import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity

sealed trait RecoveryPayload extends Serializable {}

// Notify controller on worker recovery starts/ends
final case class UpdateRecoveryStatus(isRecovering: Boolean) extends RecoveryPayload

// Notify upstream worker to resend output to another worker for recovery
final case class ResendOutputTo(vid: ActorVirtualIdentity, ref: ActorRef) extends RecoveryPayload

// Notify controller when the machine fails and triggers recovery
final case class NotifyFailedNode(addr: Address) extends RecoveryPayload
