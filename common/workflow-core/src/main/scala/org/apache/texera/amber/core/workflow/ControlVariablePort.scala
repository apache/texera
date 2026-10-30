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

/**
  * The control-variable port that every operator has besides its data input ports.
  *
  * Whatever arrives on it becomes control variables of the operator: a control message (the
  * loop's state) is merged in, and a tuple is converted, one variable per column. The port has
  * no schema. Every edge into it ends before the operator starts, so the scheduler always
  * materializes such an edge.
  *
  * The port uses the reserved id -1, which no data port uses (data ports are numbered from 0),
  * so it needs no change to the port protos.
  */
object ControlVariablePort {
  val Id: PortIdentity = PortIdentity(-1)

  def is(port: PortIdentity): Boolean = port == Id

  val inputPort: InputPort = InputPort(Id, displayName = "control variables")
}
