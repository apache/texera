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

package org.apache.texera.amber.operator.macroterminal

import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.{LogicalOp, PortDescriptor}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}

/** Saved graph boundaries. MacroExpansion removes these before physical compilation. */
abstract class MacroTerminalOpDesc(input: Boolean) extends LogicalOp with PortDescriptor {
  operatorVersion = "1"
  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      if (input) "Inputs" else "Outputs",
      "Connect this macro boundary to the operators it exposes.",
      OperatorGroupConstants.UTILITY_GROUP,
      Option(inputPorts).getOrElse(Nil).zipWithIndex.map {
        case (port, index) =>
          InputPort(PortIdentity(index), displayName = port.displayName, disallowMultiLinks = true)
      },
      Option(outputPorts).getOrElse(Nil).zipWithIndex.map {
        case (port, index) =>
          OutputPort(PortIdentity(index), displayName = port.displayName)
      },
      dynamicInputPorts = !input,
      dynamicOutputPorts = input
    )
}

class MacroInputOpDesc extends MacroTerminalOpDesc(true)
class MacroOutputOpDesc extends MacroTerminalOpDesc(false)
