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

package org.apache.texera.amber.operator.ifStatement

import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.state.State
import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.util.JSONUtils.objectMapper

class IfOpExec(descString: String) extends OperatorExecutor {
  private val desc: IfOpDesc = objectMapper.readValue(descString, classOf[IfOpDesc])
  private var outputPort: PortIdentity = PortIdentity(1) // by default, it should be the true port.

  // Whether a state arrived, whether one that arrived set the route by the condition, and the last
  // value of the condition that was not a Boolean.
  private var stateArrived = false
  private var conditionArrived = false
  private var nonBooleanCondition: Option[Any] = None

  // This function can handle one or more states, each with any number of key-value pairs; keys
  // other than conditionName are ignored. A state that carries conditionName sets the route by its
  // value: true takes the true port, false or null the false port, and any other value throws a
  // ClassCastException. A state without conditionName is passed on and keeps the route: inside a
  // loop, an operator upstream whose processState adds the condition sends the loop's state on
  // first, without it, and the change after it.
  override def processState(state: State, port: Int): Option[State] = {
    stateArrived = true
    state.values.get(desc.conditionName).foreach { value =>
      if (value != null && !value.isInstanceOf[Boolean]) nonBooleanCondition = Some(value)
      outputPort = if (value.asInstanceOf[Boolean]) PortIdentity(1) else PortIdentity()
      conditionArrived = true
    }
    Some(state)
  }

  // Before any state arrives, a tuple takes the true port. Once states arrived, a tuple fails
  // while none of them has set the route by the condition, instead of taking a route the condition
  // never chose: no state carried it, or the value it carried was not a Boolean.
  override def processTupleMultiPort(
      tuple: Tuple,
      port: Int
  ): Iterator[(TupleLike, Option[PortIdentity])] = {
    if (stateArrived && !conditionArrived) {
      throw new IllegalStateException(
        s"the If's condition refers to loop variable ${desc.conditionName}, but " +
          nonBooleanCondition.fold("no state message carried it")(value =>
            s"its value $value, a ${value.getClass.getSimpleName}, is not a Boolean"
          )
      )
    }
    Iterator((tuple, Some(outputPort)))
  }

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = ???
}
