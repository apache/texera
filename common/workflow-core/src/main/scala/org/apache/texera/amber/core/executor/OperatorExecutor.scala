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

package org.apache.texera.amber.core.executor

import com.google.common.collect.MapMaker
import org.apache.texera.amber.core.state.State
import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}
import org.apache.texera.amber.core.workflow.PortIdentity

/**
  * Inside a loop block a setting may refer to a loop variable (`$i`): the worker writes each
  * iteration's value into the settings object the executor holds, in place (`registerState`). For
  * its settings to take loop variables, an executor built from a descString must:
  *  - keep exactly one settings object, the descriptor it parses from its descString, in a field;
  *    with none or several the worker fails to start;
  *  - read its settings only while processing data (`processState`, `processTuple`, the finish
  *    callbacks), never in its constructor, `open` or `produceStateOnStart`, which run before the
  *    state arrives: a value worked out there silently keeps the placeholder.
  */
trait OperatorExecutor {

  def open(): Unit = {}

  def produceStateOnStart(port: Int): Option[State] = None

  def processState(state: State, port: Int): Option[State] = Some(state)

  def processTupleMultiPort(
      tuple: Tuple,
      port: Int
  ): Iterator[(TupleLike, Option[PortIdentity])] = {
    processTuple(tuple, port).map(t => (t, None))
  }

  def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike]

  def produceStateOnFinish(port: Int): Option[State] = None

  def onFinishMultiPort(port: Int): Iterator[(TupleLike, Option[PortIdentity])] = {
    onFinish(port).map(t => (t, None))
  }

  def onFinish(port: Int): Iterator[TupleLike] = Iterator.empty

  /** Non-fatal warnings the worker surfaces as console messages once the executor
    * completes (e.g. scan rows skipped for not matching the inferred schema).
    */
  def getWarnings: Seq[String] = Seq.empty

  def close(): Unit = {}

  /**
    * The state message most recently registered on this executor, `None` until one arrives; inside
    * a loop block it carries the iteration's loop variables.
    */
  final def state: Option[State] = Option(OperatorExecutor.states.get(this))

  /**
    * The worker calls it for every state message, right before `processState`, with the message's
    * `loopCounter`: 0 from the innermost loop around this operator, 1 from the one around that, and
    * so on. Until `bindStateReferences`, it also writes each loop variable the message carries that
    * the executor's setting (the descriptor it holds) refers to into that setting, in place. It
    * fails when a message from the same loop gives such a variable another value.
    */
  final def registerState(state: State, loopCounter: Long = 0L): Unit = {
    Option(OperatorExecutor.bindings.get(this)).foreach(_.write(state, loopCounter))
    OperatorExecutor.states.put(this, state)
  }

  /**
    * The worker calls it right before this executor first sees data or finishes. It fails, naming
    * each reference no state message could write, and otherwise ends the writing: later messages
    * no longer change the setting.
    */
  final def bindStateReferences(): Unit =
    Option(OperatorExecutor.bindings.get(this)).foreach(_.bind())
}

object OperatorExecutor {

  // Kept beside the executors instead of in fields of the trait, which a Java class implementing it
  // (a Java UDF) would have to declare itself. Weak identity keys: an entry goes with its executor.
  private val states = new MapMaker().weakKeys().makeMap[OperatorExecutor, State]()
  private[executor] val bindings =
    new MapMaker().weakKeys().makeMap[OperatorExecutor, StateReferenceBinding]()
}
