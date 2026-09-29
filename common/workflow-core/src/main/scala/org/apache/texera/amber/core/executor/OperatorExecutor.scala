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

import java.util.concurrent.ConcurrentMap

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

  def close(): Unit = {}

  /**
    * The state message most recently handed to this executor, `None` until one arrives; inside a
    * loop block it carries the iteration's loop variables. The worker registers every message
    * right before `processState` runs, so the callback and every call after it can consult it.
    */
  final def state: Option[State] = OperatorExecutor.registrationOf(this).flatMap(_.state)

  /**
    * Registers `state` as this executor's `state`; the worker calls it for every state message,
    * right before `processState`. When the executor's setting (the descriptor its constructor
    * parsed from its descString) refers to loop variables, each one the message carries is also
    * written into that setting, in place, until the executor first sees data or finishes: see
    * `bindStateReferences`.
    */
  final def registerState(state: State): Unit = {
    val registration = OperatorExecutor.registrationFor(this)
    registration.state = Some(state)
    registration.references.foreach(_.write(state))
  }

  /**
    * The worker calls it right before this executor first sees data or finishes. It fails, naming
    * each reference of the setting that no state message could write, and otherwise ends the
    * writing: a state message after it is still registered and processed, but no longer changes
    * the setting. A no-op when the setting refers to no loop variable, and once it has succeeded.
    */
  final def bindStateReferences(): Unit =
    OperatorExecutor.registrationOf(this).foreach { registration =>
      registration.references.foreach { references =>
        val unbound = references.unbound
        if (unbound.nonEmpty) throw new IllegalStateException(unbound.mkString("; "))
        registration.references = None
      }
    }
}

object OperatorExecutor {

  /** What the worker registered on one executor, and the references of its setting. */
  private final class Registration {
    var state: Option[State] = None
    var references: Option[StateReferenceBinding] = None
  }

  // Kept beside the executors instead of in fields of the trait, which a Java class implementing it
  // (a Java UDF) would have to declare itself. Weak identity keys: an entry goes with its executor.
  private val registrations: ConcurrentMap[OperatorExecutor, Registration] =
    new MapMaker().weakKeys().makeMap[OperatorExecutor, Registration]()

  private def registrationOf(executor: OperatorExecutor): Option[Registration] =
    Option(registrations.get(executor))

  private def registrationFor(executor: OperatorExecutor): Registration =
    registrations.computeIfAbsent(executor, _ => new Registration)

  /** Has each state message registered on `executor` write into its setting (see `ExecFactory`). */
  private[executor] def attach(
      executor: OperatorExecutor,
      references: StateReferenceBinding
  ): Unit =
    registrationFor(executor).references = Some(references)
}
