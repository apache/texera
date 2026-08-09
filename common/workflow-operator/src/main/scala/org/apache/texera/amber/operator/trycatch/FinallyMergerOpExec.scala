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

package org.apache.texera.amber.operator.trycatch

import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.state.State
import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.util.JSONUtils.objectMapper

import scala.collection.mutable.ArrayBuffer

/**
  * The Finally physical operator: stages both sides, flushes the winner out of
  * the output port named after it (`Try Result` on success, `Catch Result` on
  * failure) — the outcome is part of the output, not just the rows.
  *
  * Port 0 (`From Try`) is consumed first (it is the dependee of `From Catch`)
  * during the dependee phase, in which this operator has no output ports yet —
  * so the try-side results are buffered and both release decisions happen at
  * `onFinishMultiPort(From Catch)` in the second phase:
  *   - try side completed cleanly  => flush the staged try results to
  *     `Try Result` (the catch subgraph received nothing from its gate and
  *     stays empty);
  *   - try side failed             => flush the staged catch results to
  *     `Catch Result`.
  *
  * Failure handling costs no code here: an own-cone error State on `From Try`
  * clears the staged attempt and is absorbed (a caught failure must not leak
  * past the frame); per-port drain in the worker discards later tuples and
  * suppresses `onFinish` on the poisoned port. Catch-side error States are
  * forwarded (default pass-through) — that is escalation to any enclosing
  * frame; with both flushes suppressed, nothing is emitted (double failure).
  */
class FinallyMergerOpExec(descString: String) extends OperatorExecutor {

  private val config: FinallyMergerConfig =
    objectMapper.readValue(descString, classOf[FinallyMergerConfig])
  private val ownCone: Set[String] = config.ownConeOpIds.toSet

  private val FROM_TRY = 0
  private val FROM_CATCH = 1

  private val stagedTry = new ArrayBuffer[Tuple]()
  private val stagedCatch = new ArrayBuffer[Tuple]()
  private var trySideClean = false

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    if (port == FROM_TRY) stagedTry.append(tuple) else stagedCatch.append(tuple)
    Iterator.empty
  }

  override def processState(state: State, port: Int): Option[State] = {
    if (
      port == FROM_TRY && State.isError(state) &&
      State.errorOperatorId(state).exists(ownCone.contains)
    ) {
      // caught: discard the failed attempt's staged output and absorb the
      // error — it must not leak past the frame
      stagedTry.clear()
      None
    } else {
      // catch-side errors and foreign errors travel on (escalation);
      // ordinary States (loop envelope, user States) pass through
      Some(state)
    }
  }

  override def onFinishMultiPort(port: Int): Iterator[(TupleLike, Option[PortIdentity])] = {
    if (port == FROM_TRY) {
      // dependee phase: no output ports exist yet; just record the outcome
      // (this callback is suppressed by the worker if the port was poisoned)
      trySideClean = true
      Iterator.empty
    } else if (trySideClean) {
      stagedTry.iterator.map(t => (t, Some(FinallyOpDesc.TRY_RESULT)))
    } else {
      stagedCatch.iterator.map(t => (t, Some(FinallyOpDesc.CATCH_RESULT)))
    }
  }
}
