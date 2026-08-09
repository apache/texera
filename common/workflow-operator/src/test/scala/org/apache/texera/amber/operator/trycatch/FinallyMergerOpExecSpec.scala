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

import org.apache.texera.amber.core.state.State
import org.apache.texera.amber.core.tuple.{AttributeType, Schema, Tuple, TupleLike}
// Tuple is used for the field-level assertions below
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FinallyMergerOpExecSpec extends AnyFlatSpec with Matchers {

  private val schema: Schema = Schema().add("field1", AttributeType.INTEGER)
  private def tuples(range: Range): List[Tuple] =
    range.map(i => TupleLike(i).enforceSchema(schema)).toList

  private val FROM_TRY = 0
  private val FROM_CATCH = 1
  private val TRY_RESULT = FinallyOpDesc.TRY_RESULT
  private val CATCH_RESULT = FinallyOpDesc.CATCH_RESULT

  private def mkMerger(ownCone: List[String] = List("myop/main")): FinallyMergerOpExec = {
    val config = new FinallyMergerConfig()
    config.ownConeOpIds = ownCone
    new FinallyMergerOpExec(objectMapper.writeValueAsString(config))
  }

  private val ownError = State.errorState("myop/main", "worker-0", new RuntimeException("boom"))
  private val foreignError =
    State.errorState("upstream/main", "worker-9", new RuntimeException("outer boom"))

  "FinallyMerger" should "flush the try side out the Try Result port on success" in {
    val merger = mkMerger()
    val tryResults = tuples(0 until 5)
    tryResults.foreach(t => merger.processTuple(t, FROM_TRY) shouldBe empty)
    // dependee phase: no output ports yet
    merger.onFinishMultiPort(FROM_TRY) shouldBe empty
    merger.onFinishMultiPort(FROM_CATCH).toList shouldBe
      tryResults.map(t => (t, Some(TRY_RESULT)))
  }

  it should "flush the catch side out the Catch Result port on failure" in {
    val merger = mkMerger()
    val attempt = tuples(0 until 3)
    val fallback = tuples(100 until 104)
    attempt.foreach(merger.processTuple(_, FROM_TRY))
    // own-cone failure arrives in-band: staged attempt discarded, error absorbed
    merger.processState(ownError, FROM_TRY) shouldBe None
    // the worker suppresses onFinishMultiPort(FROM_TRY) for the poisoned port — not called
    fallback.foreach(merger.processTuple(_, FROM_CATCH))
    merger.onFinishMultiPort(FROM_CATCH).toList shouldBe
      fallback.map(t => (t, Some(CATCH_RESULT)))
  }

  it should "emit nothing on double failure and forward the catch-side error (escalation)" in {
    val merger = mkMerger()
    tuples(0 until 3).foreach(merger.processTuple(_, FROM_TRY))
    merger.processState(ownError, FROM_TRY) shouldBe None
    tuples(100 until 102).foreach(merger.processTuple(_, FROM_CATCH))
    // catch-side error: forwarded downstream — that IS escalation
    merger.processState(ownError, FROM_CATCH) shouldBe Some(ownError)
    // the worker suppresses both onFinish calls (both ports poisoned): nothing flushed
  }

  it should "forward foreign errors on any port (they belong to an enclosing frame)" in {
    val merger = mkMerger()
    merger.processState(foreignError, FROM_TRY) shouldBe Some(foreignError)
    merger.processState(foreignError, FROM_CATCH) shouldBe Some(foreignError)
  }

  it should "pass ordinary States through" in {
    val merger = mkMerger()
    val envelope = State(Map("k" -> 1L))
    merger.processState(envelope, FROM_TRY) shouldBe Some(envelope)
  }

  it should "emit the winning branch's actual field values, unchanged" in {
    // Not just row COUNTS: the tuples that come out of Finally must be the
    // very rows the winning branch produced, with their values intact.
    val merger = mkMerger()
    val tryRows = List(
      TupleLike(11).enforceSchema(schema),
      TupleLike(22).enforceSchema(schema)
    )
    tryRows.foreach(merger.processTuple(_, FROM_TRY))
    merger.onFinishMultiPort(FROM_TRY)
    val emitted = merger.onFinishMultiPort(FROM_CATCH).toList
    emitted.map(_._2) shouldBe List(Some(TRY_RESULT), Some(TRY_RESULT))
    emitted.map(_._1) shouldBe tryRows
    emitted.map(_._1.asInstanceOf[Tuple].getField[Integer]("field1")) shouldBe List(11, 22)
  }

  it should "emit the catch branch's actual field values when the attempt failed" in {
    val merger = mkMerger()
    // the try attempt produced rows, then failed: those must NOT appear
    List(TupleLike(1).enforceSchema(schema)).foreach(merger.processTuple(_, FROM_TRY))
    merger.processState(ownError, FROM_TRY) shouldBe None
    val fallbackRows = List(
      TupleLike(77).enforceSchema(schema),
      TupleLike(88).enforceSchema(schema)
    )
    fallbackRows.foreach(merger.processTuple(_, FROM_CATCH))
    // onFinishMultiPort(FROM_TRY) is suppressed by the worker for the poisoned port
    val emitted = merger.onFinishMultiPort(FROM_CATCH).toList
    emitted.map(_._2) shouldBe List(Some(CATCH_RESULT), Some(CATCH_RESULT))
    emitted.map(_._1) shouldBe fallbackRows
    emitted.map(_._1.asInstanceOf[Tuple].getField[Integer]("field1")) shouldBe List(77, 88)
  }
}
