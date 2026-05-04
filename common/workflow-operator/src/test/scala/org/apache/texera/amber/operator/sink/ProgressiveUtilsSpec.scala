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

package org.apache.texera.amber.operator.sink

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.scalatest.flatspec.AnyFlatSpec

class ProgressiveUtilsSpec extends AnyFlatSpec {

  // --- helpers ---------------------------------------------------------------

  private val baseSchema: Schema = new Schema(
    new Attribute("id", AttributeType.INTEGER),
    new Attribute("name", AttributeType.STRING)
  )

  // outputSchema = flag column prepended to baseSchema
  private val outputSchema: Schema = new Schema(
    ProgressiveUtils.insertRetractFlagAttr,
    new Attribute("id", AttributeType.INTEGER),
    new Attribute("name", AttributeType.STRING)
  )

  private def baseTuple(id: Int, name: String): Tuple =
    Tuple
      .builder(baseSchema)
      .add(new Attribute("id", AttributeType.INTEGER), Int.box(id))
      .add(new Attribute("name", AttributeType.STRING), name)
      .build()

  // --- insertRetractFlagAttr -------------------------------------------------

  "ProgressiveUtils.insertRetractFlagAttr" should "be a BOOLEAN attribute named __internal_is_insertion" in {
    val attr = ProgressiveUtils.insertRetractFlagAttr
    assert(attr.getName == "__internal_is_insertion")
    assert(attr.getType == AttributeType.BOOLEAN)
  }

  // --- addInsertionFlag / addRetractionFlag ----------------------------------

  "ProgressiveUtils.addInsertionFlag" should "prepend the flag column with value true" in {
    val flagged = ProgressiveUtils.addInsertionFlag(baseTuple(1, "alice"), outputSchema)
    assert(flagged.getSchema == outputSchema)
    assert(flagged.getField[Boolean](ProgressiveUtils.insertRetractFlagAttr.getName) == true)
    assert(flagged.getField[Integer]("id") == 1)
    assert(flagged.getField[String]("name") == "alice")
  }

  "ProgressiveUtils.addRetractionFlag" should "prepend the flag column with value false" in {
    val flagged = ProgressiveUtils.addRetractionFlag(baseTuple(2, "bob"), outputSchema)
    assert(flagged.getField[Boolean](ProgressiveUtils.insertRetractFlagAttr.getName) == false)
    assert(flagged.getField[Integer]("id") == 2)
    assert(flagged.getField[String]("name") == "bob")
  }

  it should "fail an assertion if addRetractionFlag is called on an already-flagged tuple" in {
    val alreadyFlagged = ProgressiveUtils.addInsertionFlag(baseTuple(3, "x"), outputSchema)
    intercept[AssertionError] {
      ProgressiveUtils.addRetractionFlag(alreadyFlagged, outputSchema)
    }
  }

  it should "fail an assertion if addInsertionFlag is called on an already-flagged tuple" in {
    // Symmetric guard: both addInsertionFlag and addRetractionFlag carry the
    // same `assert(!containsAttribute(flagAttr))` precondition, and either
    // one may be called on already-flagged data, so each path should fail.
    val alreadyFlagged = ProgressiveUtils.addRetractionFlag(baseTuple(4, "y"), outputSchema)
    intercept[AssertionError] {
      ProgressiveUtils.addInsertionFlag(alreadyFlagged, outputSchema)
    }
  }

  // --- isInsertion -----------------------------------------------------------

  "ProgressiveUtils.isInsertion" should "return true for an unflagged tuple" in {
    // Tuples without the flag column default to insertion (the unflagged
    // default in the engine is "+").
    assert(ProgressiveUtils.isInsertion(baseTuple(1, "x")))
  }

  it should "return true when the flag column is present and true" in {
    val flagged = ProgressiveUtils.addInsertionFlag(baseTuple(1, "x"), outputSchema)
    assert(ProgressiveUtils.isInsertion(flagged))
  }

  it should "return false when the flag column is present and false" in {
    val flagged = ProgressiveUtils.addRetractionFlag(baseTuple(1, "x"), outputSchema)
    assert(!ProgressiveUtils.isInsertion(flagged))
  }

  // --- getTupleFlagAndValue --------------------------------------------------

  "ProgressiveUtils.getTupleFlagAndValue" should "split an insertion-flagged tuple into (true, base tuple)" in {
    val flagged = ProgressiveUtils.addInsertionFlag(baseTuple(1, "alice"), outputSchema)
    val (flag, stripped) = ProgressiveUtils.getTupleFlagAndValue(flagged)
    assert(flag)
    // Full schema equality (names + types + order) — name-only would let a
    // type drift on the payload columns slip through.
    assert(stripped.getSchema == baseSchema)
    assert(stripped.getField[Integer]("id") == 1)
    assert(stripped.getField[String]("name") == "alice")
  }

  it should "split a retraction-flagged tuple into (false, base tuple)" in {
    val flagged = ProgressiveUtils.addRetractionFlag(baseTuple(2, "bob"), outputSchema)
    val (flag, stripped) = ProgressiveUtils.getTupleFlagAndValue(flagged)
    assert(!flag)
    assert(stripped.getSchema == baseSchema)
    assert(stripped.getField[Integer]("id") == 2)
    assert(stripped.getField[String]("name") == "bob")
  }

  it should "treat an unflagged tuple as insertion and pass an equivalent schema through unchanged" in {
    // For a tuple that doesn't carry the flag column, isInsertion returns
    // true and getPartialSchema returns an equivalent Schema — same attributes
    // in the same order (filterNot removes nothing). Note that
    // Schema.getPartialSchema constructs a new instance every time, so this
    // is structural equality, not reference identity.
    val raw = baseTuple(3, "carol")
    val (flag, stripped) = ProgressiveUtils.getTupleFlagAndValue(raw)
    assert(flag)
    assert(stripped.getSchema == raw.getSchema)
    assert(stripped.getField[Integer]("id") == 3)
    assert(stripped.getField[String]("name") == "carol")
  }

  // --- typed payload round-trips --------------------------------------------
  // Nothing in `addInsertionFlag` / `getTupleFlagAndValue` is type-specific —
  // they only care about the BOOLEAN flag column they prepend / strip — but
  // it is worth pinning that arbitrary AttributeType payload columns survive
  // the flag → strip → unflag round-trip across the engine's value types.

  private def flagRoundTrip(payloadAttr: Attribute, payloadValue: AnyRef): (Boolean, AnyRef) = {
    val payloadSchema = new Schema(payloadAttr)
    val flaggedSchema = new Schema(ProgressiveUtils.insertRetractFlagAttr, payloadAttr)
    val raw = Tuple.builder(payloadSchema).add(payloadAttr, payloadValue).build()
    val flagged = ProgressiveUtils.addInsertionFlag(raw, flaggedSchema)
    val (flag, stripped) = ProgressiveUtils.getTupleFlagAndValue(flagged)
    (flag, stripped.getField[AnyRef](payloadAttr.getName))
  }

  "Flag round-trip" should "preserve INTEGER payload values" in {
    val (flag, value) =
      flagRoundTrip(new Attribute("v", AttributeType.INTEGER), Int.box(42))
    assert(flag)
    assert(value == Int.box(42))
  }

  it should "preserve LONG payload values" in {
    val (flag, value) =
      flagRoundTrip(new Attribute("v", AttributeType.LONG), Long.box(9876543210L))
    assert(flag)
    assert(value == Long.box(9876543210L))
  }

  it should "preserve DOUBLE payload values" in {
    val (flag, value) =
      flagRoundTrip(new Attribute("v", AttributeType.DOUBLE), Double.box(3.14159))
    assert(flag)
    assert(value == Double.box(3.14159))
  }

  it should "preserve BOOLEAN payload values (distinct from the flag column)" in {
    // The flag column is also BOOLEAN; this verifies the implementation
    // selects the correct attribute by name, not by type.
    val (flag, value) =
      flagRoundTrip(new Attribute("active", AttributeType.BOOLEAN), Boolean.box(false))
    assert(flag, "outer flag must still be insertion")
    assert(value == Boolean.box(false), "inner BOOLEAN payload must be preserved")
  }

  it should "preserve TIMESTAMP payload values" in {
    val ts = new java.sql.Timestamp(1_700_000_000_000L)
    val (flag, value) =
      flagRoundTrip(new Attribute("ts", AttributeType.TIMESTAMP), ts)
    assert(flag)
    assert(value == ts)
  }

  it should "preserve BINARY payload values" in {
    val bytes = Array[Byte](0, 1, 2, 3, -1)
    val (flag, value) =
      flagRoundTrip(new Attribute("blob", AttributeType.BINARY), bytes)
    assert(flag)
    // Use value-based equality (the Tuple contract elsewhere uses
    // `sameElements` for Array[Byte]); requiring the *same* array instance
    // would over-constrain the flag/strip path against future copy-on-write
    // changes.
    assert(value.asInstanceOf[Array[Byte]].sameElements(bytes))
  }

  it should "preserve null payload values for every AttributeType" in {
    // Cover every member of `AttributeType` (Java enum). Avoid hand-listing —
    // a future addition to the enum would still be tested.
    AttributeType.values.foreach { tpe =>
      val attr = new Attribute(s"v_${tpe.name().toLowerCase}", tpe)
      val (flag, value) = flagRoundTrip(attr, null)
      assert(flag)
      assert(value == null, s"null payload must survive round-trip for $tpe")
    }
  }
}
