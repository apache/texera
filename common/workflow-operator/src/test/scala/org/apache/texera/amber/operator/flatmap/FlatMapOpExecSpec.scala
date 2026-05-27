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

package org.apache.texera.amber.operator.flatmap

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple, TupleLike}
import org.scalatest.flatspec.AnyFlatSpec

class FlatMapOpExecSpec extends AnyFlatSpec {

  private val schema: Schema =
    Schema().add(new Attribute("v", AttributeType.INTEGER))

  private def tuple(v: Int): Tuple =
    Tuple.builder(schema).add(new Attribute("v", AttributeType.INTEGER), Integer.valueOf(v)).build()

  "FlatMapOpExec.processTuple" should "delegate to the configured flatMapFunc" in {
    val exec = new FlatMapOpExec()
    exec.setFlatMapFunc(t => Iterator(t, t))

    val out = exec.processTuple(tuple(1), 0).toList
    assert(out.size == 2)
    assert(out.forall(_.asInstanceOf[Tuple] == tuple(1)))
  }

  it should "apply a duplicating flatMap (1 → 2) across a stream of tuples" in {
    val exec = new FlatMapOpExec()
    exec.setFlatMapFunc(t => Iterator(t, t))
    val out = (1 to 4).flatMap(v => exec.processTuple(tuple(v), 0).toList)
    assert(out.size == 8)
    val expected = (1 to 4).flatMap(v => List(tuple(v), tuple(v)))
    assert(out.map(_.asInstanceOf[Tuple]) == expected)
  }

  it should "apply an expanding flatMap that fans out by the input value" in {
    val exec = new FlatMapOpExec()
    exec.setFlatMapFunc { (t: Tuple) =>
      val n = t.getField[Int]("v")
      (1 to n).map(_ => t).iterator
    }
    val out = exec.processTuple(tuple(3), 0).toList
    assert(out.size == 3)
    assert(out.forall(_.asInstanceOf[Tuple] == tuple(3)))
  }

  it should "apply a filtering flatMap that drops some inputs entirely" in {
    val exec = new FlatMapOpExec()
    // Keep only odd values
    exec.setFlatMapFunc { (t: Tuple) =>
      if (t.getField[Int]("v") % 2 == 1) Iterator.single(t) else Iterator.empty
    }
    val out = (1 to 5).flatMap(v => exec.processTuple(tuple(v), 0).toList)
    assert(out.map(_.asInstanceOf[Tuple]) == List(tuple(1), tuple(3), tuple(5)))
  }

  it should "apply a stateful flatMap (closes over an external counter)" in {
    val exec = new FlatMapOpExec()
    var counter = 0
    exec.setFlatMapFunc { (t: Tuple) =>
      counter += 1
      val emit = (1 to counter).map(_ => t)
      emit.iterator
    }
    val out = (0 until 3).flatMap(_ => exec.processTuple(tuple(7), 0).toList)
    // counter goes 1, 2, 3 → outputs 1+2+3 = 6 tuples
    assert(out.size == 6)
    assert(counter == 3)
  }

  it should "emit nothing when the flatMapFunc returns an empty iterator" in {
    val exec = new FlatMapOpExec()
    exec.setFlatMapFunc(_ => Iterator.empty)

    assert(exec.processTuple(tuple(1), 0).isEmpty)
  }

  it should "preserve the order of tuples emitted by the flatMapFunc" in {
    val exec = new FlatMapOpExec()
    exec.setFlatMapFunc(t => Iterator(tuple(99), t, tuple(0)))

    val out = exec.processTuple(tuple(7), 0).toList.map(_.asInstanceOf[Tuple])
    assert(out == List(tuple(99), tuple(7), tuple(0)))
  }

  "FlatMapOpExec.setFlatMapFunc" should "overwrite a previously installed function" in {
    val exec = new FlatMapOpExec()
    exec.setFlatMapFunc(_ => Iterator.empty)
    exec.setFlatMapFunc((t: Tuple) => Iterator[TupleLike](t))

    val out = exec.processTuple(tuple(5), 0).toList
    assert(out == List(tuple(5)))
  }

  it should "throw NullPointerException when processTuple is invoked before setFlatMapFunc" in {
    val exec = new FlatMapOpExec()
    assertThrows[NullPointerException] {
      // Iterator construction calls flatMapFunc(tuple) eagerly, so the NPE
      // surfaces here even though processTuple itself returns an iterator.
      exec.processTuple(tuple(1), 0)
    }
  }
}
