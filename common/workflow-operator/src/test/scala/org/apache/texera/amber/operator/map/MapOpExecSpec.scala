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

package org.apache.texera.amber.operator.map

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.scalatest.flatspec.AnyFlatSpec

class MapOpExecSpec extends AnyFlatSpec {

  private val vAttr = new Attribute("v", AttributeType.INTEGER)
  private val schema: Schema = Schema().add(vAttr)

  // Use the schema's Attribute when adding fields so the helper stays
  // consistent with the schema under test.
  private def tuple(v: Int): Tuple =
    Tuple.builder(schema).add(schema.getAttribute("v"), Integer.valueOf(v)).build()

  private class TestMap extends MapOpExec

  "MapOpExec.processTuple" should "emit exactly one tuple per input by applying the configured mapFunc" in {
    val exec = new TestMap()
    exec.setMapFunc((t: Tuple) => tuple(t.getField[Int]("v") * 2))

    val out = exec.processTuple(tuple(3), 0).toList
    assert(out == List(tuple(6)))
  }

  it should "apply a doubling map function to a stream of tuples" in {
    val exec = new TestMap()
    exec.setMapFunc((t: Tuple) => tuple(t.getField[Int]("v") * 2))
    val out = (1 to 5).flatMap(v => exec.processTuple(tuple(v), 0).toList)
    assert(out.map(_.asInstanceOf[Tuple]) == (1 to 5).map(v => tuple(v * 2)))
  }

  it should "apply a constant map function regardless of input" in {
    val exec = new TestMap()
    exec.setMapFunc((_: Tuple) => tuple(99))
    val out = Seq(1, 2, 3).map(v => exec.processTuple(tuple(v), 0).toList.head.asInstanceOf[Tuple])
    assert(out.forall(_ == tuple(99)))
  }

  it should "apply a stateful map function (closes over an external counter)" in {
    val exec = new TestMap()
    var counter = 0
    exec.setMapFunc { (t: Tuple) =>
      counter += 1
      tuple(t.getField[Int]("v") + counter)
    }
    val out = (1 to 3).map(v => exec.processTuple(tuple(v), 0).toList.head.asInstanceOf[Tuple])
    // counter goes 1, 2, 3 → outputs 1+1, 2+2, 3+3
    assert(out == List(tuple(2), tuple(4), tuple(6)))
    assert(counter == 3)
  }

  it should "support a map function that produces a tuple with a different schema" in {
    val outSchema =
      Schema().add(new Attribute("name", AttributeType.STRING))
    val exec = new TestMap()
    exec.setMapFunc { (t: Tuple) =>
      Tuple
        .builder(outSchema)
        .add(outSchema.getAttribute("name"), s"v=${t.getField[Int]("v")}")
        .build()
    }
    val out = exec.processTuple(tuple(7), 0).toList
    assert(out.size == 1)
    val mapped = out.head.asInstanceOf[Tuple]
    assert(mapped.getField[String]("name") == "v=7")
  }

  it should "return the same instance when mapFunc returns the input tuple" in {
    val exec = new TestMap()
    exec.setMapFunc((t: Tuple) => t)

    val input = tuple(7)
    val out = exec.processTuple(input, 0).toList
    assert(out.size == 1)
    // Reference identity: processTuple should not copy or rebuild the tuple
    // when mapFunc returns the same instance.
    assert(out.head eq input)
  }

  it should "always wrap the result in a single-element iterator" in {
    val exec = new TestMap()
    exec.setMapFunc((_: Tuple) => tuple(0))

    val it = exec.processTuple(tuple(99), 0)
    assert(it.hasNext)
    it.next()
    assert(!it.hasNext)
  }

  "MapOpExec.setMapFunc" should "overwrite a previously installed function" in {
    val exec = new TestMap()
    exec.setMapFunc((_: Tuple) => tuple(1))
    exec.setMapFunc((_: Tuple) => tuple(2))

    val out = exec.processTuple(tuple(0), 0).toList
    assert(out == List(tuple(2)))
  }

  it should "throw NullPointerException when mapFunc is invoked before setMapFunc" in {
    val exec = new TestMap()
    assertThrows[NullPointerException] {
      exec.processTuple(tuple(0), 0).toList
    }
  }
}
