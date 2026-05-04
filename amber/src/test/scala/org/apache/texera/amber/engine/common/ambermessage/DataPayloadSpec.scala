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

package org.apache.texera.amber.engine.common.ambermessage

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.scalatest.flatspec.AnyFlatSpec

class DataPayloadSpec extends AnyFlatSpec {

  private val vAttr = new Attribute("v", AttributeType.INTEGER)
  private val schema: Schema = Schema().add(vAttr)

  // Use the schema's Attribute when adding fields so the helper is always
  // consistent with the schema under test.
  private def tuple(v: Int): Tuple =
    Tuple.builder(schema).add(schema.getAttribute("v"), Integer.valueOf(v)).build()

  "DataFrame.inMemSize" should "be zero for an empty frame" in {
    assert(DataFrame(Array.empty).inMemSize == 0L)
  }

  it should "be the sum of inMemSize across the contained tuples" in {
    val a = tuple(1)
    val b = tuple(2)
    val df = DataFrame(Array(a, b))
    assert(df.inMemSize == a.inMemSize + b.inMemSize)
  }

  "DataFrame.equals" should "be reflexive on a single empty frame instance" in {
    val df = DataFrame(Array.empty)
    assert(df == df)
  }

  it should "consider two distinct empty frames equal" in {
    assert(DataFrame(Array.empty) == DataFrame(Array.empty))
  }

  it should "reject comparison against non-DataFrame values" in {
    val df = DataFrame(Array(tuple(1)))
    assert(!df.equals("not a dataframe"))
    assert(!df.equals(null))
  }

  it should "reject frames whose lengths differ" in {
    val a = DataFrame(Array(tuple(1)))
    val b = DataFrame(Array(tuple(1), tuple(2)))
    assert(a != b)
  }

  it should "treat element-wise equal frames as equal" in {
    val a = DataFrame(Array(tuple(1), tuple(2)))
    val b = DataFrame(Array(tuple(1), tuple(2)))
    assert(a == b)
  }

  it should "respect element order" in {
    val a = DataFrame(Array(tuple(1), tuple(2)))
    val b = DataFrame(Array(tuple(2), tuple(1)))
    assert(a != b)
  }

  it should "reject frames whose elements differ" in {
    val a = DataFrame(Array(tuple(1)))
    val b = DataFrame(Array(tuple(2)))
    assert(a != b)
  }
}
