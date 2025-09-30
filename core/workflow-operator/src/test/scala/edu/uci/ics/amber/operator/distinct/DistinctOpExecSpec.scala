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

package edu.uci.ics.amber.operator.distinct

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple, TupleLike}
class DistinctOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val tupleSchema: Schema = Schema()
    .add(new Attribute("field1", AttributeType.STRING))
    .add(new Attribute("field2", AttributeType.INTEGER))
    .add(new Attribute("field3", AttributeType.BOOLEAN))

  val tuple: () => Tuple = () =>
    Tuple
      .builder(tupleSchema)
      .add(new Attribute("field1", AttributeType.STRING), "hello")
      .add(new Attribute("field2", AttributeType.INTEGER), 1)
      .add(
        new Attribute("field3", AttributeType.BOOLEAN),
        true
      )
      .build()

  val tuple2: () => Tuple = () =>
    Tuple
      .builder(tupleSchema)
      .add(new Attribute("field1", AttributeType.STRING), "hello")
      .add(new Attribute("field2", AttributeType.INTEGER), 2)
      .add(
        new Attribute("field3", AttributeType.BOOLEAN),
        false
      )
      .build()

  var opExec: DistinctOpExec = _
  before {
    opExec = new DistinctOpExec()
  }

  it should "open" in {

    opExec.open()

  }

  it should "remove duplicate Tuple with the same content" in {

    opExec.open()
    (1 to 1000).map(_ => {
      opExec.processTuple(tuple(), 0)
    })

    val outputTuples: List[TupleLike] =
      opExec.onFinish(0).toList
    assert(outputTuples.size == 1)
    assert(outputTuples.head.equals(tuple()))
    opExec.close()
  }

  it should "preserve the insertion order" in {

    opExec.open()
    (1 to 1000).map(_ => {
      opExec.processTuple(tuple(), 0)
    })
    (1 to 1000).map(_ => {
      opExec.processTuple(tuple2(), 0)
    })
    (1 to 1000).map(_ => {
      opExec.processTuple(tuple(), 0)
    })

    val outputTuples: List[TupleLike] =
      opExec.onFinish(0).toList
    assert(outputTuples.size == 2)
    assert(outputTuples.head.equals(tuple()))
    assert(outputTuples.apply(1).equals(tuple2()))
    opExec.close()
  }

}
