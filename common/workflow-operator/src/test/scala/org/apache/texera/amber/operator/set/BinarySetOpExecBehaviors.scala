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

package org.apache.texera.amber.operator.set

import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.tuple._
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpecLike

/** Shared fixture and behavioral cases for two-port set-difference operator executors. */
trait BinarySetOpExecBehaviors extends AnyFlatSpecLike with BeforeAndAfter {

  protected val input1: Int = 0
  protected val input2: Int = 1
  protected var opExec: OperatorExecutor = _
  protected var counter: Int = 0

  protected val schema: Schema = Schema()
    .add(new Attribute("field1", AttributeType.STRING))
    .add(new Attribute("field2", AttributeType.INTEGER))
    .add(new Attribute("field3", AttributeType.BOOLEAN))

  protected def newExecutor(): OperatorExecutor

  protected def tuple(): Tuple = {
    counter += 1
    Tuple
      .builder(schema)
      .addSequentially(Array("hello", Int.box(counter), Boolean.box(true)))
      .build()
  }

  before {
    opExec = newExecutor()
  }

  protected def binarySetOpExecBehaviors(
      includeCommon: Boolean,
      includeLeftOnly: Boolean,
      includeRightOnly: Boolean
  ): Unit = {
    def whenLeftOnly(tuples: Seq[Tuple]): Set[TupleLike] =
      if (includeLeftOnly) tuples.toSet else Set.empty

    it should "open" in {
      opExec.open()
    }

    it should "work with basic two input streams with no duplicates" in {
      opExec.open()
      counter = 0
      val commonTuples = (1 to 10).map(_ => tuple()).toList

      (0 to 7).foreach(i => opExec.processTuple(commonTuples(i), input1))
      assert(opExec.onFinish(input1).isEmpty)

      (5 to 9).foreach(i => opExec.processTuple(commonTuples(i), input2))

      val expected =
        (if (includeCommon) commonTuples.slice(5, 8).toSet else Set.empty[TupleLike]) ++
          (if (includeLeftOnly) commonTuples.slice(0, 5).toSet else Set.empty[TupleLike]) ++
          (if (includeRightOnly) commonTuples.slice(8, 10).toSet else Set.empty[TupleLike])
      assert(opExec.onFinish(input2).toSet == expected)
      opExec.close()
    }

    it should "work with one empty input upstream after a data stream" in {
      opExec.open()
      counter = 0
      val commonTuples = (1 to 10).map(_ => tuple()).toList

      (0 to 9).foreach(i => opExec.processTuple(commonTuples(i), input1))
      assert(opExec.onFinish(input1).isEmpty)
      assert(opExec.onFinish(input2).toSet == whenLeftOnly(commonTuples))
      opExec.close()
    }

    it should "work with one empty input upstream after a data stream - other order" in {
      opExec.open()
      counter = 0
      val commonTuples = (1 to 10).map(_ => tuple()).toList

      (0 to 9).foreach(i => opExec.processTuple(commonTuples(i), input2))
      assert(opExec.onFinish(input2).isEmpty)

      val expected = if (includeRightOnly) commonTuples.toSet else Set.empty[TupleLike]
      assert(opExec.onFinish(input1).toSet == expected)
      opExec.close()
    }

    it should "work with one empty input upstream before a data stream" in {
      opExec.open()
      counter = 0
      val commonTuples = (1 to 10).map(_ => tuple()).toList

      assert(opExec.onFinish(input2).isEmpty)
      (0 to 9).foreach(i => opExec.processTuple(commonTuples(i), input1))
      assert(opExec.onFinish(input1).toSet == whenLeftOnly(commonTuples))
      opExec.close()
    }

    it should "work with one empty input upstream during a data stream" in {
      opExec.open()
      counter = 0
      val commonTuples = (1 to 10).map(_ => tuple()).toList

      (0 to 5).foreach(i => opExec.processTuple(commonTuples(i), input1))
      assert(opExec.onFinish(input2).isEmpty)
      (6 to 9).foreach(i => opExec.processTuple(commonTuples(i), input1))
      assert(opExec.onFinish(input1).toSet == whenLeftOnly(commonTuples))
      opExec.close()
    }

    it should "work with two empty input upstreams" in {
      opExec.open()
      assert(opExec.onFinish(input1).isEmpty)
      assert(opExec.onFinish(input2).isEmpty)
      opExec.close()
    }
  }
}
