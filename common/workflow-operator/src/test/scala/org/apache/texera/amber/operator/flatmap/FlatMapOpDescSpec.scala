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

import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import java.lang.reflect.Modifier
import org.scalatest.flatspec.AnyFlatSpec

class FlatMapOpDescSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Test-only concrete subclass
  // ---------------------------------------------------------------------------

  private class StubFlatMap extends FlatMapOpDesc {
    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        "StubFlatMap",
        "stub flatmap",
        OperatorGroupConstants.UTILITY_GROUP,
        inputPorts = List.empty,
        outputPorts = List.empty
      )
  }

  // ---------------------------------------------------------------------------
  // Abstract-class shape
  // ---------------------------------------------------------------------------

  "FlatMapOpDesc" should "be declared abstract (cannot be instantiated directly)" in {
    assert(Modifier.isAbstract(classOf[FlatMapOpDesc].getModifiers))
  }

  // ---------------------------------------------------------------------------
  // Inheritance — FlatMapOpDesc is a LogicalOp
  // ---------------------------------------------------------------------------

  it should "extend LogicalOp (compile-time enforced)" in {
    val s: LogicalOp = new StubFlatMap
    assert(s != null)
  }

  it should "match the LogicalOp type-pattern" in {
    val any: AnyRef = new StubFlatMap
    val matched = any match {
      case _: LogicalOp => true
      case _            => false
    }
    assert(matched)
  }

  it should "match the FlatMapOpDesc type-pattern" in {
    val any: AnyRef = new StubFlatMap
    val matched = any match {
      case _: FlatMapOpDesc => true
      case _                => false
    }
    assert(matched)
  }
}
