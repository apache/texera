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

package org.apache.texera.amber.operator.source

import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.testsupport.source.{SourceStubs, StubSource}
import org.scalatest.flatspec.AnyFlatSpec

class SourceOperatorDescriptorSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // sourceSchema — abstract member is observable
  // ---------------------------------------------------------------------------

  "SourceOperatorDescriptor (concrete subclass)" should
    "expose the `sourceSchema()` value supplied by the impl" in {
    val s = new StubSource
    assert(s.sourceSchema() == SourceStubs.testSchema)
  }

  // ---------------------------------------------------------------------------
  // Inheritance — SourceOperatorDescriptor is a LogicalOp
  // ---------------------------------------------------------------------------

  it should "be a LogicalOp (compile-time enforced)" in {
    val s: LogicalOp = new StubSource
    assert(s != null)
  }

  it should "match the LogicalOp type-pattern" in {
    val any: AnyRef = new StubSource
    val matched = any match {
      case _: LogicalOp => true
      case _            => false
    }
    assert(matched)
  }

  it should "match the SourceOperatorDescriptor type-pattern" in {
    val any: AnyRef = new StubSource
    val matched = any match {
      case _: SourceOperatorDescriptor => true
      case _                           => false
    }
    assert(matched)
  }
}
