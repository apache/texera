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

import org.apache.texera.amber.operator.{LogicalOp, PythonOperatorDescriptor}
import org.apache.texera.amber.testsupport.source.{SourceStubs, StubPythonSource}
import org.scalatest.flatspec.AnyFlatSpec

class PythonSourceOperatorDescriptorSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Composition — extends BOTH SourceOperatorDescriptor and
  // PythonOperatorDescriptor
  // ---------------------------------------------------------------------------

  "PythonSourceOperatorDescriptor (concrete subclass)" should
    "be a SourceOperatorDescriptor (compile-time enforced)" in {
    val s: SourceOperatorDescriptor = new StubPythonSource
    assert(s.sourceSchema() == SourceStubs.testSchema)
  }

  it should "be a PythonOperatorDescriptor (compile-time enforced)" in {
    val s: PythonOperatorDescriptor = new StubPythonSource
    assert(s.generatePythonCode() == "yield {'col': 'value'}")
  }

  it should "be a LogicalOp (transitively, via SourceOperatorDescriptor)" in {
    val s: LogicalOp = new StubPythonSource
    assert(s != null)
  }

  // ---------------------------------------------------------------------------
  // Type-pattern matching — every layer of the composition is reachable
  // ---------------------------------------------------------------------------

  it should "match every type in the composition via pattern-matching" in {
    val any: AnyRef = new StubPythonSource
    assert(any.isInstanceOf[PythonSourceOperatorDescriptor])
    assert(any.isInstanceOf[SourceOperatorDescriptor])
    assert(any.isInstanceOf[PythonOperatorDescriptor])
    assert(any.isInstanceOf[LogicalOp])
  }

  // ---------------------------------------------------------------------------
  // Defaults inherited from PythonOperatorDescriptor (no override)
  // ---------------------------------------------------------------------------

  "PythonSourceOperatorDescriptor inherited defaults" should
    "default `parallelizable()` to false and `asSource()` to false unless overridden" in {
    val s = new StubPythonSource
    // Both are open methods on PythonOperatorDescriptor with the
    // documented `false` default. A concrete Python source typically
    // overrides `asSource()` to true; the stub does not, so the
    // default surfaces here.
    assert(!s.parallelizable())
    assert(!s.asSource())
  }
}
