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

package org.apache.texera.amber.translator

import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.distinct.DistinctOpDesc
import org.apache.texera.amber.operator.union.UnionOpDesc
import org.apache.texera.common.compiler.model.{LogicalLink, LogicalPlan}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The placeholder substitution, which is where an operator's generated code
  * meets the variables the script actually binds. A variadic port is the case
  * the numbered placeholders cannot state, so it is the case worth pinning.
  */
class WorkflowToPythonTranslatorSpec extends AnyFlatSpec with Matchers {

  private def upstream(id: String): LogicalOp = {
    val op = new DistinctOpDesc
    op.setOperatorId(id)
    op
  }

  /** `n` upstreams, all drawn into the union's single port, which is what a
    * variadic port looks like in a plan.
    */
  private def unionOf(n: Int): String = {
    val union = new UnionOpDesc
    union.setOperatorId("union")
    val ups = (1 to n).map(i => upstream(s"up$i"))
    val links = ups.map { up =>
      LogicalLink(
        up.operatorIdentifier,
        PortIdentity(0),
        union.operatorIdentifier,
        PortIdentity(0)
      )
    }
    new WorkflowToPythonTranslator().translate(
      LogicalPlan(ups.toList :+ union, links.toList)
    )
  }

  "WorkflowToPythonTranslator" should "hand a variadic port every upstream it was drawn" in {
    unionOf(3) should include("pd.concat([df1, df2, df3], ignore_index=True)")
  }

  it should "hand a variadic port a one-element list when only one link is drawn" in {
    // The case the old fixed `[in1df, in2df]` got wrong in the other direction:
    // it named a second frame the script never bound.
    unionOf(1) should include("pd.concat([df1], ignore_index=True)")
  }

  it should "leave no placeholder behind for a variadic port" in {
    unionOf(2) should not include "inAlldf"
  }

  // head() shows five rows and does not say how many there were, so a script whose
  // leaf holds more reads as if that were the whole answer.
  it should "print the leaf frame rather than its first rows" in {
    val script = unionOf(2)
    script should include("print(df3)")
    script should not include ".head())"
  }

  // A script that only reshapes a table should run wherever pandas is installed,
  // so an import no operator in the plan asked for must not be in the header.
  it should "import pandas alone for a plan that asks for nothing else" in {
    val script = unionOf(2)
    script should include("import pandas as pd")
    script should not include "import plotly"
  }

  // Two operators naming the same module yield one import, the way two operators
  // sharing one helper yield one copy of it.
  it should "emit an operator's declared import once per plan" in {
    val ops = List("a", "b").map { id =>
      val op = new DistinctOpDesc {
        override def standaloneImports(): Seq[String] = Seq("import numpy as np")
      }
      op.setOperatorId(id)
      op
    }
    val script = new WorkflowToPythonTranslator().translate(LogicalPlan(ops, List.empty))
    script.linesIterator.count(_ == "import numpy as np") shouldBe 1
  }

  it should "still resolve a numbered placeholder against its own upstream" in {
    // The variadic form is an addition, not a replacement: a chain of ordinary
    // single-input operators has to keep reading `in1df` as its predecessor.
    val first = upstream("first")
    val second = upstream("second")
    val script = new WorkflowToPythonTranslator().translate(
      LogicalPlan(
        List(first, second),
        List(
          LogicalLink(
            first.operatorIdentifier,
            PortIdentity(0),
            second.operatorIdentifier,
            PortIdentity(0)
          )
        )
      )
    )
    script should include("df2 = df1.drop_duplicates(ignore_index=True)")
  }

  /** The translator's own contract when it meets an operator it cannot render:
    * a comment rather than a silently wrong line.
    */
  it should "leave a TODO for an operator with no standalone code generator" in {
    val op = new org.apache.texera.amber.operator.udf.python.PythonUDFOpDescV2
    op.setOperatorId("udf")
    val script = new WorkflowToPythonTranslator().translate(LogicalPlan(List(op), List.empty))
    script should include("# TODO:")
  }
}
