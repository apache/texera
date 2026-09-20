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

import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PhysicalOp, PortIdentity}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.{LogicalOp, StandaloneCodeGenerator}
import org.apache.texera.common.compiler.model.{LogicalLink, LogicalPlan}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The placeholder substitution, which is where an operator's generated code
  * meets the variables the script actually binds. A variadic port is the case
  * the numbered placeholders cannot state, so it is the case worth pinning.
  *
  * Every operator here is a stub. What the translator does is place a block and
  * bind the variables around it, so a stub that says exactly which block to
  * place keeps these assertions off any real operator's emitted text, which
  * would otherwise turn a change to that operator into a failure here.
  */
class WorkflowToPythonTranslatorSpec extends AnyFlatSpec with Matchers {

  private class StubOp(block: String) extends LogicalOp with StandaloneCodeGenerator {
    override def getPhysicalOp(
        workflowId: WorkflowIdentity,
        executionId: ExecutionIdentity
    ): PhysicalOp =
      throw new UnsupportedOperationException("the translator never builds a physical op")

    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        "Stub",
        "Stands in for an operator that implements the trait",
        OperatorGroupConstants.UTILITY_GROUP,
        inputPorts = List(InputPort()),
        outputPorts = List(OutputPort())
      )

    override def generateStandaloneCode(): String = block
  }

  private def stub(id: String, block: String): LogicalOp = {
    val op = new StubOp(block)
    op.setOperatorId(id)
    op
  }

  private def upstream(id: String): LogicalOp = stub(id, "out1df = in1df.copy()")

  private def link(from: LogicalOp, to: LogicalOp): LogicalLink =
    LogicalLink(from.operatorIdentifier, PortIdentity(0), to.operatorIdentifier, PortIdentity(0))

  /** `n` upstreams, all drawn into one port, which is what a variadic port looks
    * like in a plan.
    */
  private def variadicOf(n: Int): String = {
    val sink = stub("sink", "out1df = pd.concat(inAlldf, ignore_index=True)")
    val ups = (1 to n).map(i => upstream(s"up$i"))
    new WorkflowToPythonTranslator().translate(
      LogicalPlan(ups.toList :+ sink, ups.map(link(_, sink)).toList)
    )
  }

  "WorkflowToPythonTranslator" should "hand a variadic port every upstream it was drawn" in {
    variadicOf(3) should include("pd.concat([df1, df2, df3], ignore_index=True)")
  }

  it should "hand a variadic port a one-element list when only one link is drawn" in {
    // The case the old fixed `[in1df, in2df]` got wrong in the other direction:
    // it named a second frame the script never bound.
    variadicOf(1) should include("pd.concat([df1], ignore_index=True)")
  }

  it should "leave no placeholder behind for a variadic port" in {
    variadicOf(2) should not include "inAlldf"
  }

  // head() shows five rows and does not say how many there were, so a script whose
  // leaf holds more reads as if that were the whole answer.
  it should "print the leaf frame rather than its first rows" in {
    val script = variadicOf(2)
    script should include("print(df3)")
    script should not include ".head())"
  }

  // A script that only reshapes a table should run wherever pandas is installed,
  // so an import no operator in the plan asked for must not be in the header.
  it should "import pandas alone for a plan that asks for nothing else" in {
    val script = variadicOf(2)
    script should include("import pandas as pd")
    script should not include "import plotly"
  }

  // Two operators naming the same module yield one import, the way two operators
  // sharing one helper yield one copy of it.
  it should "emit an operator's declared import once per plan" in {
    val ops = List("a", "b").map { id =>
      val op = new StubOp("out1df = in1df.copy()") {
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
      LogicalPlan(List(first, second), List(link(first, second)))
    )
    script should include("df2 = df1.copy()")
  }

  /** Nothing stops a column from being named after a placeholder. The
    * substitution rewrites the variable a block reads with, never the column
    * name it asks that variable for.
    */
  it should "leave a column named after a placeholder alone" in {
    val source = upstream("source")
    val reader = stub("reader", """out1df = in1df[["in1df"]].copy()""")
    val script = new WorkflowToPythonTranslator().translate(
      LogicalPlan(List(source, reader), List(link(source, reader)))
    )
    script should include("""df2 = df1[["in1df"]].copy()""")
  }

  /** Two operators that write a file write two of them. The plan runs as one
    * program in one directory, so a name either of them had chosen for itself
    * would leave one picture where the workflow drew two.
    */
  it should "give each operator writing a file a name of its own" in {
    val ops = List("a", "b").map { id =>
      val op = new StubOp("fig.write_json(outputJson)\nfig.write_html(outputHtml)")
      op.setOperatorId(id)
      op
    }
    val script = new WorkflowToPythonTranslator().translate(LogicalPlan(ops, List.empty))
    script should include("""fig.write_json("stub_1.json")""")
    script should include("""fig.write_html("stub_1.html")""")
    script should include("""fig.write_html("stub_2.html")""")
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
