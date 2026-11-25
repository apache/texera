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

package org.apache.texera.amber.operator

import org.apache.texera.amber.core.executor.{OpExecWithClassName, OperatorExecutor}
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.amber.util.PythonTestSupport.{canImport, configuredPython, runSource}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

abstract class FilterSearchSpecSupport extends AnyFlatSpec with Matchers {

  protected val bodyAttr: Attribute = new Attribute("body", AttributeType.STRING)
  protected val bodySchema: Schema = Schema().add(bodyAttr)

  protected def tuple(text: String): Tuple =
    Tuple.builder(bodySchema).add(bodyAttr, text).build()

  protected def assertMatches(exec: OperatorExecutor, text: String): Unit = {
    val t = tuple(text)
    assert(exec.processTuple(t, port = 0).toList == List(t))
  }

  protected def assertMisses(exec: OperatorExecutor, text: String): Unit =
    assert(exec.processTuple(tuple(text), port = 0).toList.isEmpty)

  protected def assertPhysicalOpWired(op: LogicalOp, expectedClassName: String): Unit = {
    val physical = op.getPhysicalOp(WorkflowIdentity(1L), ExecutionIdentity(1L))
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe expectedClassName
        descString should not be empty
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    // Pin the actual port identities (not just counts).
    physical.inputPorts.keySet shouldBe op.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
  }

  protected def assertStandaloneParity[D <: StandaloneCodeGenerator](
      prefix: String,
      flagName: String,
      cases: Seq[(String, Boolean, Seq[String])],
      extraPreamble: Seq[String] = Nil
  )(buildDesc: (String, Boolean) => D)(buildExec: String => OperatorExecutor): Unit = {
    val python = configuredPython().getOrElse(cancel("No runnable python executable"))
    if (!canImport(python, "pandas")) cancel(s"'$python' cannot import pandas")

    val schema = Schema().add("v", AttributeType.STRING).add("i", AttributeType.INTEGER)
    val runs = cases.map {
      case (needle, flag, values) =>
        val desc = buildDesc(needle, flag)
        val exec = buildExec(objectMapper.writeValueAsString(desc))
        exec.open()
        val kept = values.zipWithIndex.flatMap {
          case (v, i) => exec.processTuple(Tuple(schema, Array[Any](v, i)), 0)
        }
        exec.close()
        val label = s"'$needle' $flagName=$flag"
        val engine = s"$label: ${kept.map(_.getFields(1)).mkString(" ")}"
        val code = (desc.standaloneHelpers() :+
          desc.generateStandaloneCode(Map(PortIdentity() -> schema))).mkString("\n")
        val script =
          s"""in1df = pd.DataFrame({"v": pd.Series(${objectMapper.writeValueAsString(
            values
          )}, dtype=object)})
             |in1df["i"] = range(len(in1df))
             |ns = {"pd": pd, "in1df": in1df}
             |exec(${objectMapper.writeValueAsString(code)}, ns)
             |print(${objectMapper.writeValueAsString(
            label + ":"
          )}, " ".join(str(i) for i in ns["out1df"]["i"]))""".stripMargin
        (engine, script)
    }

    val driver = (("import pandas as pd" +: extraPreamble) ++ runs.map(_._2)).mkString("\n")
    val (exitCode, out) = runSource(python, prefix, driver)
    withClue(s"python said:\n$out\n") {
      exitCode shouldBe 0
      out.linesIterator.map(_.trim).toList shouldBe runs.map(_._1.trim).toList
    }
  }
}
