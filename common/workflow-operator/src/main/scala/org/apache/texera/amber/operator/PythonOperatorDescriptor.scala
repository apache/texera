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

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.texera.amber.core.executor.OpExecWithCode
import org.apache.texera.amber.core.state.StateReferencing.textReferences
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{PhysicalOp, PortIdentity, SchemaPropagationFunc}
import org.apache.texera.amber.operator.PythonOperatorDescriptor.loopVariableLookup
import org.apache.texera.amber.pybuilder.PythonLexerUtils.updateTripleQuotedStringState
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.decoderExpression
import org.apache.texera.amber.util.JSONUtils.objectMapper

import scala.annotation.tailrec

trait PythonOperatorDescriptor extends LogicalOp {
  private def generatePythonCodeForRaisingException(ex: Throwable): String = {
    s"#EXCEPTION DURING CODE GENERATION: ${ex.getMessage}"
  }

  /**
    * `code` with every loop variable that one of this descriptor's String properties refers to
    * read from the iteration's state. The code is generated before the loop runs, from the
    * literal `$name` the property holds; pyb renders that text as a decode expression the
    * operator evaluates when it runs, which becomes `loopVariableLookup(name)` instead. A typed
    * placeholder (0 / 0.0 / false) is already a value in the code, so it stays (the compiler
    * rejects it), and outside every loop block the sidecar is empty and the code is untouched.
    */
  private def withLoopVariableLookups(code: String): String =
    if (stateReferences.isEmpty) {
      code
    } else {
      textReferences(objectMapper.valueToTree[ObjectNode](this), stateReferences).values.toSet
        .foldLeft(code) { (rewritten, name) =>
          rewritten.replace(decoderExpression("$" + name), loopVariableLookup(name))
        }
    }

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    val pythonCode =
      try {
        withLoopVariableLookups(generatePythonCode())
      } catch {
        case ex: Throwable =>
          // instead of throwing error directly, we embed the error in the code
          // this can let upper-level compiler catch the error without interrupting the schema propagation
          generatePythonCodeForRaisingException(ex)
      }
    val physicalOp = if (asSource()) {
      PhysicalOp.sourcePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithCode(pythonCode, "python")
      )
    } else {
      PhysicalOp.oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithCode(pythonCode, "python")
      )
    }

    physicalOp
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withParallelizable(parallelizable())
      .withPropagateSchema(SchemaPropagationFunc(inputSchemas => getOutputSchemas(inputSchemas)))
  }

  def parallelizable(): Boolean = false

  def asSource(): Boolean = false

  /**
    * This method is to be implemented to generate the actual Python source code
    * based on operators predicates.
    *
    * @return a String representation of the executable Python source code.
    */
  def generatePythonCode(): String

  def getOutputSchemas(inputSchemas: Map[PortIdentity, Schema]): Map[PortIdentity, Schema]

}

object PythonOperatorDescriptor {

  /**
    * The call through which generated code reads loop variable `name` as text: pyamber's
    * `Operator.loop_variable_text` answers it from the iteration's state message.
    */
  def loopVariableLookup(name: String): String = s"self.loop_variable_text('$name')"

  /** The functions whose code runs while the operator is built or opened. */
  private val StartupFunctions = Set("__init__", "open")

  private val FunctionHeader = """(?:async\s+)?def\s+([A-Za-z_]\w*)\s*\(""".r
  private val ClassHeader = """class\s+[A-Za-z_]\w*\s*[(:]""".r

  /**
    * Whether generated `code` runs what it holds at `offset` when the operator starts: the
    * innermost function around `offset` is `__init__` or `open`, which run before the worker
    * starts, so before any state message can arrive. A function defined inside one of them may
    * run later, so the innermost one decides. Python nests the code by indentation: a line's
    * function is the closest `def` above it indented less than every block in between, past the
    * lines inside a triple-quoted text (`PythonLexerUtils`). Like pyb's own checks this is lexical,
    * which generated code, written from templates, keeps to.
    */
  def runsAtStartup(code: String, offset: Int): Boolean = {
    val lines = code.split("\n", -1)
    // Whether each line starts inside a triple-quoted text, so is no line of code of its own.
    val insideText = lines
      .scanLeft(Option.empty[String])((open, line) => updateTripleQuotedStringState(line, open))
      .map(_.nonEmpty)
    def indent(line: String): Int = line.takeWhile(c => c == ' ' || c == '\t').length
    def function(line: String): Option[String] =
      FunctionHeader.findPrefixMatchOf(line.trim).map(_.group(1))
    // The function of a line indented `bound`, from line `i` up: the first header indented less
    // opens the block the line is in; a block that is no function's sits in one indented less.
    @tailrec
    def enclosing(i: Int, bound: Int): Boolean =
      if (i < 0) {
        false
      } else {
        val line = lines(i)
        val trimmed = line.trim
        if (insideText(i) || trimmed.isEmpty || trimmed.startsWith("#") || indent(line) >= bound) {
          enclosing(i - 1, bound)
        } else if (ClassHeader.findPrefixOf(trimmed).nonEmpty) {
          false
        } else {
          function(line) match {
            case Some(name) => StartupFunctions(name)
            case None       => enclosing(i - 1, indent(line))
          }
        }
      }
    val at = code.substring(0, offset).count(_ == '\n')
    // The line where the statement holding `offset` starts: a text continues the line it opens.
    val start = (at to 0 by -1).find(!insideText(_)).getOrElse(0)
    // A function on one line, `def open(self): ...`, holds the statement itself.
    function(lines(start)) match {
      case Some(name) => StartupFunctions(name)
      case None       => enclosing(start - 1, indent(lines(start)))
    }
  }
}
