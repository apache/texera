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

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.core.JsonPointer
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, TextNode}
import org.apache.texera.amber.core.executor.OpExecWithCode
import org.apache.texera.amber.core.state.StateReferencing.{SIDECAR_PROPERTY, textReferences}
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{PhysicalOp, PortIdentity, SchemaPropagationFunc}
import org.apache.texera.amber.operator.PythonOperatorDescriptor._
import org.apache.texera.amber.operator.metadata.PropertyNameConstants
import org.apache.texera.amber.pybuilder.PythonLexerUtils.updateTripleQuotedStringState
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.decoderExpression
import org.apache.texera.amber.util.JSONUtils.objectMapper

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.Try

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

  /**
    * Each of `texts` (pointer -> name: a String property holding `$name`) that this descriptor
    * checks, or computes its output schemas from, given `inputSchemas`: pointer -> why, as the
    * compiler's error says it. Both happen before the loop runs, from the literal `$name`, so
    * neither sees the iteration's value.
    *
    * A copy of this descriptor with another text in the reference's place shows it: a column of
    * each type on each input, or another non-empty text among its settings, which a check commonly
    * compares the text to. Where only one of that copy and the copy it is compared with computes
    * its output schemas, or that copy fails to generate its code where the other does not, the
    * descriptor checks the text; where both compute them and they differ, the text decides them.
    * The copy compared with holds every reference's `$name`, unless its schemas fail: then it is
    * the first copy that computes them with such texts in the references' places, the same text
    * in each or each the next one, so that a reference that fails a check cannot hide another's.
    * When no copy can be made, nothing tells.
    *
    * A check against anything else goes unseen -- the empty text, or a constant the settings hold
    * nowhere -- and so does a read that shapes the code without failing it, such as a branch on a
    * sentinel or an order of columns: the copies' code counts only when it fails to generate.
    */
  def textsReadBeforeTheLoop(
      texts: Map[String, String],
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[String, String] =
    if (texts.isEmpty) {
      Map.empty
    } else {
      val tree = copyMapper.valueToTree[ObjectNode](this)
      tree.remove(SIDECAR_PROPERTY) // so that a copy refers to no loop variable
      val values = (columnPerType(inputSchemas) ++ settingTexts(tree)).toSet - ""
      val literals = texts.map { case (pointer, name) => pointer -> ("$" + name) }
      def copyWith(assignment: Map[String, String]): Option[PythonOperatorDescriptor] =
        Try {
          val json = assignment.foldLeft(tree) {
            case (copy, (pointer, text)) => withValue(copy, pointer, TextNode.valueOf(text))
          }
          copyMapper.treeToValue(json, getClass)
        }.toOption
      def schemasOf(copy: PythonOperatorDescriptor) =
        Try(copy.getOutputSchemas(inputSchemas)).toOption
      val pointers = texts.keys.toSeq.sorted
      val sorted = values.toSeq.sorted
      val assignments = Iterator(literals) ++ sorted.indices.iterator.flatMap { k =>
        Iterator(
          pointers.map(_ -> sorted(k)).toMap,
          pointers.zipWithIndex.map {
            case (pointer, i) => pointer -> sorted((k + i) % sorted.size)
          }.toMap
        )
      }
      val compared = assignments.distinct
        .flatMap(assignment =>
          copyWith(assignment).map(copy => (assignment, copy, schemasOf(copy)))
        )
        .find { case (_, _, schemas) => schemas.isDefined }
        .orElse(copyWith(literals).map(copy => (literals, copy, None)))
      compared.fold(Map.empty[String, String]) {
        case (assignment, base, schemas) =>
          lazy val generates = Try(base.generatePythonCode()).isSuccess
          texts.flatMap {
            case (pointer, _) =>
              // Each other text in the reference's place -- its own `$name` too, which is among the
              // settings -- built and judged once, and only as far as needed: its schemas, and
              // whether they or its code show a check.
              val outcomes = LazyList
                .from((values - assignment(pointer)).toSeq.sorted)
                .flatMap(text => copyWith(assignment.updated(pointer, text)))
                .map { copy =>
                  val own = schemasOf(copy)
                  own -> (own.isDefined != schemas.isDefined ||
                  (generates && Try(copy.generatePythonCode()).isFailure))
                }
              if (outcomes.exists { case (_, checks) => checks }) {
                Some(pointer -> checkedText)
              } else if (outcomes.exists { case (own, _) => own != schemas }) {
                Some(pointer -> textDecidesSchema)
              } else {
                None
              }
          }
      }
    }

}

object PythonOperatorDescriptor {

  /**
    * The call through which generated code reads loop variable `name` as text: pyamber's
    * `Operator.loop_variable_text` answers it from the iteration's state message.
    */
  def loopVariableLookup(name: String): String = s"self.loop_variable_text('$name')"

  // Why a text reference is not bound, as the compiler's error says it (`textsReadBeforeTheLoop`).
  val checkedText = "it checks the text before the loop runs"
  val textDecidesSchema = "the text decides its output schema"

  /**
    * `objectMapper`, but writing null properties as well: a copy parsed from a descriptor's JSON
    * keeps the nulls the descriptor holds, where the fields' defaults would take their place.
    */
  private lazy val copyMapper = objectMapper.copy().setSerializationInclusion(Include.ALWAYS)

  /** Every text `node` holds, however deep. */
  private def textsOf(node: JsonNode): Iterator[String] =
    if (node.isTextual) Iterator(node.asText()) else node.elements().asScala.flatMap(textsOf)

  /** The properties every descriptor's JSON holds beside its settings: no check reads them. */
  private val DescriptorProperties = Set(
    PropertyNameConstants.OPERATOR_ID,
    PropertyNameConstants.OPERATOR_VERSION,
    "operatorType",
    "inputPorts",
    "outputPorts"
  )

  /** The texts of a descriptor's settings, in its JSON `tree`, however deep. */
  private def settingTexts(tree: ObjectNode): Iterator[String] =
    tree
      .fields()
      .asScala
      .filterNot(field => DescriptorProperties(field.getKey))
      .flatMap(field => textsOf(field.getValue))

  /** The first column of each type on each input. */
  private def columnPerType(inputSchemas: Map[PortIdentity, Schema]): Iterable[String] =
    inputSchemas.values.flatMap(_.attributes.groupBy(_.getType).values.map(_.head.getName))

  /** A copy of `tree` with the value at `pointer` replaced by `node`. */
  private def withValue(tree: ObjectNode, pointer: String, node: JsonNode): ObjectNode = {
    val copy = tree.deepCopy()
    replaceAt(copy, pointer, node)
    copy
  }

  /** `tree` with the value at `pointer` replaced by `node`. */
  private def replaceAt(tree: ObjectNode, pointer: String, node: JsonNode): Unit = {
    val path = JsonPointer.compile(pointer)
    tree.at(path.head) match {
      case parent: ObjectNode => parent.replace(path.last.getMatchingProperty, node)
      case parent: ArrayNode  => parent.set(path.last.getMatchingIndex, node)
      case _                  => throw new IllegalStateException(s"no property at $pointer")
    }
  }

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
