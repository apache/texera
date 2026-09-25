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
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode}
import org.apache.texera.amber.core.executor.OpExecWithCode
import org.apache.texera.amber.core.state.StateReferencing.{SIDECAR_PROPERTY, textReferences}
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{PhysicalOp, PortIdentity, SchemaPropagationFunc}
import org.apache.texera.amber.operator.PythonOperatorDescriptor._
import org.apache.texera.amber.pybuilder.PythonLexerUtils.{
  hasCommentOutsideQuotes,
  hasUnclosedQuote,
  isIdentChar,
  updateTripleQuotedStringState
}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.decoderExpression
import org.apache.texera.amber.util.JSONUtils.objectMapper

import scala.annotation.tailrec
import scala.util.{Success, Try}

trait PythonOperatorDescriptor extends LogicalOp {
  private def generatePythonCodeForRaisingException(ex: Throwable): String = {
    s"#EXCEPTION DURING CODE GENERATION: ${ex.getMessage}"
  }

  /**
    * `code` with every loop variable in `names`, each one that a String property of this
    * descriptor refers to, read from the iteration's state. The code is generated before the loop
    * runs, from the literal `$name` the property holds; pyb renders that text as a decode
    * expression the operator evaluates when it runs, which becomes `loopVariableLookup(name)`
    * instead.
    */
  private def withLoopVariableLookups(code: String, names: Iterable[String]): String =
    names.toSet.foldLeft(code) { (rewritten, name) =>
      rewritten.replace(decoderExpression("$" + name), loopVariableLookup(name))
    }

  /** This descriptor's code, rewritten by `rewrite`, or the code that says why it failed. */
  private def generatedCode(rewrite: String => String): String =
    try {
      rewrite(generatePythonCode())
    } catch {
      case ex: Throwable =>
        // instead of throwing error directly, we embed the error in the code
        // this can let upper-level compiler catch the error without interrupting the schema propagation
        generatePythonCodeForRaisingException(ex)
    }

  /**
    * This descriptor's code with every loop variable its properties refer to read from the
    * iteration's state, and each numeric or boolean reference that the code cannot read so: its
    * pointer -> why. It generates the code anew on every call. Outside every loop block the
    * sidecar is empty, and the code is exactly the one `generatePythonCode` returns. When no
    * probe value generates the code, the code is why the generation failed, for the compiler to
    * report, and no reference is said to be unbound.
    */
  def loopVariableBinding: LoopVariableBinding = bindLoopVariables().binding

  /**
    * The binding, and the output schemas of the descriptor its code was generated from.
    *
    * The code is generated before the loop runs, so a numeric or boolean property's value is
    * written into it, and the property holds a placeholder (0 / 0.0 / false) there. To find where
    * the value lands, the code is generated from copies of this descriptor: the baseline sets each
    * such reference to the first value of its probe pair, and one variant per reference sets it to
    * the second. Where the two codes differ by the two values as Python writes them, and by
    * nothing else, the baseline holds the value, and reads the variable instead
    * (`loopVariableValueLookup`), unless it lies in a text, a comment or a name, or the descriptor
    * checks or adjusts the value: a copy with an edge value or another probe in its place must
    * generate, holding that value in the very same places. When the descriptor rejects a probe,
    * the next pair is tried, for the other references' sake; when no baseline generates, whether
    * the failure is the values' cannot be told, and the code says why the first one failed. The
    * schema is the baseline's.
    */
  private def bindLoopVariables(): Bound =
    if (stateReferences.isEmpty) {
      Bound(LoopVariableBinding(generatedCode(identity), Map.empty), getOutputSchemas)
    } else {
      val tree = copyMapper.valueToTree[ObjectNode](this)
      tree.remove(SIDECAR_PROPERTY) // so that a copy refers to no loop variable
      val text = textReferences(tree, stateReferences)
      val typed = stateReferences -- text.keys
      val kinds = typed.flatMap { case (pointer, _) => kindOf(tree.at(pointer)).map(pointer -> _) }
      val unknown = (typed -- kinds.keys).map { case (pointer, _) => pointer -> generatedFromValue }
      val textLookups = withLoopVariableLookups(_: String, text.values)
      if (kinds.isEmpty) {
        Bound(LoopVariableBinding(generatedCode(textLookups), unknown), getOutputSchemas)
      } else {
        probing(tree, kinds).fold(
          failure =>
            Bound(
              LoopVariableBinding(generatePythonCodeForRaisingException(failure), unknown),
              getOutputSchemas
            ),
          probed => probedBinding(probed, kinds, unknown, textLookups)
        )
      }
    }

  /**
    * The binding of the references in `kinds` (pointer -> kind) that `probed` shows, with the
    * `unknown` ones' reasons and the text lookups on top, and the baseline's output schemas.
    */
  private def probedBinding(
      probed: Probing,
      kinds: Map[String, String],
      unknown: Map[String, String],
      textLookups: String => String
  ): Bound = {
    val found = probed.variants.map {
      case (pointer, variant) =>
        val (first, second) = probed.pairs(pointer)
        val width = first.rendered.length
        def sitesWith(value: Probe, code: String) =
          sitesOf(probed.code, code, first.rendered, value.rendered)
        // Generated with `value` in the reference's place, the code holds it where the sites are.
        def landsAsIs(sites: List[Int])(value: Probe): Boolean =
          generatedCopy(withValue(probed.tree, pointer, value.node)).toOption.exists {
            case (_, code) => sitesWith(value, code).contains(sites)
          }
        pointer -> variant
          .toRight(checkedValue)
          .flatMap { case (_, code) => sitesWith(second, code).toRight(generatedFromValue) }
          .flatMap {
            case Nil => Left(unreadValue)
            case sites if sites.exists(insideTextOrName(probed.code, _, width)) =>
              Left(valueInText)
            case sites if !checkValues(kinds(pointer), (first, second)).forall(landsAsIs(sites)) =>
              Left(checkedValue)
            case sites => Right(sites.map(site => (site, site + width)))
          }
    }
    val spans = found.collect { case (pointer, Right(sites)) => pointer -> sites }
    // Two references' values never land in one place; if they seem to, neither is bound.
    val overlapping = (for {
      (pointer, own) <- spans
      (other, theirs) <- spans
      if pointer != other && own.exists {
        case (start, end) => theirs.exists { case (s, e) => start < e && s < end }
      }
    } yield pointer).toSet
    val code = (spans -- overlapping).toSeq
      .flatMap { case (pointer, sites) => sites.map(pointer -> _) }
      .sortBy { case (_, (start, _)) => -start }
      .foldLeft(probed.code) {
        case (rewritten, (pointer, (start, end))) =>
          val lookup = loopVariableValueLookup(stateReferences(pointer), kinds(pointer))
          rewritten.patch(start, lookup, end - start)
      }
    val unbound = unknown ++ found.collect { case (pointer, Left(why)) => pointer -> why } ++
      overlapping.map(_ -> generatedFromValue)
    val variants = probed.variants.collect {
      case (pointer, Some((copy, _))) => pointer -> copy
    }
    Bound(
      LoopVariableBinding(textLookups(code), unbound),
      inputSchemas => {
        val schemas = probed.baseline.getOutputSchemas(inputSchemas)
        val deciding = variants.collect {
          case (pointer, variant)
              if !Try(variant.getOutputSchemas(inputSchemas)).toOption.contains(schemas) =>
            pointer
        }
        if (deciding.nonEmpty) {
          throw new UnsupportedOperationException(
            s"${operatorInfo.userFriendlyName} cannot refer to loop variables in " +
              s"${deciding.toSeq.sorted.mkString(", ")} yet: $decidesOutputSchema"
          )
        }
        schemas
      }
    )
  }

  /**
    * The first probing of the references in `kinds` (pointer -> kind) whose baseline and variants
    * all generate code, else the first whose baseline does; else why the first baseline fails.
    */
  private def probing(tree: ObjectNode, kinds: Map[String, String]): Either[Throwable, Probing] = {
    val attempts = LazyList
      .range(0, kinds.values.map(ProbePairs(_).size).max)
      .map { attempt =>
        val pairs = kinds.map {
          case (pointer, kind) =>
            pointer -> ProbePairs(kind)(attempt.min(ProbePairs(kind).size - 1))
        }
        val baseline = tree.deepCopy()
        pairs.foreach { case (pointer, (first, _)) => replaceAt(baseline, pointer, first.node) }
        generatedCopy(baseline).map {
          case (copy, code) =>
            val variants = pairs.map {
              case (pointer, (_, second)) =>
                pointer -> generatedCopy(withValue(baseline, pointer, second.node)).toOption
            }
            Probing(baseline, copy, code, pairs, variants)
        }
      }
    attempts
      .collectFirst { case Success(probed) if probed.variants.values.forall(_.nonEmpty) => probed }
      .orElse(attempts.collectFirst { case Success(probed) => probed })
      .toRight(attempts.head.failed.get)
  }

  /** A descriptor like this one, parsed from `tree`, and the code it generates, if it does. */
  private def generatedCopy(tree: ObjectNode): Try[(PythonOperatorDescriptor, String)] =
    Try {
      val copy: PythonOperatorDescriptor = copyMapper.treeToValue(tree, getClass)
      copy -> copy.generatePythonCode()
    }

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    val bound = bindLoopVariables()
    val pythonCode = bound.binding.code
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
      .withPropagateSchema(SchemaPropagationFunc(bound.outputSchemas))
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

  /**
    * The call through which generated code reads loop variable `name` as a value of `kind`
    * (`integer`, `number` or `boolean`): pyamber's `Operator.loop_variable_value` answers it from
    * the iteration's state message, converting it as the JVM's `LateBoundExecutor` does.
    */
  def loopVariableValueLookup(name: String, kind: String): String =
    s"self.loop_variable_value('$name', '$kind')"

  /**
    * A descriptor's code with its loop variables read from the iteration's state, and each
    * numeric or boolean reference it cannot read so: its JSON pointer -> why, as the compiler's
    * error says it.
    */
  final case class LoopVariableBinding(code: String, unbound: Map[String, String])

  // Why a numeric or boolean reference is not bound, as the error says it.
  private val generatedFromValue = "its code is generated from the value before the loop runs"
  private val checkedValue = "it checks or adjusts the value before the loop runs"
  private val unreadValue = "its code does not read the value when the loop runs"
  private val valueInText = "its value lands inside a text or a comment of the generated code"
  private val decidesOutputSchema = "the value decides its output schema"

  /** A binding, and the output schemas of the descriptor its code was generated from. */
  private final case class Bound(
      binding: LoopVariableBinding,
      outputSchemas: Map[PortIdentity, Schema] => Map[PortIdentity, Schema]
  )

  /** A value a reference is set to in a copy, and the text Python code writes it as. */
  private final case class Probe(node: JsonNode, rendered: String)

  /**
    * The baseline copy's JSON, the copy and its code, each reference's probe pair, and each
    * reference's variant copy and its code, when it generates.
    */
  private final case class Probing(
      tree: ObjectNode,
      baseline: PythonOperatorDescriptor,
      code: String,
      pairs: Map[String, (Probe, Probe)],
      variants: Map[String, Option[(PythonOperatorDescriptor, String)]]
  )

  /**
    * `objectMapper`, but writing null properties as well: a copy parsed from a descriptor's JSON
    * keeps the nulls the descriptor holds, where the fields' defaults would take their place.
    */
  private lazy val copyMapper = objectMapper.copy().setSerializationInclusion(Include.ALWAYS)

  private val nodes = JsonNodeFactory.instance

  private def integer(value: Int) = Probe(nodes.numberNode(value), value.toString)
  private def number(value: Double) = Probe(nodes.numberNode(value), value.toString)
  private def boolean(value: Boolean) =
    Probe(nodes.booleanNode(value), if (value) "True" else "False")

  /** The probe pairs of each kind of reference, in the order they are tried. */
  private val ProbePairs: Map[String, Seq[(Probe, Probe)]] = Map(
    "integer" -> Seq(integer(3) -> integer(5), integer(70) -> integer(90)),
    "number" -> Seq(number(0.25) -> number(0.75), number(2.5) -> number(7.5)),
    "boolean" -> Seq(boolean(true) -> boolean(false))
  )

  /**
    * The values of each kind, beside the probes, where a descriptor's checks and clamps commonly
    * act: zero, one, minus one and a large one. The probes are every boolean already.
    */
  private val EdgeValues: Map[String, Seq[Probe]] = Map(
    "integer" -> Seq(0, 1, -1, 1000000).map(integer),
    "number" -> Seq(0.0, 1.0, -1.0, 1000000.0).map(number),
    "boolean" -> Seq.empty
  )

  /**
    * The values a reference of `kind`, probed with `pair`, must also land as, where the pair does:
    * the kind's edge values and its other probes. A check or a clamp of the descriptor that acts
    * on one of them would never see the iteration's value.
    */
  private def checkValues(kind: String, pair: (Probe, Probe)): Seq[Probe] =
    EdgeValues(kind) ++ ProbePairs(kind).filterNot(_ == pair).flatMap { case (a, b) => Seq(a, b) }

  /** The kind of reference a placeholder stands for: its JSON type decides, as it does at bind. */
  private def kindOf(placeholder: JsonNode): Option[String] =
    if (placeholder.isIntegralNumber) Some("integer")
    else if (placeholder.isNumber) Some("number")
    else if (placeholder.isBoolean) Some("boolean")
    else None

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

  /**
    * The offsets at which `baseline` holds `first` where `variant` holds `second`, when the two
    * differ there and nowhere else; None when they differ otherwise.
    */
  private def sitesOf(
      baseline: String,
      variant: String,
      first: String,
      second: String
  ): Option[List[Int]] = {
    // A mismatch lies this far into the site: the two values' common prefix.
    val common = first.zip(second).takeWhile { case (a, b) => a == b }.length
    @tailrec
    def walk(i: Int, j: Int, sites: List[Int]): Option[List[Int]] =
      if (i == baseline.length || j == variant.length) {
        Option.when(i == baseline.length && j == variant.length)(sites.reverse)
      } else if (baseline(i) == variant(j)) {
        walk(i + 1, j + 1, sites)
      } else {
        val (site, other) = (i - common, j - common)
        if (site >= 0 && baseline.startsWith(first, site) && variant.startsWith(second, other)) {
          walk(site + first.length, other + second.length, site :: sites)
        } else {
          None
        }
      }
    walk(0, 0, Nil)
  }

  /**
    * Whether the `width` characters at `site` of `code` lie inside a string literal or a comment,
    * or next to a character of a name or a `.`, where a lookup would not be Python code of its
    * own: a part of a name, or of a number such as `3.0`.
    */
  private def insideTextOrName(code: String, site: Int, width: Int): Boolean = {
    val lineStart = code.lastIndexOf('\n', site - 1) + 1
    val line = code.substring(lineStart, site)
    val tripleQuoted = code
      .substring(0, lineStart)
      .split('\n')
      .foldLeft(Option.empty[String])((open, before) => updateTripleQuotedStringState(before, open))
    def glued(neighbour: Char) = isIdentChar(neighbour) || neighbour == '.'
    updateTripleQuotedStringState(line, tripleQuoted).nonEmpty ||
    hasUnclosedQuote(line) || hasCommentOutsideQuotes(line) ||
    (site > 0 && glued(code(site - 1))) ||
    (site + width < code.length && glued(code(site + width)))
  }
}
