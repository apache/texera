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
import org.apache.texera.amber.core.workflow.{
  PhysicalOp,
  PhysicalPlan,
  PortIdentity,
  SchemaPropagationFunc
}
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
    * iteration's state, and what tells which numeric or boolean references the code cannot read
    * so (see `LoopVariableBinding`). It generates the code anew on every call: a compile calls it
    * once, and builds the physical operator from the binding with
    * `getPhysicalPlan(workflowId, executionId, binding)`. Outside every loop block the sidecar is
    * empty, and the code is exactly the one `generatePythonCode` returns. When neither the probe
    * values nor the properties' defaults generate the code, the code is why the defaults'
    * generation failed, for the compiler to report, and no reference is said to be unbound.
    *
    * The code is generated before the loop runs, so a numeric or boolean property's value is
    * written into it, and the property holds a placeholder (0 / 0.0 / false) there. To find where
    * the value lands, the code is generated from copies of this descriptor: the baseline sets each
    * such reference to the first value of its probe pair, and one variant per reference sets it to
    * the second. Where the two codes differ by the two values as Python writes them, and by
    * nothing else, the baseline holds the value, and reads the variable instead
    * (`loopVariableValueLookup`, as a `long` where the property takes a whole number past 32
    * bits), unless it lies in a text, a comment or a name, the descriptor checks or adjusts the
    * value, or the code reads it when the operator starts (`runsAtStartup`). To pass the check, a
    * copy with an edge value or another probe in its place must generate, holding that value in
    * the very same places. When the descriptor rejects a probe, the next pair is tried, for the
    * other references' sake. When it rejects every pair's baseline, the baseline sets each
    * reference to its property's default instead, a value the descriptor's author made it take,
    * and the probes are values to check like the edge ones; when that baseline fails too, the
    * failure is the descriptor's own, and the code says why, naming no probe value where a
    * property has a default. The schema is the baseline's.
    */
  def loopVariableBinding: LoopVariableBinding =
    if (stateReferences.isEmpty) {
      unprobed(generatedCode(identity), Map.empty)
    } else {
      val tree = copyMapper.valueToTree[ObjectNode](this)
      tree.remove(SIDECAR_PROPERTY) // so that a copy refers to no loop variable
      val text = textReferences(tree, stateReferences)
      val typed = stateReferences -- text.keys
      val kinds = typed.flatMap { case (pointer, _) => kindOf(tree.at(pointer)).map(pointer -> _) }
      val unknown = (typed -- kinds.keys).map { case (pointer, _) => pointer -> generatedFromValue }
      val textLookups = withLoopVariableLookups(_: String, text.values)
      if (kinds.isEmpty) {
        unprobed(generatedCode(textLookups), unknown)
      } else {
        probing(tree, kinds).fold(
          failure => unprobed(generatePythonCodeForRaisingException(failure), unknown),
          probed => probedBinding(probed, kinds, unknown, textLookups)
        )
      }
    }

  /** The binding of `code`, generated from this descriptor itself: its schemas are this one's. */
  private def unprobed(code: String, unbound: Map[String, String]): LoopVariableBinding =
    new LoopVariableBinding(
      operatorInfo.userFriendlyName,
      code,
      unbound,
      getOutputSchemas,
      Map.empty
    )

  /**
    * The binding of the references in `kinds` (pointer -> kind) that `probed` shows, with the
    * `unknown` ones' reasons and the text lookups on top: its schemas are the baseline's, and each
    * variant's tell whether that reference's value decides them.
    */
  private def probedBinding(
      probed: Probing,
      kinds: Map[String, String],
      unknown: Map[String, String],
      textLookups: String => String
  ): LoopVariableBinding = {
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
            case sites if sites.exists(runsAtStartup(probed.code, _)) =>
              Left(valueReadAtStartup)
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
          val lookup = loopVariableValueLookup(
            stateReferences(pointer),
            readKind(probed.tree, pointer, kinds(pointer))
          )
          rewritten.patch(start, lookup, end - start)
      }
    val unbound = unknown ++ found.collect { case (pointer, Left(why)) => pointer -> why } ++
      overlapping.map(_ -> generatedFromValue)
    new LoopVariableBinding(
      operatorInfo.userFriendlyName,
      textLookups(code),
      unbound,
      probed.baseline.getOutputSchemas,
      probed.variants.collect { case (pointer, Some((copy, _))) => pointer -> copy }
    )
  }

  /**
    * The first probing of the references in `kinds` (pointer -> kind) whose baseline and variants
    * all generate code, else the first whose baseline does. When no probe pair's baseline
    * generates, the probing from the properties' defaults (`defaultPairs`), else why its baseline
    * fails: the descriptor's own values fail it, not a probe value.
    */
  private def probing(tree: ObjectNode, kinds: Map[String, String]): Either[Throwable, Probing] = {
    val attempts = LazyList
      .range(0, kinds.values.map(ProbePairs(_).size).max)
      .map { attempt =>
        probingWith(
          tree,
          kinds.map {
            case (pointer, kind) =>
              pointer -> ProbePairs(kind)(attempt.min(ProbePairs(kind).size - 1))
          }
        )
      }
    attempts
      .collectFirst { case Success(probed) if probed.variants.values.forall(_.nonEmpty) => probed }
      .orElse(attempts.collectFirst { case Success(probed) => probed })
      .map(Right(_))
      .getOrElse(probingWith(tree, defaultPairs(tree, kinds)).toEither)
  }

  /**
    * The probing of the references in `pairs` (pointer -> probe pair): the baseline, which sets each
    * one to the first value of its pair, and its code, if it generates.
    */
  private def probingWith(tree: ObjectNode, pairs: Map[String, (Probe, Probe)]): Try[Probing] = {
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

  /**
    * The pair each reference in `kinds` (pointer -> kind) is probed with when no probe pair
    * generates the code: the value this descriptor's class gives the property, one its author made
    * it take, and the value next to it. A reference whose property has no such value -- an element
    * of a list, or a property with none by default -- keeps its first probe pair.
    */
  private def defaultPairs(
      tree: ObjectNode,
      kinds: Map[String, String]
  ): Map[String, (Probe, Probe)] = {
    // Parsed without the properties, a copy holds their defaults.
    val bare = tree.deepCopy()
    val removed = kinds.keys.filter(removeAt(bare, _)).toSet
    val defaults = Try(copyMapper.valueToTree[ObjectNode](copyMapper.treeToValue(bare, getClass)))
    kinds.map {
      case (pointer, kind) =>
        val default = defaults.toOption.filter(_ => removed(pointer)).map(_.at(pointer))
        pointer -> default.flatMap(pairAround(kind, _)).getOrElse(ProbePairs(kind).head)
    }
  }

  /**
    * The kind generated code reads a reference of `kind` at `pointer` as: an integer property that
    * takes a whole number past 32 bits, a long, is read as a `long`; an int one stays an
    * `integer`, which Jackson refuses such a number for, as it binds a JVM operator's property.
    */
  private def readKind(tree: ObjectNode, pointer: String, kind: String): String =
    if (
      kind == "integer" &&
      Try(copyMapper.treeToValue(withValue(tree, pointer, PastInt), getClass)).isSuccess
    ) {
      "long"
    } else {
      kind
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
  ): PhysicalOp = getPhysicalOp(workflowId, executionId, loopVariableBinding)

  // Final, so that the plan the compiler builds from a binding is the one this builds.
  final override def getPhysicalPlan(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalPlan = getPhysicalPlan(workflowId, executionId, loopVariableBinding)

  /**
    * The physical plan that runs `binding`'s code, this descriptor's `loopVariableBinding`: the
    * compiler generates the code once, and tells the references the code leaves unbound from the
    * same binding.
    */
  def getPhysicalPlan(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      binding: LoopVariableBinding
  ): PhysicalPlan =
    PhysicalPlan(
      operators = Set(getPhysicalOp(workflowId, executionId, binding)),
      links = Set.empty
    )

  /** The physical operator that runs `binding`'s code, this descriptor's `loopVariableBinding`. */
  def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      binding: LoopVariableBinding
  ): PhysicalOp = {
    val pythonCode = binding.code
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
      .withPropagateSchema(SchemaPropagationFunc(binding.outputSchemas(_)))
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

  /**
    * The call through which generated code reads loop variable `name` as a value of `kind`
    * (`integer` for an int property, `long`, `number` or `boolean`): pyamber's
    * `Operator.loop_variable_value` answers it from the iteration's state message, converting it
    * as the JVM's `StateReferenceBinding` does, and Jackson as it writes the value.
    */
  def loopVariableValueLookup(name: String, kind: String): String =
    s"self.loop_variable_value('$name', '$kind')"

  /**
    * A descriptor's code with its loop variables read from the iteration's state, generated once,
    * and what tells which numeric or boolean references it cannot read so, each one's JSON
    * pointer -> why, as the compiler's error says it: `unbound`, found from the code, and
    * `unboundBySchema`, given the input schemas.
    *
    * @param operator the descriptor's name, as the errors say it.
    * @param baselineSchemas the output schemas of the copy the code was generated from.
    * @param variants each numeric or boolean reference's variant copy, when it generates.
    */
  final class LoopVariableBinding private[operator] (
      operator: String,
      val code: String,
      val unbound: Map[String, String],
      baselineSchemas: Map[PortIdentity, Schema] => Map[PortIdentity, Schema],
      variants: Map[String, PythonOperatorDescriptor]
  ) {

    /**
      * Each reference whose value decides the output schemas, given `inputSchemas`: pointer ->
      * why. The schemas are computed before the loop runs, from the probe in its place.
      */
    def unboundBySchema(inputSchemas: Map[PortIdentity, Schema]): Map[String, String] =
      Try(baselineSchemas(inputSchemas)).toOption.fold(Map.empty[String, String]) { schemas =>
        variants.collect {
          case (pointer, variant)
              if !Try(variant.getOutputSchemas(inputSchemas)).toOption.contains(schemas) =>
            pointer -> decidesOutputSchema
        }
      }

    /** The output schemas, given `inputSchemas`: none while a reference's value decides them. */
    def outputSchemas(inputSchemas: Map[PortIdentity, Schema]): Map[PortIdentity, Schema] = {
      val schemas = baselineSchemas(inputSchemas)
      val deciding = unboundBySchema(inputSchemas)
      if (deciding.nonEmpty) {
        throw new UnsupportedOperationException(
          s"$operator cannot refer to loop variables in " +
            s"${deciding.keys.toSeq.sorted.mkString(", ")} yet: $decidesOutputSchema"
        )
      }
      schemas
    }
  }

  // Why a numeric or boolean reference is not bound, as the error says it.
  private val generatedFromValue = "its code is generated from the value before the loop runs"
  private val checkedValue = "it checks or adjusts the value before the loop runs"
  private val unreadValue = "its code does not read the value when the loop runs"
  private val valueInText = "its value lands inside a text or a comment of the generated code"
  private val decidesOutputSchema = "the value decides its output schema"
  private val valueReadAtStartup =
    "its code reads the value when the operator starts, before the iteration's state arrives"

  /** The least whole number an int property cannot hold. */
  private val PastInt: JsonNode = JsonNodeFactory.instance.numberNode(Int.MaxValue.toLong + 1)

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

  /**
    * The pair a reference of `kind` is probed with from its property's default `value`: the value
    * and the one next to it (one more, a half more for a number, the other boolean), if `value`
    * is of that kind.
    */
  private def pairAround(kind: String, value: JsonNode): Option[(Probe, Probe)] =
    kind match {
      case "integer" if value.isIntegralNumber && value.canConvertToInt =>
        Some(integer(value.intValue) -> integer(value.intValue + 1))
      case "number" if value.isNumber =>
        Some(number(value.doubleValue) -> number(value.doubleValue + 0.5))
      case "boolean" if value.isBoolean =>
        Some(boolean(value.booleanValue) -> boolean(!value.booleanValue))
      case _ => None
    }

  /** Removes the property at `pointer` from `tree`, if it is an object's: whether it did. */
  private def removeAt(tree: ObjectNode, pointer: String): Boolean = {
    val path = JsonPointer.compile(pointer)
    tree.at(path.head) match {
      case parent: ObjectNode => parent.remove(path.last.getMatchingProperty) != null
      case _                  => false
    }
  }

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
