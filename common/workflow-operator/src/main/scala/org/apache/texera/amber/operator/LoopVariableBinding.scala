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
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode, TextNode}
import org.apache.texera.amber.core.state.StateReferencing.{SIDECAR_PROPERTY, textReferences}
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.LoopVariableBinding._
import org.apache.texera.amber.operator.PythonOperatorDescriptor.{
  loopVariableLookup,
  loopVariableValueLookup
}
import org.apache.texera.amber.operator.metadata.PropertyNameConstants
import org.apache.texera.amber.pybuilder.PythonLexerUtils.{
  hasCommentOutsideQuotes,
  hasUnclosedQuote,
  isIdentChar,
  updateTripleQuotedStringState
}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.decoderExpression
import org.apache.texera.amber.util.JSONUtils.objectMapper

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.{Success, Try}

/**
  * A generated operator's code with its loop variables read from the iteration's state, generated
  * once (`LoopVariableBinding.of`), and why each reference the code cannot read so is not bound,
  * each one's JSON pointer -> why, as the compiler's error says it: a numeric or boolean
  * reference's from the code (`unbound`) and given the input schemas (`unboundBySchema`), and
  * every reference's, a text's too, in `reasons`, which the compiler reports.
  *
  * @param operator the descriptor's name, as the errors say it.
  * @param outputPorts the descriptor's output ports, which its physical operator has.
  * @param texts the descriptor's text references: a String property's pointer -> name.
  * @param baseline the copy the code was generated from: the descriptor itself, or the probing's
  *                 baseline, which holds a probe value, or the property's default, in each
  *                 numeric or boolean reference's place.
  * @param variants each numeric or boolean reference's variant copy, when it generates.
  */
final class LoopVariableBinding private (
    operator: String,
    outputPorts: Set[PortIdentity],
    val code: String,
    val unbound: Map[String, String],
    texts: Map[String, String],
    baseline: PythonOperatorDescriptor,
    variants: Map[String, PythonOperatorDescriptor]
) {

  /**
    * Each reference whose value decides the output schemas, given `inputSchemas`: pointer ->
    * why. The schemas are computed before the loop runs, from the probe in its place.
    */
  private def unboundBySchema(inputSchemas: Map[PortIdentity, Schema]): Map[String, String] =
    decidedBy(inputSchemas, Try(baseline.getOutputSchemas(inputSchemas)).toOption)

  /**
    * Each reference whose variant's output schemas, given `inputSchemas`, differ from
    * `baselineSchemas`, the baseline's (none when they fail): pointer -> why.
    */
  private def decidedBy(
      inputSchemas: Map[PortIdentity, Schema],
      baselineSchemas: Option[Map[PortIdentity, Schema]]
  ): Map[String, String] =
    baselineSchemas.fold(Map.empty[String, String]) { schemas =>
      variants.collect {
        case (pointer, variant)
            if !Try(variant.getOutputSchemas(inputSchemas)).toOption.contains(schemas) =>
          pointer -> decidesOutputSchema("value")
      }
    }

  /** The output schemas, given `inputSchemas`: none while a reference's value decides them. */
  private[operator] def outputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val schemas = baseline.getOutputSchemas(inputSchemas)
    val deciding = unboundBySchema(inputSchemas)
    if (deciding.nonEmpty) {
      throw new UnsupportedOperationException(
        s"$operator cannot refer to loop variables in " +
          s"${deciding.keys.toSeq.sorted.mkString(", ")} yet: ${decidesOutputSchema("value")}"
      )
    }
    schemas
  }

  /**
    * Why each reference is not bound, given the input schemas when every input has one, as the
    * schema propagation computes the output schemas only then: the references with each reason,
    * in the order the compiler's error gives them. First each numeric or boolean reason, in the
    * order of its first pointer: from the code (`unbound`), or given the input schemas
    * (`unboundBySchema`). Then each text reason, in a fixed order.
    *
    * A String property's `$name` is bound only where pyb's decoder rendered it, which `of` turned
    * into a read of the loop state, `loopVariableLookup(name)`, and then only if the code holds
    * the text `$name` nowhere (where it does, the code pastes it in as it is), the code reads it
    * nowhere when the operator starts (`readsAtStartup`), before the iteration's state can
    * arrive, and no output column the operator adds holds the text (its schema is computed from
    * the literal). Nor may a copy of the operator with a column of an input or another of its
    * texts in the reference's place show it checking the text, or computing its output schemas
    * from it, both of which it does from the literal too (`textsReadBeforeTheLoop`, asked only
    * once every input has a schema). The texts are judged once every numeric or boolean
    * reference is bound: until then the code holds a probe value in its place, which may decide
    * what the code reads. When the generation failed there is no code to read either way, and
    * the compiler's code-generation check reports why it failed instead.
    */
  def reasons(inputSchemas: Option[Map[PortIdentity, Schema]]): Seq[(Iterable[String], String)] = {
    // The baseline's output schemas, computed once. The texts are judged only when no variant
    // decides them, so there they are also the operator's `outputSchemas`, which the columns it
    // adds are read from.
    val baselineSchemas =
      inputSchemas.flatMap(inputs => Try(baseline.getOutputSchemas(inputs)).toOption)
    val bySchema = inputSchemas.fold(Map.empty[String, String])(decidedBy(_, baselineSchemas))
    val typedUnbound = unbound ++ (bySchema -- unbound.keys)
    val textReasons: Map[String, String] =
      if (typedUnbound.nonEmpty || code.contains(FailedGeneration)) {
        Map.empty
      } else {
        lazy val added = addedColumns(inputSchemas, baselineSchemas)
        def whyUnbound(name: String): Option[String] = {
          val lookup = loopVariableLookup(name)
          if (code.contains("$" + name)) Some(pastedIntoCode)
          else if (!code.contains(lookup)) Some(unread("text"))
          else if (readsAtStartup(code, offsetsOf(code, lookup))) Some(readAtStartup("text"))
          else if (added.exists(_.contains("$" + name))) Some(namesOutputColumn)
          else None
        }
        val inCode = texts.flatMap {
          case (pointer, name) => whyUnbound(name).map(pointer -> _)
        }
        inCode ++ inputSchemas.fold(Map.empty[String, String])(
          textsReadBeforeTheLoop(texts -- inCode.keys, _)
        )
      }
    val typedReasons = typedUnbound
      .groupMap(_._2)(_._1)
      .toSeq
      .sortBy { case (_, pointers) => pointers.min }
      .map(_.swap)
    val all = typedReasons ++ Seq(
      pastedIntoCode,
      unread("text"),
      readAtStartup("text"),
      namesOutputColumn,
      checkedText,
      decidesOutputSchema("text")
    ).map { why =>
      textReasons.collect { case (pointer, `why`) => pointer } -> why
    }
    all.filter { case (pointers, _) => pointers.nonEmpty }
  }

  /**
    * The columns the operator adds, given `inputSchemas` and its output schemas `outputs`: on an
    * output port of its, but on no input port. Its output schemas are computed only once every
    * input has a schema, and none while they fail.
    */
  private def addedColumns(
      inputSchemas: Option[Map[PortIdentity, Schema]],
      outputs: Option[Map[PortIdentity, Schema]]
  ): Set[String] = {
    def columns(schemas: Iterable[(PortIdentity, Schema)]): Set[String] =
      schemas
        .collect { case (port, schema) if !port.internal => schema.getAttributeNames }
        .flatten
        .toSet
    inputSchemas.fold(Set.empty[String]) { inputs =>
      outputs.fold(Set.empty[String]) { outputs =>
        columns(outputs.filter { case (port, _) => outputPorts(port) }) -- columns(inputs)
      }
    }
  }

  /**
    * Each of `texts` (pointer -> name: a String property holding `$name`) that the copy the code
    * was generated from checks, or computes its output schemas from, given `inputSchemas`:
    * pointer -> why, as the compiler's error says it. Both happen before the loop runs, from the
    * literal `$name`, so neither sees the iteration's value. That copy holds the probe values the
    * code and the schemas are computed with, where the descriptor holds placeholders, which a
    * check may reject whatever the text is.
    *
    * A copy of it with another text in the reference's place shows it: a column of each type on
    * each input, or another non-empty text among its settings, which a check commonly compares the
    * text to. Where only one of that copy and the copy it is compared with computes its output
    * schemas, or that copy fails to generate its code where the other does not, the descriptor
    * checks the text; where both compute them and they differ, the text decides them. The copy
    * compared with holds every reference's `$name`, unless its schemas fail: then it is the first
    * copy that computes them with such texts in the references' places, the same text in each or
    * each the next one, so that a reference that fails a check cannot hide another's. When no copy
    * can be made, nothing tells.
    *
    * A check against anything else goes unseen -- the empty text, or a constant the settings hold
    * nowhere -- and so does a read that shapes the code without failing it, such as a branch on a
    * sentinel or an order of columns: the copies' code counts only when it fails to generate.
    */
  private def textsReadBeforeTheLoop(
      texts: Map[String, String],
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[String, String] =
    if (texts.isEmpty) {
      Map.empty
    } else {
      val tree = copyMapper.valueToTree[ObjectNode](baseline)
      tree.remove(SIDECAR_PROPERTY) // so that a copy refers to no loop variable
      val values = (columnPerType(inputSchemas) ++ settingTexts(tree)).toSet - ""
      val literals = texts.map { case (pointer, name) => pointer -> ("$" + name) }
      def copyWith(assignment: Map[String, String]): Option[PythonOperatorDescriptor] =
        Try {
          val json = assignment.foldLeft(tree) {
            case (copy, (pointer, text)) => withValue(copy, pointer, TextNode.valueOf(text))
          }
          copyMapper.treeToValue(json, baseline.getClass)
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
                Some(pointer -> decidesOutputSchema("text"))
              } else {
                None
              }
          }
      }
    }
}

object LoopVariableBinding {

  /**
    * `descriptor`'s code with every loop variable its properties refer to read from the
    * iteration's state, and why each reference the code cannot read so is not bound. It generates
    * the code anew on every call: a compile calls it once, and builds the physical operator from
    * the binding with `getPhysicalPlan(workflowId, executionId, binding)`. With an empty sidecar,
    * as every operator outside a loop block has in a workflow that compiles, the code is exactly
    * the one `generatePythonCode` returns.
    * When neither the probe values nor the properties' defaults generate the code, the code is why
    * the defaults' generation failed, for the compiler to report, and no reference it probed is
    * said to be unbound.
    *
    * The code is generated before the loop runs, so a numeric or boolean property's value is
    * written into it, and the property holds a placeholder (0 / 0.0 / false) there. To find where
    * the value lands, the code is generated from copies of the descriptor: the baseline sets each
    * such reference to the first value of its probe pair, and one variant per reference sets it to
    * the second. Where the two codes differ by the two values as Python writes them, and by
    * nothing else, the baseline holds the value, and reads the variable instead
    * (`loopVariableValueLookup`, as a `long` where the property takes a whole number past 32
    * bits), unless it lies in a text, a comment or a name, the descriptor checks or adjusts the
    * value, or the code reads it when the operator starts (`readsAtStartup`). To pass the check, a
    * copy with an edge value or another probe in its place must generate, holding that value in
    * the very same places. When the descriptor rejects a probe, the next pair is tried, for the
    * other references' sake. When it rejects every pair's baseline, the baseline sets each
    * reference to its property's default instead, a value the descriptor's author made it take,
    * and the probes are values to check like the edge ones; when that baseline fails too, the
    * failure is the descriptor's own, and the code says why, naming no probe value where a
    * property has a default. The schema is the baseline's, and so is the check of the texts
    * (`reasons`): a placeholder may fail a check whatever the text is.
    *
    * A String property's `$name` is rendered by pyb as a decode expression the operator evaluates
    * when it runs, which becomes `loopVariableLookup(name)` instead.
    */
  private[operator] def of(descriptor: PythonOperatorDescriptor): LoopVariableBinding = {
    val stateReferences = descriptor.stateReferences
    if (stateReferences.isEmpty) {
      unprobed(descriptor, generatedCode(descriptor, identity), Map.empty, Map.empty)
    } else {
      val tree = copyMapper.valueToTree[ObjectNode](descriptor)
      tree.remove(SIDECAR_PROPERTY) // so that a copy refers to no loop variable
      val text = textReferences(tree, stateReferences)
      val typed = stateReferences -- text.keys
      val kinds = typed.flatMap { case (pointer, _) => kindOf(tree.at(pointer)).map(pointer -> _) }
      val unknown = (typed -- kinds.keys).map { case (pointer, _) => pointer -> generatedFromValue }
      if (kinds.isEmpty) {
        unprobed(
          descriptor,
          generatedCode(descriptor, withLoopVariableLookups(_, text.values)),
          unknown,
          text
        )
      } else {
        probing(descriptor, tree, kinds).fold(
          failure =>
            unprobed(descriptor, generatePythonCodeForRaisingException(failure), unknown, text),
          probed => probedBinding(descriptor, probed, kinds, unknown, text)
        )
      }
    }
  }

  /** The start of the code that says why generating a descriptor's code failed. */
  private val FailedGeneration = "#EXCEPTION DURING CODE GENERATION:"

  private def generatePythonCodeForRaisingException(ex: Throwable): String = {
    s"$FailedGeneration ${ex.getMessage}"
  }

  /**
    * `code` with every loop variable in `names`, each one that a String property of the descriptor
    * refers to, read from the iteration's state. The code is generated before the loop runs, from
    * the literal `$name` the property holds; pyb renders that text as a decode expression the
    * operator evaluates when it runs, which becomes `loopVariableLookup(name)` instead.
    */
  private def withLoopVariableLookups(code: String, names: Iterable[String]): String =
    names.toSet.foldLeft(code) { (rewritten, name) =>
      rewritten.replace(decoderExpression("$" + name), loopVariableLookup(name))
    }

  /** `descriptor`'s code, rewritten by `rewrite`, or the code that says why it failed. */
  private def generatedCode(
      descriptor: PythonOperatorDescriptor,
      rewrite: String => String
  ): String =
    try {
      rewrite(descriptor.generatePythonCode())
    } catch {
      case ex: Throwable =>
        // instead of throwing error directly, we embed the error in the code
        // this can let upper-level compiler catch the error without interrupting the schema propagation
        generatePythonCodeForRaisingException(ex)
    }

  /**
    * The binding of `code`, generated from `descriptor` itself: its schemas are the descriptor's,
    * and so are the texts it reads before the loop runs.
    */
  private def unprobed(
      descriptor: PythonOperatorDescriptor,
      code: String,
      unbound: Map[String, String],
      texts: Map[String, String]
  ): LoopVariableBinding =
    new LoopVariableBinding(
      descriptor.operatorInfo.userFriendlyName,
      outputPortsOf(descriptor),
      code,
      unbound,
      texts,
      descriptor,
      Map.empty
    )

  /** The ports `descriptor`'s physical operator outputs on. */
  private def outputPortsOf(descriptor: PythonOperatorDescriptor): Set[PortIdentity] =
    descriptor.operatorInfo.outputPorts.map(_.id).toSet

  /**
    * The binding of the references in `kinds` (pointer -> kind) that `probed` shows, with the
    * `unknown` ones' reasons and the lookups of the `texts` (pointer -> name) on top: its schemas,
    * and the texts it reads before the loop runs, are the baseline's, and each variant's tell
    * whether that reference's value decides them.
    */
  private def probedBinding(
      descriptor: PythonOperatorDescriptor,
      probed: Probing,
      kinds: Map[String, String],
      unknown: Map[String, String],
      texts: Map[String, String]
  ): LoopVariableBinding = {
    val found = probed.variants.map {
      case (pointer, variant) =>
        val (first, second) = probed.pairs(pointer)
        val width = first.rendered.length
        def sitesWith(value: Probe, code: String) =
          sitesOf(probed.code, code, first.rendered, value.rendered)
        // Generated with `value` in the reference's place, the code holds it where the sites are.
        def landsAsIs(sites: List[Int])(value: Probe): Boolean =
          generatedCopy(descriptor, withValue(probed.tree, pointer, value.node)).toOption.exists {
            case (_, code) => sitesWith(value, code).contains(sites)
          }
        pointer -> variant
          .toRight(checkedValue)
          .flatMap { case (_, code) => sitesWith(second, code).toRight(generatedFromValue) }
          .flatMap {
            case Nil => Left(unread("value"))
            case sites if sites.exists(insideTextOrName(probed.code, _, width)) =>
              Left(valueInText)
            case sites if !checkValues(kinds(pointer), (first, second)).forall(landsAsIs(sites)) =>
              Left(checkedValue)
            case sites if readsAtStartup(probed.code, sites) =>
              Left(readAtStartup("value"))
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
            descriptor.stateReferences(pointer),
            readKind(descriptor, probed.tree, pointer, kinds(pointer))
          )
          rewritten.patch(start, lookup, end - start)
      }
    val unbound = unknown ++ found.collect { case (pointer, Left(why)) => pointer -> why } ++
      overlapping.map(_ -> generatedFromValue)
    new LoopVariableBinding(
      descriptor.operatorInfo.userFriendlyName,
      outputPortsOf(descriptor),
      withLoopVariableLookups(code, texts.values),
      unbound,
      texts,
      probed.baseline,
      probed.variants.collect { case (pointer, Some((copy, _))) => pointer -> copy }
    )
  }

  /**
    * The first probing of the references in `kinds` (pointer -> kind) whose baseline and variants
    * all generate code, else the first whose baseline does. When no probe pair's baseline
    * generates, the probing from the properties' defaults (`defaultPairs`), else why its baseline
    * fails: the descriptor's own values fail it, not a probe value.
    */
  private def probing(
      descriptor: PythonOperatorDescriptor,
      tree: ObjectNode,
      kinds: Map[String, String]
  ): Either[Throwable, Probing] = {
    val attempts = LazyList
      .range(0, kinds.values.map(ProbePairs(_).size).max)
      .map { attempt =>
        probingWith(
          descriptor,
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
      .getOrElse(probingWith(descriptor, tree, defaultPairs(descriptor, tree, kinds)).toEither)
  }

  /**
    * The probing of the references in `pairs` (pointer -> probe pair): the baseline, which sets each
    * one to the first value of its pair, and its code, if it generates.
    */
  private def probingWith(
      descriptor: PythonOperatorDescriptor,
      tree: ObjectNode,
      pairs: Map[String, (Probe, Probe)]
  ): Try[Probing] = {
    val baseline = tree.deepCopy()
    pairs.foreach { case (pointer, (first, _)) => replaceAt(baseline, pointer, first.node) }
    generatedCopy(descriptor, baseline).map {
      case (copy, code) =>
        val variants = pairs.map {
          case (pointer, (_, second)) =>
            pointer -> generatedCopy(descriptor, withValue(baseline, pointer, second.node)).toOption
        }
        Probing(baseline, copy, code, pairs, variants)
    }
  }

  /**
    * The pair each reference in `kinds` (pointer -> kind) is probed with when no probe pair
    * generates the code: the value the descriptor's class gives the property, one its author made
    * it take, and the value next to it. A reference whose property has no such value -- an element
    * of a list, or a property with none by default -- keeps its first probe pair.
    */
  private def defaultPairs(
      descriptor: PythonOperatorDescriptor,
      tree: ObjectNode,
      kinds: Map[String, String]
  ): Map[String, (Probe, Probe)] = {
    // Parsed without the properties, a copy holds their defaults.
    val bare = tree.deepCopy()
    val removed = kinds.keys.filter(removeAt(bare, _)).toSet
    val defaults =
      Try(copyMapper.valueToTree[ObjectNode](copyMapper.treeToValue(bare, descriptor.getClass)))
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
  private def readKind(
      descriptor: PythonOperatorDescriptor,
      tree: ObjectNode,
      pointer: String,
      kind: String
  ): String =
    if (
      kind == "integer" &&
      Try(copyMapper.treeToValue(withValue(tree, pointer, PastInt), descriptor.getClass)).isSuccess
    ) {
      "long"
    } else {
      kind
    }

  /** A descriptor like `descriptor`, parsed from `tree`, and the code it generates, if it does. */
  private def generatedCopy(
      descriptor: PythonOperatorDescriptor,
      tree: ObjectNode
  ): Try[(PythonOperatorDescriptor, String)] =
    Try {
      val copy: PythonOperatorDescriptor = copyMapper.treeToValue(tree, descriptor.getClass)
      copy -> copy.generatePythonCode()
    }

  /**
    * Whether generated `code` reads what it holds at any of `offsets` when the operator starts
    * (`runsAtStartup`), before the iteration's state can arrive: a numeric or boolean value where
    * it lands, or a text's lookup.
    */
  private def readsAtStartup(code: String, offsets: IterableOnce[Int]): Boolean =
    offsets.iterator.exists(runsAtStartup(code, _))

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
  private def runsAtStartup(code: String, offset: Int): Boolean = {
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

  /** Where `text` starts in `code`, each time it occurs. */
  private def offsetsOf(code: String, text: String): Iterator[Int] =
    Iterator.iterate(code.indexOf(text))(at => code.indexOf(text, at + 1)).takeWhile(_ >= 0)

  // Why a reference is not bound, as the compiler's error says it. The reasons shared by a text
  // and a numeric or boolean value take the noun: "text" or "value".
  private def unread(noun: String) = s"its code does not read the $noun when the loop runs"
  private def readAtStartup(noun: String) =
    s"its code reads the $noun when the operator starts, before the iteration's state arrives"
  private def decidesOutputSchema(noun: String) = s"the $noun decides its output schema"

  // Why a text reference is not bound.
  private val pastedIntoCode = "its code embeds the text before the loop runs"
  private val namesOutputColumn =
    "the text names an output column, which is set before the loop runs"
  private val checkedText = "it checks the text before the loop runs"

  // Why a numeric or boolean reference is not bound.
  private val generatedFromValue = "its code is generated from the value before the loop runs"
  private val checkedValue = "it checks or adjusts the value before the loop runs"
  private val valueInText = "its value lands inside a text or a comment of the generated code"

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
