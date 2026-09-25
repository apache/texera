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

package org.apache.texera.common.compiler

import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.protobuf.timestamp.Timestamp
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.apache.texera.common.compiler.WorkflowCompiler.{
  collectOutputSchemaFromPhysicalPlan,
  convertErrorListToWorkflowFatalErrorMap,
  failedCodeGeneration,
  normalizeStateReferences,
  unbindableStateReferences
}
import org.apache.texera.common.compiler.model.{LogicalLink, LogicalPlan, LogicalPlanPojo}
import org.apache.texera.amber.core.executor.OpExecWithCode
import org.apache.texera.amber.core.state.StateReferencing.{literalReferences, textReferences}
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow.{
  GlobalPortIdentity,
  PhysicalLink,
  PhysicalOp,
  PhysicalPlan,
  PortIdentity,
  WorkflowContext
}
import org.apache.texera.amber.core.workflowruntimestate.FatalErrorType.COMPILATION_ERROR
import org.apache.texera.amber.core.workflowruntimestate.WorkflowFatalError
import org.apache.texera.amber.operator.PythonOperatorDescriptor.loopVariableLookup
import org.apache.texera.amber.operator.loop.LoopStartOpDesc
import org.apache.texera.amber.operator.{LogicalOp, PythonOperatorDescriptor}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.amber.util.StackTraceUtils.getStackTraceWithAllCauses

import java.time.Instant
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.{Failure, Success, Try}

object WorkflowCompiler {
  // util function to convert the error list to an error map, and report the errors in the log
  private def convertErrorListToWorkflowFatalErrorMap(
      logger: Logger,
      errorList: List[(OperatorIdentity, Throwable)]
  ): Map[OperatorIdentity, WorkflowFatalError] = {
    val opIdToError = mutable.Map[OperatorIdentity, WorkflowFatalError]()
    errorList.foreach {
      case (opId, err) =>
        // map each error to WorkflowFatalError, and report them in the log
        logger.error(s"Error occurred in logical plan compilation for opId: $opId", err)
        // keep only the first error per operator: it is the root cause (e.g. a file
        // resolution failure), while later stages re-fail on the same operator with
        // less specific messages (e.g. schema propagation seeing an unresolved file)
        if (!opIdToError.contains(opId)) {
          opIdToError += (opId -> WorkflowFatalError(
            COMPILATION_ERROR,
            Timestamp(Instant.now),
            err.toString,
            getStackTraceWithAllCauses(err),
            opId.id
          ))
        }
    }
    opIdToError.toMap
  }

  /**
    * Make `logicalOp`'s `stateReferences` sidecar final, now that its place in the plan is known.
    * This runs before the operator's physical plan is built: the descriptor serializes the sidecar
    * into its executor's descString there, and the worker binds exactly what it names.
    *
    * Inside a loop block the sidecar also gets every whole-string `$name` value of the operator's
    * JSON (`StateReferencing.literalReferences`), next to the typed placeholders the parse put in;
    * an entry whose pointer names no value of that JSON has nothing to bind and is dropped. An
    * input fed from outside the block is then an error (`inputsAheadOfLoopState`).
    *
    * Outside every block a string such as "$AAPL" stays the literal it was before loop variables
    * existed, and the sidecar holds only typed placeholders (the user wrote "$n" for an Int),
    * which nothing would ever bind: each is an error naming the property and the variable. They
    * stay in the sidecar, so compiling the same operator again reports them again.
    */
  def normalizeStateReferences(
      logicalOp: LogicalOp,
      plan: LogicalPlan,
      insideLoopBlocks: Set[OperatorIdentity]
  ): Option[Throwable] =
    if (insideLoopBlocks.contains(logicalOp.operatorIdentifier)) {
      val tree = objectMapper.valueToTree[ObjectNode](logicalOp)
      logicalOp.stateReferences = (literalReferences(tree) ++ logicalOp.stateReferences).filter {
        case (pointer, _) => tree.at(pointer).isValueNode
      }
      inputsAheadOfLoopState(logicalOp, plan, insideLoopBlocks)
    } else {
      Option.when(logicalOp.stateReferences.nonEmpty) {
        val name = logicalOp.operatorInfo.userFriendlyName
        new IllegalArgumentException(
          logicalOp.stateReferences.toSeq.sorted
            .map {
              case (pointer, variable) =>
                s"property $pointer refers to loop variable $variable, " +
                  s"but $name is not inside a loop block"
            }
            .mkString("; ")
        )
      }
    }

  /**
    * An operator inside a loop block binds its references once the loop state has arrived (see
    * `LateBoundExecutor`), so a tuple that reaches it earlier cannot be processed. The state comes
    * ahead of the tuples on a link from a LoopStart or from another operator inside a block, but
    * not on one from outside every block. An input with such a link is an error, unless the
    * operator reads it only after an input whose links all carry the state (a join's probe input).
    */
  private def inputsAheadOfLoopState(
      logicalOp: LogicalOp,
      plan: LogicalPlan,
      insideLoopBlocks: Set[OperatorIdentity]
  ): Option[Throwable] =
    if (logicalOp.stateReferences.isEmpty) {
      None
    } else {
      val loopStarts =
        plan.operators.collect { case start: LoopStartOpDesc => start.operatorIdentifier }.toSet
      def carriesState(link: LogicalLink): Boolean =
        insideLoopBlocks.contains(link.fromOpId) || loopStarts.contains(link.fromOpId)
      val linksByPort = plan.getUpstreamLinks(logicalOp.operatorIdentifier).groupBy(_.toPortId)
      def carriesOnlyState(port: PortIdentity): Boolean =
        linksByPort.get(port).exists(_.forall(carriesState))
      val exposed = logicalOp.operatorInfo.inputPorts.filter { input =>
        linksByPort.get(input.id).exists(!_.forall(carriesState)) &&
        !input.dependencies.exists(carriesOnlyState)
      }
      Option.when(exposed.nonEmpty) {
        val inputs = exposed.map { input =>
          if (input.displayName.nonEmpty) s"input '${input.displayName}'"
          else s"input port ${input.id.id}"
        }
        new IllegalArgumentException(
          s"${logicalOp.operatorInfo.userFriendlyName} refers to loop variables " +
            s"(${formatReferences(logicalOp.stateReferences)}), but its ${inputs.mkString(" and ")} " +
            s"${if (exposed.size == 1) "is" else "are"} fed from outside the loop block, so " +
            "tuples from there can arrive before the loop state"
        )
      }
    }

  /**
    * A reference is bound by the late-bound executor that `ExecFactory` builds from the
    * descriptor's JSON. An operator whose executor runs code instead -- a UDF, or a descriptor
    * that generates Python from its properties, such as the sklearn operators -- never takes that
    * path: its code is built before the loop runs. A UDF's code and properties are the user's, and
    * nothing binds them. In generated code a typed placeholder is a value, so nothing binds it
    * either. A String property's `$name` is bound only where pyb's decoder rendered it, which
    * `PythonOperatorDescriptor` turned into a read of the loop state, `loopVariableLookup(name)`,
    * and then only if the code holds the text `$name` nowhere (where it does, the code pastes it
    * in as it is) and no output column the operator adds holds the text (its schema is computed
    * from the literal). A lookup the code runs before the iteration's state arrives, such as one
    * in `open`, fails when it runs, with a message that says so.
    * When the generation failed there is no code to read either way, and the code-generation
    * check reports why it failed instead.
    *
    * @param physicalOps the operator's own physical operators, their schemas propagated.
    */
  def unbindableStateReferences(
      logicalOp: LogicalOp,
      physicalOps: Iterable[PhysicalOp]
  ): Option[Throwable] = {
    val codes = physicalOps.toSeq.map(_.opExecInitInfo).collect {
      case OpExecWithCode(code, language) => (code, language)
    }
    if (logicalOp.stateReferences.isEmpty || codes.isEmpty) {
      None
    } else {
      val generated =
        logicalOp.isInstanceOf[PythonOperatorDescriptor] && codes.forall(_._2 == "python")
      val unbound: Seq[(Iterable[String], String)] =
        if (!generated) {
          Seq(logicalOp.stateReferences.keys -> fixedInUdf)
        } else {
          val tree = objectMapper.valueToTree[ObjectNode](logicalOp)
          val text = textReferences(tree, logicalOp.stateReferences)
          val scripts = codes.map(_._1)
          val added = addedColumns(physicalOps)
          def whyUnbound(name: String): Option[String] = {
            val lookup = loopVariableLookup(name)
            if (scripts.exists(_.contains("$" + name))) Some(pastedIntoCode)
            else if (!scripts.exists(_.contains(lookup))) Some(unreadByCode)
            else if (added.exists(_.contains("$" + name))) Some(namesOutputColumn)
            else None
          }
          val textReasons: Map[String, String] =
            if (scripts.exists(failedCodeGeneration.findFirstIn(_).nonEmpty)) Map.empty
            else text.flatMap { case (pointer, name) => whyUnbound(name).map(pointer -> _) }
          ((logicalOp.stateReferences -- text.keys).keys -> writtenIntoCode) +:
            Seq(pastedIntoCode, unreadByCode, namesOutputColumn).map { why =>
              textReasons.collect { case (pointer, `why`) => pointer } -> why
            }
        }
      val operator = logicalOp.operatorInfo.userFriendlyName
      val reasons = unbound.collect {
        case (pointers, why) if pointers.nonEmpty =>
          s"$operator cannot refer to loop variables in " +
            s"${pointers.toSeq.sorted.mkString(", ")} yet: $why"
      }
      Option.when(reasons.nonEmpty)(new UnsupportedOperationException(reasons.mkString("; ")))
    }
  }

  // Why a reference of an operator whose executor runs code is not bound, as the error says it.
  private val fixedInUdf = "its code and properties are fixed before the loop runs"
  private val writtenIntoCode =
    "numeric and boolean properties are written into its generated code before the loop runs"
  private val pastedIntoCode = "its code embeds the text before the loop runs"
  private val unreadByCode = "its code does not read the text when the loop runs"
  private val namesOutputColumn =
    "the text names an output column, which is set before the loop runs"

  /** The columns `physicalOps` add: on an output port of theirs, but on no input port. */
  private def addedColumns(physicalOps: Iterable[PhysicalOp]): Set[String] = {
    def columns(schemas: Iterable[(PortIdentity, Either[Throwable, Schema])]): Set[String] =
      schemas
        .collect {
          case (port, Right(schema)) if !port.internal => schema.getAttributeNames
        }
        .flatten
        .toSet
    columns(physicalOps.flatMap(_.outputPorts.map { case (port, (_, _, s)) => port -> s })) --
      columns(physicalOps.flatMap(_.inputPorts.map { case (port, (_, _, s)) => port -> s }))
  }

  /** What a Python-based operator's code is when generating it failed; the group is the reason. */
  private val failedCodeGeneration = """#EXCEPTION DURING CODE GENERATION:\s*(.*)""".r

  /** A descriptor's references as the compile errors name them: `/pointer -> $name, ...`. */
  private def formatReferences(references: Map[String, String]): String =
    references.toSeq.sorted.map { case (pointer, name) => s"$pointer -> $$$name" }.mkString(", ")

  private def collectOutputSchemaFromPhysicalPlan(
      physicalPlan: PhysicalPlan,
      errorList: ArrayBuffer[(OperatorIdentity, Throwable)]
  ): Map[OperatorIdentity, Map[PortIdentity, Option[Schema]]] = {

    // Collect output schemas per physical operator
    val physicalOutputSchemas =
      physicalPlan.operators.map { physicalOp =>
        val portSchemas = physicalOp.outputPorts.values
          .filterNot(_._1.id.internal)
          .map {
            case (port, _, schema) =>
              schema match {
                case Left(err) =>
                  errorList.append((physicalOp.id.logicalOpId, err))
                  port.id -> None
                case Right(validSchema) =>
                  port.id -> Some(validSchema)
              }
          }
          .toMap
        physicalOp.id -> portSchemas
      }

    // Group by logical operator ID and merge port schemas
    physicalOutputSchemas
      .groupBy(_._1.logicalOpId)
      .view
      .mapValues { list =>
        list.flatMap(_._2).toMap
      }
      .toMap
  }

}

case class WorkflowCompilationResult(
    logicalPlan: LogicalPlan,
    physicalPlan: Option[PhysicalPlan], // if physicalPlan is None, compilation failed
    operatorIdToOutputSchemas: Map[OperatorIdentity, Map[PortIdentity, Option[Schema]]],
    operatorIdToError: Map[OperatorIdentity, WorkflowFatalError],
    outputPortsNeedingStorage: Set[GlobalPortIdentity]
)

/**
  * The single workflow compiler shared in-process by both compilation call sites:
  * workflow-compiling-service (editing path, [[CompilationErrorHandling.Lenient]] — accumulate
  * per-operator errors so the UI can render them, `physicalPlan = None` when any exist) and
  * amber (execution path, [[CompilationErrorHandling.Strict]] — fail fast before a run).
  *
  * This module depends only on WorkflowOperator, so neither service leaks its HTTP stack into
  * the other; the engine `Workflow` wrapper stays in amber as a thin adapter over
  * [[WorkflowCompilationResult]]. `outputPortsNeedingStorage` is always computed — the
  * editing-path caller simply ignores it — keeping both paths on one code path.
  */
class WorkflowCompiler(
    context: WorkflowContext
) extends LazyLogging {

  /**
    * Expands the logical plan to a physical plan.
    * @return the expanded physical plan and a set of output ports that need storage
    */
  private def expandLogicalPlan(
      logicalPlan: LogicalPlan,
      logicalOpsToViewResult: List[String],
      errorList: Option[ArrayBuffer[(OperatorIdentity, Throwable)]]
  ): (PhysicalPlan, Set[GlobalPortIdentity]) = {
    val terminalLogicalOps = logicalPlan.getTerminalOperatorIds
    val logicalOpsNeedingStorage =
      (terminalLogicalOps ++ logicalOpsToViewResult.map(OperatorIdentity(_))).toSet
    var physicalPlan = PhysicalPlan(operators = Set.empty, links = Set.empty)
    val outputPortsNeedingStorage: mutable.HashSet[GlobalPortIdentity] = mutable.HashSet()
    val insideLoopBlocks = LoopBlockMembership.operatorsInsideLoopBlocks(logicalPlan)

    logicalPlan.getTopologicalOpIds.asScala.foreach(logicalOpId =>
      Try {
        val logicalOp = logicalPlan.getOperator(logicalOpId)
        val upstreamLinks = logicalPlan.getUpstreamLinks(logicalOp.operatorIdentifier)

        def report(error: Throwable): Unit =
          errorList match {
            case Some(list) => list.append((logicalOpId, error))
            case None       => throw error
          }
        // Before the physical plan: the descriptor serializes its sidecar into its descString.
        normalizeStateReferences(logicalOp, logicalPlan, insideLoopBlocks).foreach(report)
        val subPlan = logicalOp.getPhysicalPlan(context.workflowId, context.executionId)
        subPlan
          .topologicalIterator()
          .map(subPlan.getOperator)
          .foreach({ physicalOp =>
            {
              val externalLinks = upstreamLinks
                .filter(link => physicalOp.inputPorts.contains(link.toPortId))
                .flatMap { link =>
                  physicalPlan
                    .getPhysicalOpsOfLogicalOp(link.fromOpId)
                    .find(_.outputPorts.contains(link.fromPortId))
                    .map(fromOp =>
                      PhysicalLink(fromOp.id, link.fromPortId, physicalOp.id, link.toPortId)
                    )
                }

              val internalLinks = subPlan.getUpstreamPhysicalLinks(physicalOp.id)

              // Add the operator to the physical plan
              physicalPlan = physicalPlan.addOperator(physicalOp.propagateSchema())

              // Add all the links to the physical plan
              physicalPlan = (externalLinks ++ internalLinks)
                .foldLeft(physicalPlan) { (plan, link) => plan.addLink(link) }
            }
          })
        // The operator's own physical operators, their schemas propagated: a reference may name
        // a column they add.
        val physicalOps = subPlan.topologicalIterator().map(physicalPlan.getOperator).toList
        // Before the code-generation check below: the first error per operator is reported, and
        // code generated from a placeholder may fail on it with a less specific message.
        unbindableStateReferences(logicalOp, physicalOps).foreach(report)

        // **Check for Python-based operator errors during code generation**
        physicalOps.filter(_.isPythonBased).foreach { physicalOp =>
          failedCodeGeneration.findFirstMatchIn(physicalOp.getCode).foreach { matchResult =>
            val errorMessage = matchResult.group(1).trim
            report(new RuntimeException(s"Operator is not configured properly: $errorMessage"))
          }
        }

        // convert logical operators needing storage to output ports needing storage
        subPlan
          .topologicalIterator()
          .filter(opId => logicalOpsNeedingStorage.contains(opId.logicalOpId))
          .map(physicalPlan.getOperator)
          .foreach { physicalOp =>
            physicalOp.outputPorts
              .filterNot(_._1.internal)
              .foreach {
                case (outputPortId, _) =>
                  outputPortsNeedingStorage += GlobalPortIdentity(
                    opId = physicalOp.id,
                    portId = outputPortId
                  )
              }
          }
      } match {
        case Success(_) =>

        case Failure(err) =>
          errorList match {
            case Some(list) => list.append((logicalOpId, err))
            case None       => throw err
          }
      }
    )
    (physicalPlan, outputPortsNeedingStorage.toSet)
  }

  /**
    * Compiles a workflow to a physical plan, along with the schema propagation result and
    * errors (if any).
    *
    * @param logicalPlanPojo the POJO parsed from the workflow string provided by the user
    * @param errorHandling   Lenient (editing-time, collect all errors) or Strict (pre-execution, throw)
    * @return WorkflowCompilationResult, containing the logical plan, physical plan, output schemas per
    *         op, errors per op, and the output ports that need storage
    */
  def compile(
      logicalPlanPojo: LogicalPlanPojo,
      errorHandling: CompilationErrorHandling = CompilationErrorHandling.Lenient
  ): WorkflowCompilationResult = {
    // Lenient collects into a buffer; Strict passes None so the first error is thrown.
    val errorList: Option[ArrayBuffer[(OperatorIdentity, Throwable)]] =
      errorHandling match {
        case CompilationErrorHandling.Lenient =>
          Some(new ArrayBuffer[(OperatorIdentity, Throwable)]())
        case CompilationErrorHandling.Strict => None
      }

    // 1. convert the pojo to logical plan
    val logicalPlan: LogicalPlan = LogicalPlan(logicalPlanPojo)

    // 2. resolve the file name in each scan source operator
    logicalPlan.resolveScanSourceOpFileName(errorList)

    // 3. expand the logical plan to the physical plan, and get the output ports that need storage
    val (physicalPlan, outputPortsNeedingStorage) =
      expandLogicalPlan(logicalPlan, logicalPlanPojo.opsToViewResult, errorList)

    // 4. collect the output schema for each logical op
    // even if an error is encountered during logical => physical expansion, we still want to
    // collect the output schemas of the remaining no-error operators. In Lenient mode
    // schema-propagation failures land in
    // the shared buffer alongside the other errors; in Strict mode they must fail fast too (e.g. a
    // Projection on a missing column would otherwise be launched and only fail at runtime).
    val schemaErrorList = errorList.getOrElse(new ArrayBuffer[(OperatorIdentity, Throwable)]())
    val opIdToOutputSchema = collectOutputSchemaFromPhysicalPlan(physicalPlan, schemaErrorList)
    if (errorHandling == CompilationErrorHandling.Strict && schemaErrorList.nonEmpty) {
      throw schemaErrorList.head._2
    }

    val hasErrors = errorList.exists(_.nonEmpty)
    WorkflowCompilationResult(
      logicalPlan = logicalPlan,
      physicalPlan = if (hasErrors) None else Some(physicalPlan),
      operatorIdToOutputSchemas = opIdToOutputSchema,
      // map each error from OpId to WorkflowFatalError, and report them via logger
      operatorIdToError = convertErrorListToWorkflowFatalErrorMap(
        logger,
        errorList.map(_.toList).getOrElse(List.empty)
      ),
      outputPortsNeedingStorage = outputPortsNeedingStorage
    )
  }
}
