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

package org.apache.texera.amber.translator.verify

import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.amber.core.executor.{ExecFactory, OpExecWithClassName, OperatorExecutor}
import org.apache.texera.amber.core.tuple.{AttributeType, Schema, Tuple, TupleLike}
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{PhysicalOp, PhysicalPlan, PortIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.nio.file.{Files, Path}
import java.sql.Timestamp
import java.util.Base64
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
  * Generic harness that drives an OpDesc's OpExec(s) directly, bypassing the
  * Pekko/actor runtime.
  *
  * Works uniformly for: single-OpExec ops (filter, sort, projection), multi-
  * OpExec ops (hash join build+probe), multi-output ops (split), and source
  * ops (empty input map). All wiring info — number of OpExecs, internal links,
  * input-port dependency order — is derived from `opDesc.getPhysicalPlan(...)`,
  * so adding a new operator requires no harness changes.
  *
  * I/O is JSON Lines with sidecar schemas. Each input/output `*.jsonl` file
  * has a companion `*.jsonl.schema.json` describing its [[Schema]].
  *
  * Limitations (intentional for MVP):
  *   - Only `OpExecWithClassName` is supported; Python UDFs (`OpExecWithCode`)
  *     are out of scope because driving them needs a real Python worker.
  *   - Single worker only (idx=0, workerCount=1). Multi-worker partitioning
  *     would require coordinating partitioners across executors.
  *   - JSONL types: STRING / INTEGER / LONG / DOUBLE / BOOLEAN / BINARY /
  *     TIMESTAMP (the latter two via explicit base64 / JDBC-string codecs).
  */
object OpExecHarness extends LazyLogging {

  // Test-only workflow / execution IDs. The values don't matter — the harness
  // never persists state under them — but the PhysicalOp factory needs *some*
  // IDs to embed in PhysicalOpIdentity.
  private val TestWorkflowId = WorkflowIdentity(0L)
  private val TestExecutionId = ExecutionIdentity(0L)

  /**
    * @param outputs        external output port → JSONL file path
    * @param outputSchemas  same keys as outputs, gives each port's [[Schema]]
    */
  final case class Result(
      outputs: Map[PortIdentity, Path],
      outputSchemas: Map[PortIdentity, Schema]
  )

  /**
    * Run `opDesc` against the given inputs and write the outputs to `outputDir`.
    *
    * @param inputs map keyed by the *external* input port identifier (the one
    *               the user-visible LogicalOp exposes). Each path points to a
    *               `.jsonl` file with a sibling `.schema.json`.
    * @param outputDir destination directory; created if missing. Output files
    *                  are named `output_port_<id>.jsonl` per external output.
    */
  def execute(
      opDesc: LogicalOp,
      inputs: Map[PortIdentity, Path],
      outputDir: Path
  ): Result = {
    Files.createDirectories(outputDir)

    // 1. Compile OpDesc → PhysicalPlan. For most ops this is a single-PhysicalOp
    //    plan; HashJoin and other multi-stage ops return multiple PhysicalOps
    //    plus internal PhysicalLinks (e.g. build.out → probe.in0).
    val plan = opDesc.getPhysicalPlan(TestWorkflowId, TestExecutionId)

    // 2. Identify external input ports. A PhysicalOp port is "external" iff no
    //    PhysicalLink in this plan terminates at it. The user's `inputs` map
    //    must cover exactly these (matched by PortIdentity).
    val externalInputs: Set[(PhysicalOpIdentity, PortIdentity)] =
      plan.operators.flatMap { phOp =>
        phOp.inputPorts.keys.collect {
          case portId if !plan.links.exists(l => l.toOpId == phOp.id && l.toPortId == portId) =>
            (phOp.id, portId)
        }
      }
    validateInputCoverage(externalInputs, inputs.keySet)

    // 3. Load each input file as (Schema, Iterator[Tuple]). We read schemas
    //    eagerly but tuples lazily (saves memory on large fixtures).
    val inputSchemas: Map[PortIdentity, Schema] =
      inputs.map { case (portId, path) => portId -> TupleIO.readSchemaSidecar(path) }
    val inputTuples: Map[PortIdentity, () => Iterator[Tuple]] =
      inputs.map {
        case (portId, path) =>
          val schema = inputSchemas(portId)
          portId -> (() => TupleIO.readTuples(path, schema))
      }

    // 4. Propagate schemas. We CAN'T use `plan.propagateSchema(inputSchemas)`
    //    directly because it indexes by PortIdentity globally — for HashJoin
    //    probe's internal port 0 would collide with build's external port 0.
    //    Instead, set schemas only on the external (phOpId, portId) pairs and
    //    let `addLink` propagate the internal links' schemas naturally.
    val planWithSchemas =
      propagateExternalSchemas(plan, externalInputs, inputSchemas)

    // 5. Identify external output ports symmetrically: no outgoing PhysicalLink.
    val externalOutputs: Set[(PhysicalOpIdentity, PortIdentity)] =
      planWithSchemas.operators.flatMap { phOp =>
        phOp.outputPorts.keys.collect {
          case portId
              if !planWithSchemas.links
                .exists(l => l.fromOpId == phOp.id && l.fromPortId == portId) =>
            (phOp.id, portId)
        }
      }

    // 6. Instantiate OpExec per PhysicalOp. We only support OpExecWithClassName;
    //    fail loudly otherwise so test authors know to mock Python UDFs out.
    val opExecs: Map[PhysicalOpIdentity, OperatorExecutor] =
      planWithSchemas.operators.map { phOp =>
        phOp.opExecInitInfo match {
          case OpExecWithClassName(className, descString) =>
            phOp.id -> ExecFactory.newExecFromJavaClassName(
              className,
              descString,
              idx = 0,
              workerCount = 1
            )
          case other =>
            throw new UnsupportedOperationException(
              s"OpExecHarness only supports OpExecWithClassName, got: $other"
            )
        }
      }.toMap

    // 7. Drive each PhysicalOp in topological order. Buffer outputs in memory
    //    keyed by (producer phOpId, output port). Downstream PhysicalOps then
    //    consume from these buffers via the plan's internal links.
    val producedBuffer =
      mutable.Map.empty[(PhysicalOpIdentity, PortIdentity), mutable.ArrayBuffer[Tuple]]

    planWithSchemas.topologicalIterator().foreach { phOpId =>
      val phOp = planWithSchemas.getOperator(phOpId)
      val opExec = opExecs(phOpId)
      runOneOp(
        phOp,
        opExec,
        externalInputProvider = portId => inputTuples.get(portId).map(_.apply()),
        upstreamBuffer = producedBuffer,
        plan = planWithSchemas,
        produced = producedBuffer
      )
    }

    // 8. Materialize external outputs to JSONL with their propagated schemas.
    val outputPaths = mutable.Map.empty[PortIdentity, Path]
    val outputSchemas = mutable.Map.empty[PortIdentity, Schema]
    externalOutputs.foreach {
      case (phOpId, portId) =>
        val schema = planWithSchemas
          .getOperator(phOpId)
          .outputPorts(portId)
          ._3
          .toOption
          .getOrElse(
            throw new IllegalStateException(
              s"Output schema for ($phOpId, $portId) was not propagated"
            )
          )
        val tuples =
          producedBuffer.getOrElse((phOpId, portId), mutable.ArrayBuffer.empty[Tuple])
        val file = outputDir.resolve(s"output_port_${portId.id}.jsonl")
        TupleIO.writeTuples(file, tuples.iterator, schema)
        outputPaths(portId) = file
        outputSchemas(portId) = schema
    }

    Result(outputPaths.toMap, outputSchemas.toMap)
  }

  /**
    * Drives one PhysicalOp's lifecycle: open → input ports in dependency order
    * (processTupleMultiPort + onFinishMultiPort per port) → close. Source ops
    * (no input ports) get a single onFinishMultiPort(0) call which gives their
    * `produceTuple()`-backed implementation a chance to emit.
    *
    * Outputs are bucketed by output PortIdentity. `processTupleMultiPort`'s
    * `Option[PortIdentity]` return: None means port 0 (the default single-
    * output convention used by the trait's fallback). Multi-output ops like
    * Split set it explicitly.
    */
  private def runOneOp(
      phOp: PhysicalOp,
      opExec: OperatorExecutor,
      externalInputProvider: PortIdentity => Option[Iterator[Tuple]],
      upstreamBuffer: mutable.Map[
        (PhysicalOpIdentity, PortIdentity),
        mutable.ArrayBuffer[Tuple]
      ],
      plan: PhysicalPlan,
      produced: mutable.Map[
        (PhysicalOpIdentity, PortIdentity),
        mutable.ArrayBuffer[Tuple]
      ]
  ): Unit = {
    opExec.open()
    try {
      def bucket(emitted: Iterator[(TupleLike, Option[PortIdentity])]): Unit = {
        emitted.foreach {
          case (tupleLike, portOpt) =>
            // Default: the op's single output port. Most operators have one
            // output and use the trait's default port-0 wrapping, but
            // multi-stage plans (e.g. HashJoin build) put their internal
            // output on PortIdentity(0, internal = true) — a hardcoded
            // PortIdentity(0, false) would NoSuchElementException here.
            val outPortId = portOpt.getOrElse {
              if (phOp.outputPorts.size == 1) phOp.outputPorts.keys.head
              else PortIdentity(0)
            }
            val outSchema = phOp
              .outputPorts(outPortId)
              ._3
              .toOption
              .getOrElse(
                throw new IllegalStateException(
                  s"Op ${phOp.id} emitted to port $outPortId before its output schema was propagated"
                )
              )
            val tuple = tupleLike
              .asInstanceOf[org.apache.texera.amber.core.tuple.SeqTupleLike]
              .enforceSchema(outSchema)
            produced
              .getOrElseUpdate((phOp.id, outPortId), mutable.ArrayBuffer.empty[Tuple]) += tuple
        }
      }

      // Process each input port in declared dependency order (e.g. HashJoin
      // probe's build-side port must finish before the data-side port starts).
      val portOrder =
        if (phOp.getInputPortDependencyPairs.nonEmpty)
          phOp.getInputPortDependencyPairs
        else phOp.inputPorts.keys.toList.sortBy(_.id)

      portOrder.foreach { portId =>
        val tuples: Iterator[Tuple] =
          if (externalInputProvider(portId).isDefined) {
            externalInputProvider(portId).get
          } else {
            // Internal port: stitch upstream PhysicalLinks' buffers together
            val upstream = plan.links
              .filter(l => l.toOpId == phOp.id && l.toPortId == portId)
              .toList
              .sortBy(l => (l.fromOpId.toString, l.fromPortId.id))
            upstream.iterator
              .flatMap(l =>
                upstreamBuffer
                  .getOrElse(
                    (l.fromOpId, l.fromPortId),
                    mutable.ArrayBuffer.empty[Tuple]
                  )
                  .iterator
              )
          }

        tuples.foreach { t =>
          bucket(opExec.processTupleMultiPort(t, portId.id))
        }
        bucket(opExec.onFinishMultiPort(portId.id))
      }

      // Source operator: no input ports. Trigger production via onFinishMultiPort
      // on a synthetic port 0 — SourceOperatorExecutor.onFinish ignores the port
      // and emits everything from produceTuple().
      if (phOp.inputPorts.isEmpty) {
        bucket(opExec.onFinishMultiPort(0))
      }
    } finally {
      opExec.close()
    }
  }

  // Walks the plan in topo order, propagating schemas only at the truly external
  // input ports. Internal ports get their schema via `addLink` (PhysicalPlan
  // re-applies the source's output schema to the destination port). This avoids
  // a collision when multiple PhysicalOps share a PortIdentity (e.g. HashJoin
  // probe.in0 internal vs build.in0 external both have PortIdentity(0)).
  private def propagateExternalSchemas(
      plan: PhysicalPlan,
      externalPorts: Set[(PhysicalOpIdentity, PortIdentity)],
      schemas: Map[PortIdentity, Schema]
  ): PhysicalPlan = {
    var acc = PhysicalPlan(operators = Set.empty, links = Set.empty)
    plan.topologicalIterator().map(plan.getOperator).foreach { phOp =>
      val updated = phOp.inputPorts.keys.foldLeft(phOp) { (op, portId) =>
        if (externalPorts.contains((phOp.id, portId)) && schemas.contains(portId)) {
          op.propagateSchema(Some((portId, schemas(portId))))
        } else op
      }
      // .propagateSchema() with no arg re-fires output derivation if all inputs
      // are now resolved (source ops trigger immediately since inputPorts empty).
      acc = acc.addOperator(updated.propagateSchema())
      plan.getUpstreamPhysicalLinks(phOp.id).foreach { link =>
        acc = acc.addLink(link)
      }
    }
    acc
  }

  private def validateInputCoverage(
      external: Set[(PhysicalOpIdentity, PortIdentity)],
      provided: Set[PortIdentity]
  ): Unit = {
    val expected = external.map(_._2)
    val missing = expected -- provided
    val extra = provided -- expected
    require(
      missing.isEmpty,
      s"Missing input fixtures for external ports: $missing (expected $expected)"
    )
    if (extra.nonEmpty) {
      logger.warn(s"Input fixtures provided for non-external ports (ignored): $extra")
    }
  }
}

/**
  * JSON Lines I/O for Tuples. Each `.jsonl` file is paired with a
  * `.jsonl.schema.json` sidecar listing [[Attribute]]s in column order.
  *
  * Format example:
  *
  *   records.jsonl:
  *     {"id":1,"name":"alice"}
  *     {"id":2,"name":"bob"}
  *
  *   records.jsonl.schema.json:
  *     {"attributes":[{"attributeName":"id","attributeType":"integer"},
  *                    {"attributeName":"name","attributeType":"string"}]}
  *
  * pandas symmetry: `pd.read_json(path, lines=True)` and
  * `df.to_json(path, orient='records', lines=True)` round-trip cleanly for the
  * supported types (STRING / INTEGER / LONG / DOUBLE / BOOLEAN).
  */
object TupleIO {

  private def sidecar(path: Path): Path =
    path.resolveSibling(path.getFileName.toString + ".schema.json")

  def readSchemaSidecar(path: Path): Schema = {
    val text = new String(Files.readAllBytes(sidecar(path)))
    objectMapper.readValue(text, classOf[Schema])
  }

  def readTuples(path: Path, schema: Schema): Iterator[Tuple] = {
    // readAllLines closes the underlying handle; safer than Files.lines for
    // test-scale fixtures where memory cost is negligible.
    val lines = Files.readAllLines(path).asScala
    lines.iterator.filter(_.trim.nonEmpty).map { line =>
      val node = objectMapper.readTree(line)
      val builder = Tuple.builder(schema)
      schema.getAttributes.foreach { attr =>
        val fieldNode = node.get(attr.getName)
        val v: Any =
          if (fieldNode == null || fieldNode.isNull) null
          else
            attr.getType match {
              case AttributeType.STRING  => fieldNode.asText()
              case AttributeType.INTEGER => Int.box(fieldNode.asInt())
              case AttributeType.LONG    => Long.box(fieldNode.asLong())
              case AttributeType.DOUBLE  => Double.box(fieldNode.asDouble())
              case AttributeType.BOOLEAN => Boolean.box(fieldNode.asBoolean())
              case AttributeType.BINARY =>
                Base64.getDecoder.decode(fieldNode.asText())
              // Timestamps round-trip through the JDBC string form
              // ("yyyy-mm-dd hh:mm:ss[.f]"), the exact inverse of Timestamp.toString
              // below — timezone-free, so no shift across write/read. The Python
              // side reads this column with convert_dates=False (see
              // StandaloneRunner) and treats it as an opaque string, so both paths
              // agree on pass-through.
              case AttributeType.TIMESTAMP =>
                Timestamp.valueOf(fieldNode.asText())
              case other =>
                throw new UnsupportedOperationException(
                  s"TupleIO MVP doesn't support $other yet"
                )
            }
        builder.add(attr, v)
      }
      builder.build()
    }
  }

  def writeTuples(path: Path, tuples: Iterator[Tuple], schema: Schema): Unit = {
    // Sidecar first so a partial main-file write still has a recoverable schema.
    Files.write(sidecar(path), objectMapper.writeValueAsBytes(schema))
    val writer = Files.newBufferedWriter(path)
    try {
      tuples.foreach { t =>
        val node: ObjectNode = objectMapper.createObjectNode()
        schema.getAttributes.zipWithIndex.foreach {
          case (attr, idx) =>
            val v = t.getField[Any](idx)
            if (v == null) node.putNull(attr.getName)
            else
              attr.getType match {
                case AttributeType.STRING  => node.put(attr.getName, v.toString)
                case AttributeType.INTEGER => node.put(attr.getName, v.asInstanceOf[Int])
                case AttributeType.LONG    => node.put(attr.getName, v.asInstanceOf[Long])
                case AttributeType.DOUBLE  => node.put(attr.getName, v.asInstanceOf[Double])
                case AttributeType.BOOLEAN => node.put(attr.getName, v.asInstanceOf[Boolean])
                case AttributeType.BINARY =>
                  node.put(
                    attr.getName,
                    Base64.getEncoder.encodeToString(v.asInstanceOf[Array[Byte]])
                  )
                case AttributeType.TIMESTAMP =>
                  node.put(attr.getName, v.asInstanceOf[Timestamp].toString)
                case other =>
                  throw new UnsupportedOperationException(
                    s"TupleIO MVP doesn't support $other yet"
                  )
              }
        }
        writer.write(objectMapper.writeValueAsString(node))
        writer.newLine()
      }
    } finally writer.close()
  }
}
