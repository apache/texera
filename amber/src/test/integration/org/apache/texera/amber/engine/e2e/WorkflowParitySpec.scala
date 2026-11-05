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

package org.apache.texera.amber.engine.e2e

import com.twitter.util.Duration
import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.testkit.{ImplicitSender, TestKit}
import org.apache.texera.amber.clustering.SingleNodeListener
import org.apache.texera.amber.core.tuple.{AttributeType, Tuple}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.e2e.TestUtils.{
  buildWorkflow,
  cleanupWorkflowExecutionData,
  initiateTexeraDBForTestCases,
  runWorkflowAndReadTerminalResults,
  setUpWorkflowExecutionData
}
import org.apache.texera.amber.operator.{LogicalOp, TestOperators}
import org.apache.texera.amber.operator.distinct.DistinctOpDesc
import org.apache.texera.amber.operator.filter.{
  ComparisonType,
  FilterPredicate,
  SpecializedFilterOpDesc
}
import org.apache.texera.amber.operator.limit.LimitOpDesc
import org.apache.texera.amber.operator.projection.{AttributeUnit, ProjectionOpDesc}
import org.apache.texera.amber.operator.regex.RegexOpDesc
import org.apache.texera.amber.operator.sort.{SortCriteriaUnit, SortOpDesc, SortPreference}
import org.apache.texera.amber.operator.source.scan.file.FileScanOpDesc
import org.apache.texera.amber.operator.source.scan.text.TextInputSourceOpDesc
import org.apache.texera.amber.operator.split.SplitOpDesc
import org.apache.texera.amber.operator.typecasting.{TypeCastingOpDesc, TypeCastingUnit}
import org.apache.texera.amber.operator.union.UnionOpDesc
import org.apache.texera.amber.tags.IntegrationTest
import org.apache.texera.amber.translator.WorkflowToPythonTranslator
import org.apache.texera.amber.translator.verify.{
  CanonicalSourceFixture,
  Comparator,
  StandaloneRunner,
  TupleIO
}
import org.apache.texera.common.compiler.model.{LogicalLink, LogicalPlan, LogicalPlanPojo}
import org.apache.texera.common.compiler.{CompilationErrorHandling, WorkflowCompiler}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

/** A whole workflow, run both ways: the Texera engine is the reference, and the
  * script the export writes for the same plan is what is under test.
  *
  * The per-operator specs feed every operator the shared table. Here an operator
  * reads what the operator before it produced, in each path, which is where a
  * fragment that agrees with the engine on its own can still disagree.
  *
  * Each leaf compares as a set of rows, as in the per-operator specs, unless the
  * operator that produces it declares an order: the engine runs most operators on
  * several workers and promises no order after them.
  *
  * Every plan reads the shared table, as the CSV the CSV source reads, each aimed
  * at one way fragments can go wrong together. TranslatedPlanRunSpec keeps the two
  * the engine has nothing to compare with: the files two charts write, and a chart
  * that ends the script.
  */
@IntegrationTest
class WorkflowParitySpec
    extends TestKit(ActorSystem("WorkflowParitySpec", AmberRuntime.pekkoConfig))
    with ImplicitSender
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private val specId = 7

  override protected def beforeEach(): Unit = setUpWorkflowExecutionData(specId)

  override protected def afterEach(): Unit = cleanupWorkflowExecutionData(specId)

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
    Class.forName("org.postgresql.Driver")
    initiateTexeraDBForTestCases()
  }

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  private def link(from: LogicalOp, to: LogicalOp): LogicalLink =
    LogicalLink(from.operatorIdentifier, PortIdentity(), to.operatorIdentifier, PortIdentity())

  private def link(from: LogicalOp, fromPort: Int, to: LogicalOp): LogicalLink =
    LogicalLink(
      from.operatorIdentifier,
      PortIdentity(fromPort),
      to.operatorIdentifier,
      PortIdentity()
    )

  /** The shared table, written as the CSV the CSV source reads, so a plan starts
    * from the rows every operator is verified on. Each call hands back a new
    * source over the same file, for a plan that reads it twice.
    */
  private def sharedTable(rows: Seq[Tuple]): (Path, () => LogicalOp) = {
    val dir = Files.createTempDirectory("workflow-parity-")
    val path = CanonicalSourceFixture.writeCsv(dir, StandardCharsets.UTF_8, ',', rows)
    (dir, () => TestOperators.getCsvScanOpDesc(path.toString, header = true))
  }

  /** Runs the plan in the engine and its translated script, and compares every
    * leaf the plan has.
    */
  private def assertParity(dir: Path, ops: List[LogicalOp], links: List[LogicalLink]): Unit = {
    val context = TestUtils.workflowContext(specId)
    val workflow = buildWorkflow(ops, links, context)
    // The schemas the export endpoint hands the translator, from the same compile.
    val schemas = new WorkflowCompiler(context)
      .compile(
        LogicalPlanPojo(ops, links, List.empty, List.empty),
        CompilationErrorHandling.Lenient
      )
      .operatorIdToOutputSchemas
    val leafIds = workflow.logicalPlan.getTerminalOperatorIds.toList
    val engineRows = runWorkflowAndReadTerminalResults(system, workflow, Duration.fromMinutes(2))

    val script = new WorkflowToPythonTranslator().translate(LogicalPlan(ops, links), schemas)
    // The translator prints each leaf under `[<operator name>] dfN:`, which is how
    // the frame a leaf ends up in is found.
    val printed: Map[String, String] = """print\("\\n\[([^\]]*)\] (df\d+):"\)""".r
      .findAllMatchIn(script)
      .map(m => m.group(1) -> m.group(2))
      .toList
      .groupMap(_._1)(_._2)
      .map {
        case (name, Seq(v)) => name -> v
        case (name, _)      => fail(s"two leaves print as [$name]; name them apart")
      }
    val leaves = leafIds.map { id =>
      val name = workflow.logicalPlan.getOperator(id).operatorInfo.userFriendlyName
      (id, printed.getOrElse(name, fail(s"no [$name] leaf in the script")))
    }
    val leafSchemas = leaves.map { case (id, _) => schemas(id)(PortIdentity()).get }
    val result = StandaloneRunner.runTranslated(script, leaves.map(_._2), leafSchemas, dir)

    leaves.zip(leafSchemas).zipWithIndex.foreach {
      case (((id, _), schema), i) =>
        val actual = dir.resolve(s"engine_$i.jsonl")
        TupleIO.writeTuples(actual, engineRows(id).iterator, schema)
        withClue(s"leaf ${workflow.logicalPlan.getOperator(id).operatorInfo.userFriendlyName}: ") {
          Comparator.assertEqual(
            actual,
            result.outputs(i + 1),
            orderSensitive = workflow.logicalPlan.getOperator(id).orderSensitive
          )
        }
    }
  }

  /** File Scan reads the paths its upstream hands it, so it cannot run in the
    * per-operator specs, which give a source no input. Here Text Input names the
    * shared table's own file, the same one every operator reads its rows from.
    */
  "A translated workflow" should "read the files its upstream names" in {
    val shared = Paths.get(getClass.getResource("/verify/canonical_fixture.json").toURI)
    val dir = Files.createTempDirectory("workflow-parity-")
    val paths = new TextInputSourceOpDesc
    paths.textInput = shared.toString
    val scan = new FileScanOpDesc
    assertParity(dir, List(paths, scan), List(link(paths, scan)))
  }

  // Each plan reads the shared table twice, whole and with a hole in every
  // column, so the values a type reaches only at its edges, and the empty cells,
  // pass through several operators rather than one.
  Seq(
    "the shared table" -> CanonicalSourceFixture.rows,
    "the shared table with a hole in every column" -> CanonicalSourceFixture.holedRows
  ).foreach {
    case (table, rows) =>
      // Four hops, so every variable the translator assigns is read by the next
      // fragment rather than by the print at the end. A two-operator plan cannot
      // show a chain threaded wrong, since df1 is the only name to get right.
      //
      // Every score differs, so the limit after the sort takes the same rows on
      // both sides. The projection comes before the sort, so the source's LONG
      // column never reaches a Python operator: the engine hands one a column
      // holding a null as float64, which cannot hold its values past 2^53, and
      // pauses the run. Move it back to the end once the engine keeps them.
      it should s"carry $table through a filter, a projection, a sort and a limit" in {
        val (dir, src) = sharedTable(rows)
        val filter = new SpecializedFilterOpDesc
        filter.predicates = List(new FilterPredicate("score", ComparisonType.GREATER_THAN, "1.0"))
        val sort = new SortOpDesc
        val criterion = new SortCriteriaUnit
        criterion.attributeName = "score"
        criterion.sortPreference = SortPreference.DESC
        sort.attributes = List(criterion)
        val limit = new LimitOpDesc
        limit.limit = 5
        val projection = new ProjectionOpDesc
        projection.attributes ++= List("id", "score", "edge_double", "edge_ts", "digits")
          .map(name => new AttributeUnit(name, name))

        val chain = List(src(), filter, projection, sort, limit)
        assertParity(dir, chain, chain.sliding(2).map { case List(a, b) => link(a, b) }.toList)
      }

      // Java writes a double past 1e7 or under 1e-3 in E notation, and the regex
      // after the cast keeps exactly those rows.
      it should s"cast $table's edge values to text the way the engine writes them" in {
        val (dir, src) = sharedTable(rows)
        val cast = new TypeCastingOpDesc
        cast.typeCastingUnits = List("edge_double", "edge_ts").map { name =>
          val unit = new TypeCastingUnit
          unit.attribute = name
          unit.resultType = AttributeType.STRING
          unit
        }
        val regex = new RegexOpDesc
        regex.attribute = "edge_double"
        regex.regex = "E"
        val source = src()
        assertParity(dir, List(source, cast, regex), List(link(source, cast), link(cast, regex)))
      }

      // A variadic port is the one placeholder a fragment cannot name, so the list
      // the translator writes in its place is only exercised by a real plan. The
      // two halves share no row, so a list that names one upstream twice and
      // drops the other loses rows rather than repeating them.
      it should s"union the two halves of $table and keep one copy of each row" in {
        val (dir, src) = sharedTable(rows)
        def half(comparison: ComparisonType): (LogicalOp, LogicalOp) = {
          val filter = new SpecializedFilterOpDesc
          filter.predicates = List(new FilterPredicate("score", comparison, "3.0"))
          (src(), filter)
        }
        val (highSource, high) = half(ComparisonType.GREATER_THAN)
        val (lowSource, low) = half(ComparisonType.LESS_THAN_OR_EQUAL_TO)
        val union = new UnionOpDesc
        val distinct = new DistinctOpDesc
        assertParity(
          dir,
          List(highSource, high, lowSource, low, union, distinct),
          List(
            link(highSource, high),
            link(lowSource, low),
            link(high, union),
            link(low, union),
            link(union, distinct)
          )
        )
      }

      // Every other plan here reads its upstream's port 0, so a translator that
      // handed each downstream the first frame its upstream produced would pass
      // all of them. A split sends different rows to each of its two ports, and
      // each branch is its own leaf, so a branch reading the other port's frame
      // disagrees with the engine on its rows. Split runs on one worker, so the
      // engine draws the same rows the script does.
      it should s"give each branch of a split over $table the port its link named" in {
        val (dir, src) = sharedTable(rows)
        val split = new SplitOpDesc
        split.k = 50
        split.random = false
        split.seed = 1
        val distinct = new DistinctOpDesc
        val limit = new LimitOpDesc
        limit.limit = 10
        val source = src()
        assertParity(
          dir,
          List(source, split, distinct, limit),
          List(link(source, split), link(split, 0, distinct), link(split, 1, limit))
        )
      }

      // A CSV carries no types, so the hole in `id` widens it and its 3 would be
      // matched as "3.0". Only a whole plan shows it: the type belongs to the
      // source, a hop away with a file in between. The hole is in the row whose
      // id is 7, so 3 is a value both tables hold.
      it should s"match $table's integer column on the type its source declared" in {
        val (dir, src) = sharedTable(rows)
        val regex = new RegexOpDesc
        regex.attribute = "id"
        regex.regex = "^3$"
        val source = src()
        assertParity(dir, List(source, regex), List(link(source, regex)))
      }

      // A cast makes the column a datetime, and Python's str() then writes no
      // fraction at all on a whole second where `Timestamp.toString` writes ".0".
      // One operator cannot show it, since the executor casts each column once
      // from its original value. Every trade date is a midnight.
      it should s"render $table's cast dates the way the engine's toString does" in {
        val (dir, src) = sharedTable(rows)
        val cast = new TypeCastingOpDesc
        val unit = new TypeCastingUnit
        unit.attribute = "trade_date"
        unit.resultType = AttributeType.TIMESTAMP
        cast.typeCastingUnits = List(unit)
        val regex = new RegexOpDesc
        regex.attribute = "trade_date"
        regex.regex = "\\.0$"
        val source = src()
        assertParity(dir, List(source, cast, regex), List(link(source, cast), link(cast, regex)))
      }

      // The substitution rewrites the variable a fragment reads with, not the
      // column name it asks that variable for. A fragment tested on its own never
      // shows the difference, because the harness binds the placeholder to itself.
      it should s"keep $table's column whose name matches a placeholder" in {
        val (dir, src) = sharedTable(rows)
        val projection = new ProjectionOpDesc
        projection.attributes ++= List(new AttributeUnit("in1df", "kept"))
        val source = src()
        assertParity(dir, List(source, projection), List(link(source, projection)))
      }
  }
}
