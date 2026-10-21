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
import org.apache.texera.amber.core.tuple.AttributeType
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
import org.apache.texera.amber.translator.verify.{Comparator, StandaloneRunner, TupleIO}
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
  * The plans are TranslatedPlanRunSpec's, each aimed at one way fragments can go
  * wrong together. That spec keeps the two the engine has nothing to compare with:
  * the files two charts write, and a chart that ends the script.
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
  private val CsvName = "input.csv"

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

  /** Writes each CSV into a fresh directory and hands back a source for it. */
  private def inputs(csvs: Map[String, String]): (Path, Map[String, LogicalOp]) = {
    val dir = Files.createTempDirectory("workflow-parity-")
    val sources = csvs.map {
      case (name, content) =>
        val path = dir.resolve(name)
        Files.write(path, content.getBytes(StandardCharsets.UTF_8))
        name -> (TestOperators.getCsvScanOpDesc(path.toString, header = true): LogicalOp)
    }
    (dir, sources)
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

  /** Four hops, so every variable the translator assigns is read by the next
    * fragment rather than by the print at the end. A two-operator plan cannot
    * show a chain threaded wrong, since df1 is the only name to get right.
    */
  "A translated workflow" should "carry a frame through a chain of operators" in {
    val (dir, src) = inputs(Map(CsvName -> "label,amount\nant,3\nbee,1\ncat,5\ndog,2\n"))
    val filter = new SpecializedFilterOpDesc
    filter.predicates = List(new FilterPredicate("amount", ComparisonType.GREATER_THAN, "1"))
    val sort = new SortOpDesc
    val criterion = new SortCriteriaUnit
    criterion.attributeName = "amount"
    criterion.sortPreference = SortPreference.DESC
    sort.attributes = List(criterion)
    val limit = new LimitOpDesc
    limit.limit = 2
    val projection = new ProjectionOpDesc
    projection.attributes ++= List(new AttributeUnit("label", "label"))

    val chain = List(src(CsvName), filter, sort, limit, projection)
    assertParity(dir, chain, chain.sliding(2).map { case List(a, b) => link(a, b) }.toList)
  }

  /** A CSV carries no types, so a blank cell widens an integer column and its 6
    * would be matched as "6.0". Only a whole plan shows it: the type belongs to
    * the source, a hop away with a file in between.
    */
  it should "match an integer column on the type its source declared" in {
    val (dir, src) = inputs(Map(CsvName -> "label,n\nant,6\nbee,\ncat,70\n"))
    val regex = new RegexOpDesc
    regex.attribute = "n"
    regex.regex = "^6$"
    assertParity(dir, List(src(CsvName), regex), List(link(src(CsvName), regex)))
  }

  /** A cast makes the column a datetime, and Python's str() then writes no
    * fraction at all on a whole second where `Timestamp.toString` writes ".0".
    * One operator cannot show it, since the executor casts each column once from
    * its original value.
    */
  it should "render a cast timestamp the way the engine's toString does" in {
    val (dir, src) = inputs(
      Map(CsvName -> "label,ts\nant,2024-01-07 00:00:00\nbee,2024-03-15 08:30:00.123\n")
    )
    val cast = new TypeCastingOpDesc
    val unit = new TypeCastingUnit
    unit.attribute = "ts"
    unit.resultType = AttributeType.TIMESTAMP
    cast.typeCastingUnits = List(unit)
    val regex = new RegexOpDesc
    regex.attribute = "ts"
    regex.regex = "\\.0$"
    assertParity(
      dir,
      List(src(CsvName), cast, regex),
      List(link(src(CsvName), cast), link(cast, regex))
    )
  }

  /** A variadic port is the one placeholder a fragment cannot name, so the list
    * the translator writes in its place is only exercised by a real plan.
    */
  it should "hand a variadic port the frames its upstreams produced" in {
    val (dir, src) = inputs(
      // A row only one of them reads, so a list that names one upstream twice
      // and drops the other loses a row rather than repeating one.
      Map("left.csv" -> "label,amount\nant,3\n", "right.csv" -> "label,amount\nbee,1\n")
    )
    val union = new UnionOpDesc
    assertParity(
      dir,
      List(src("left.csv"), src("right.csv"), union),
      List(link(src("left.csv"), union), link(src("right.csv"), union))
    )
  }

  /** Every other plan here reads its upstream's port 0, so a translator that
    * handed each downstream the first frame its upstream produced would pass
    * all of them. A split sends different rows to each of its two ports, and
    * each branch is its own leaf, so a branch reading the other port's frame
    * disagrees with the engine on its rows. Split runs on one worker, so the
    * engine draws the same rows the script does.
    */
  it should "give each branch of a split the port its link named" in {
    val (dir, src) = inputs(
      Map(CsvName -> "label,amount\nant,1\nbee,2\ncat,3\ndog,4\neel,5\nfox,6\n")
    )
    val split = new SplitOpDesc
    split.k = 50
    split.random = false
    split.seed = 1
    val distinct = new DistinctOpDesc
    val limit = new LimitOpDesc
    limit.limit = 10
    assertParity(
      dir,
      List(src(CsvName), split, distinct, limit),
      List(link(src(CsvName), split), link(split, 0, distinct), link(split, 1, limit))
    )
  }

  /** File Scan reads the paths its upstream hands it, so it cannot run in the
    * per-operator specs, which give a source no input. Here Text Input names the
    * shared table's own file, the same one every operator reads its rows from.
    */
  it should "read the files its upstream names" in {
    val shared = Paths.get(getClass.getResource("/verify/canonical_fixture.json").toURI)
    val dir = Files.createTempDirectory("workflow-parity-")
    val paths = new TextInputSourceOpDesc
    paths.textInput = shared.toString
    val scan = new FileScanOpDesc
    assertParity(dir, List(paths, scan), List(link(paths, scan)))
  }

  /** The substitution rewrites the variable a fragment reads with, not the
    * column name it asks that variable for. A fragment tested on its own never
    * shows the difference, because the harness binds the placeholder to itself.
    */
  it should "keep a column whose name matches a placeholder" in {
    val (dir, src) = inputs(Map(CsvName -> "in1df,other\n1,a\n2,b\n"))
    val projection = new ProjectionOpDesc
    projection.attributes ++= List(new AttributeUnit("in1df", "kept"))
    assertParity(dir, List(src(CsvName), projection), List(link(src(CsvName), projection)))
  }
}
