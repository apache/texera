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
import org.apache.pekko.util.Timeout
import org.apache.texera.amber.clustering.SingleNodeListener
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType}
import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow.{
  ExecutionMode,
  PortIdentity,
  WorkflowContext,
  WorkflowSettings
}
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.e2e.TestUtils.{
  buildWorkflow,
  cleanupWorkflowExecutionData,
  initiateTexeraDBForTestCases,
  runWorkflowAndReadResults,
  setUpWorkflowExecutionData,
  workflowContext
}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.filter.{
  ComparisonType,
  FilterPredicate,
  SpecializedFilterOpDesc
}
import org.apache.texera.amber.operator.hashJoin.{HashJoinOpDesc, JoinType}
import org.apache.texera.amber.operator.ifStatement.IfOpDesc
import org.apache.texera.amber.operator.limit.LimitOpDesc
import org.apache.texera.amber.operator.loop.{LoopEndOpDesc, LoopStartOpDesc}
import org.apache.texera.amber.operator.sleep.SleepOpDesc
import org.apache.texera.amber.operator.source.scan.text.TextInputSourceOpDesc
import org.apache.texera.amber.operator.udf.python.PythonUDFOpDescV2
import org.apache.texera.amber.operator.union.UnionOpDesc
import org.apache.texera.amber.tags.IntegrationTest
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.common.compiler.model.LogicalLink
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Outcome, Retries}

import scala.concurrent.duration.DurationInt

/**
  * End-to-end loop tests: run a real TextInput -> LoopStart -> LoopEnd workflow
  * through the engine (controller + Python workers + the DCM back-jump and
  * region re-execution) and assert both that it terminates AND that it ran the
  * expected number of iterations.
  *
  * Termination alone is too weak: a counter bug that still terminated (e.g.
  * off-by-one) would pass. So each test asserts the LoopEnd's MATERIALIZED
  * result-table row count, read from iceberg after the run. LoopEnd is an
  * identity pass-through on data, so the rows it materializes equal the rows
  * that flowed through it: a single (outermost) LoopEnd accumulates every
  * iteration (3 for the single loop; 4 for the terminal outer LoopEnd of the
  * 2x2 nested loop), and an inner LoopEnd resets once per outer iteration (so
  * 2, not 4).
  *
  * Iteration counts are data-driven (`i < len(table)`), so the input length is
  * the cost knob: every iteration re-executes its region and respawns every
  * worker in it (~2s per Python worker). The single-loop tests keep a 3-row
  * input so one loop takes the same back-edge twice in a row; the nested tests
  * use a 2-row input (2 outer x 2 inner) because nesting depth, not the
  * per-level iteration count, is what exercises the counter/routing envelope,
  * and 3x3 nearly doubles the suite's worker respawns for no extra coverage.
  *
  * NOTE: the cumulative `ExecutionStatsUpdate` output count is NOT usable as
  * the iteration count here. A loop region's workers are recreated on every
  * `JumpToOperatorRegion` re-execution (each iteration spawns a fresh worker
  * actor), so the per-logical-op output statistic reflects only the final
  * iteration's worker and does not accumulate. The iceberg result, by
  * contrast, persists across iterations (the scheduler reuses a LoopEnd's
  * output document via its output port's `reuseStorage` flag), so it is
  * the reliable signal.
  *
  * Tagged @IntegrationTest because it spawns Python workers; routed to the
  * `amber-integration` CI job.
  */
@IntegrationTest
class LoopIntegrationSpec
    extends TestKit(ActorSystem("LoopIntegrationSpec", AmberRuntime.pekkoConfig))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Retries {

  override def withFixture(test: NoArgTest): Outcome =
    withRetry { super.withFixture(test) }

  implicit val timeout: Timeout = Timeout(5.seconds)

  // Unique per-suite id so the seeded user/workflow/version/execution rows
  // (and the context's workflow/execution ids) don't collide with the other
  // integration suites running against the shared test database (#5888).
  // 1-4 and 5 are taken by the other e2e/integration specs (5 by
  // MultiRegionWorkflowIntegrationSpec), so this suite uses 6.
  private val specId = 6

  override protected def beforeEach(): Unit = setUpWorkflowExecutionData(specId)

  override protected def afterEach(): Unit = cleanupWorkflowExecutionData(specId)

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
    Class.forName("org.postgresql.Driver")
    initiateTexeraDBForTestCases()
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  // Loops require MATERIALIZED execution mode (the cross-region state channel
  // is the loop back-edge). Built on the suite's specId so the context's
  // workflow/execution ids match the rows seeded by setUpWorkflowExecutionData.
  private def materializedContext(): WorkflowContext =
    workflowContext(
      specId,
      WorkflowSettings(
        dataTransferBatchSize = 400,
        executionMode = ExecutionMode.MATERIALIZED
      )
    )

  /**
    * Run the loop workflow to completion and return each operator's materialized
    * RESULT-table row count, keyed by operator id. Delegates to the shared
    * `TestUtils.runWorkflowAndReadResults` harness (a correct loop terminates
    * within the 90-second deadline; a broken one hangs until it). The deadline
    * is ~2.5x the slowest healthy case on CI (the nested 2x2 workflows, ~35s on
    * the 3-core macOS runner). A hung run burns the whole deadline and is then
    * re-run by `withRetry`, so a loose deadline directly inflates CI time.
    */
  private def runAndGetMaterializedRowCounts(
      operators: List[LogicalOp],
      links: List[LogicalLink]
  ): Map[OperatorIdentity, Long] =
    runWorkflowAndReadResults(
      system,
      buildWorkflow(operators, links, materializedContext()),
      operators.map(_.operatorIdentifier),
      _.getCount,
      Duration.fromSeconds(90)
    )

  private def textInput(text: String): TextInputSourceOpDesc = {
    val op = new TextInputSourceOpDesc()
    op.textInput = text
    op
  }

  private def loopStart(initialization: String, output: String): LoopStartOpDesc = {
    val op = new LoopStartOpDesc()
    op.initialization = initialization
    op.output = output
    op
  }

  private def loopEnd(update: String, condition: String): LoopEndOpDesc = {
    val op = new LoopEndOpDesc()
    op.update = update
    op.condition = condition
    op
  }

  private def limit(n: Int): LimitOpDesc = {
    val op = new LimitOpDesc()
    op.limit = n
    op
  }

  private def sleep(seconds: Int): SleepOpDesc = {
    val op = new SleepOpDesc()
    op.sleepTime = seconds
    op
  }

  /** A Limit whose Int `limit` is `"$name"`, parsed as the editor's JSON is, into a placeholder. */
  private def limitReferringTo(name: String): LogicalOp =
    objectMapper.readValue(s"""{"operatorType":"Limit","limit":"$$$name"}""", classOf[LogicalOp])

  /** `attribute = value`; a `$name` value refers to a loop variable (bound at the worker). */
  private def filterEquals(attribute: String, value: String): SpecializedFilterOpDesc = {
    val op = new SpecializedFilterOpDesc()
    op.predicates = List(new FilterPredicate(attribute, ComparisonType.EQUAL_TO, value))
    op
  }

  // NOTE: a JavaUDF (runtime-compiled Java) case cannot run in this suite.
  // The integration tests run forkless under sbt (fork := false), where
  // `javax.tools` sees only the sbt launcher on `java.class.path` -- the
  // Texera classes live in sbt's layered classloaders, so
  // JavaRuntimeCompilation fails with "package ... does not exist" before
  // any loop logic runs. Production workers launch with a real classpath,
  // so this is a harness limitation, not an engine one.

  /** A pass-through Python UDF that ALSO emits its own boundary state via the
    * public `produce_state_on_finish` API -- the state arrives downstream
    * with the "no loop" envelope (counter 0, no LoopStart stamp).
    */
  private def statefulPythonUDF(): PythonUDFOpDescV2 = {
    val op = new PythonUDFOpDescV2()
    op.code = """from pytexera import *
                |
                |class ProcessTupleOperator(UDFOperatorV2):
                |
                |    @overrides
                |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
                |        yield tuple_
                |
                |    @overrides
                |    def produce_state_on_finish(self, port: int) -> Optional[State]:
                |        return State({"note": "from-body-op"})
                |""".stripMargin
    op.workers = 1
    op.retainInputColumns = true
    op
  }

  /** A Python UDF that adds an `i` column to every row it passes: loop variable `i`, as it reads
    * it off `self.loop_variables` in `process_tuple`.
    */
  private def pythonUDFReadingLoopVariableInProcessTuple(): PythonUDFOpDescV2 = {
    val op = new PythonUDFOpDescV2()
    op.code = """from pytexera import *
                |
                |class ProcessTupleOperator(UDFOperatorV2):
                |
                |    @overrides
                |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
                |        tuple_["i"] = self.loop_variables["i"]
                |        yield tuple_
                |""".stripMargin
    op.workers = 1
    op.retainInputColumns = true
    op.outputColumns = List(new Attribute("i", AttributeType.INTEGER))
    op
  }

  /** As `pythonUDFReadingLoopVariableInProcessTuple`, but reading `i` in `process_table`, which
    * runs once its input has ended, after every state message on it arrived.
    */
  private def pythonUDFReadingLoopVariableInProcessTable(): PythonUDFOpDescV2 = {
    val op = new PythonUDFOpDescV2()
    op.code = """from pytexera import *
                |
                |class ProcessTableOperator(UDFTableOperator):
                |
                |    @overrides
                |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
                |        for line in table["line"]:
                |            yield {"line": line, "i": self.loop_variables["i"]}
                |""".stripMargin
    op.workers = 1
    op.retainInputColumns = true
    op.outputColumns = List(new Attribute("i", AttributeType.INTEGER))
    op
  }

  /** A pass-through Python UDF that publishes the row it saw as `score` when its input ends. */
  private def scoringPythonUDF(): PythonUDFOpDescV2 = {
    val op = new PythonUDFOpDescV2()
    op.code = """from pytexera import *
                |
                |class ProcessTupleOperator(UDFOperatorV2):
                |
                |    @overrides
                |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
                |        self.score = int(tuple_["line"])
                |        yield tuple_
                |
                |    @overrides
                |    def produce_state_on_finish(self, port: int) -> Optional[State]:
                |        return State({"score": self.score})
                |""".stripMargin
    op.workers = 1
    op.retainInputColumns = true
    op
  }

  /** A pass-through Python UDF that adds one row for each state message its process_state runs on. */
  private def messageCountingPythonUDF(): PythonUDFOpDescV2 = {
    val op = new PythonUDFOpDescV2()
    op.code = """from pytexera import *
                |
                |class ProcessTupleOperator(UDFOperatorV2):
                |
                |    @overrides
                |    def open(self) -> None:
                |        self.messages = 0
                |
                |    @overrides
                |    def process_state(self, state: State, port: int) -> Optional[State]:
                |        self.messages += 1
                |        return state
                |
                |    @overrides
                |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
                |        yield tuple_
                |
                |    @overrides
                |    def on_finish(self, port: int) -> Iterator[Optional[TupleLike]]:
                |        for _ in range(self.messages):
                |            yield Tuple({"line": "message"})
                |""".stripMargin
    op.workers = 1
    op.retainInputColumns = true
    op
  }

  /** A pass-through Python UDF whose process_state sets the loop variable `x` to 2. */
  private def xSettingPythonUDF(): PythonUDFOpDescV2 = {
    val op = new PythonUDFOpDescV2()
    op.code = """from pytexera import *
                |
                |class ProcessTupleOperator(UDFOperatorV2):
                |
                |    @overrides
                |    def process_state(self, state: State, port: int) -> Optional[State]:
                |        state["x"] = 2
                |        return state
                |
                |    @overrides
                |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
                |        yield tuple_
                |""".stripMargin
    op.workers = 1
    op.retainInputColumns = true
    op
  }

  /** A pass-through Python UDF whose process_state halves the loop variable `lr` when it is there. */
  private def lrHalvingPythonUDF(): PythonUDFOpDescV2 = {
    val op = new PythonUDFOpDescV2()
    op.code = """from pytexera import *
                |
                |class ProcessTupleOperator(UDFOperatorV2):
                |
                |    @overrides
                |    def process_state(self, state: State, port: int) -> Optional[State]:
                |        if "lr" in state:
                |            state["lr"] = state["lr"] // 2
                |        return state
                |
                |    @overrides
                |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
                |        yield tuple_
                |""".stripMargin
    op.workers = 1
    op.retainInputColumns = true
    op
  }

  private def link(from: LogicalOp, to: LogicalOp): LogicalLink =
    LogicalLink(from.operatorIdentifier, PortIdentity(), to.operatorIdentifier, PortIdentity())

  private def portLink(from: LogicalOp, fromPort: Int, to: LogicalOp, toPort: Int): LogicalLink =
    LogicalLink(
      from.operatorIdentifier,
      PortIdentity(fromPort),
      to.operatorIdentifier,
      PortIdentity(toPort)
    )

  /** Run the loop workflow and return the lines the Loop End `end` materialized, sorted. */
  private def runAndGetLines(
      operators: List[LogicalOp],
      links: List[LogicalLink],
      end: LogicalOp
  ): List[String] =
    runWorkflowAndReadResults(
      system,
      buildWorkflow(operators, links, materializedContext()),
      List(end.operatorIdentifier),
      _.get().toList,
      Duration.fromSeconds(90)
    ).getOrElse(end.operatorIdentifier, Nil)
      .map(_.getField[String]("line"))
      .sorted

  /** Run the loop workflow and return the (line, i) rows the Loop End `end` materialized, sorted. */
  private def runAndGetLinesWithLoopVariable(
      operators: List[LogicalOp],
      links: List[LogicalLink],
      end: LogicalOp
  ): List[(String, Int)] =
    runWorkflowAndReadResults(
      system,
      buildWorkflow(operators, links, materializedContext()),
      List(end.operatorIdentifier),
      _.get().toList,
      Duration.fromSeconds(90)
    ).getOrElse(end.operatorIdentifier, Nil)
      .map(row => (row.getField[String]("line"), row.getField[Int]("i")))
      .sorted

  "Engine" should "run a single TextInput -> LoopStart -> LoopEnd loop for exactly 3 iterations" in {
    val src = textInput("1\n2\n3")
    val start = loopStart("i = 0", "table.iloc[i]")
    val end = loopEnd("i += 1", "i < len(table)")
    val materialized = runAndGetMaterializedRowCounts(
      List(src, start, end),
      List(link(src, start), link(start, end))
    )
    // LoopStart emits one row per iteration (table.iloc[i]); i advances 0,1,2
    // and stops at i == 3, so the body runs exactly 3 times. The outermost
    // LoopEnd never resets, so its materialized result holds all 3 rows. An
    // off-by-one counter bug that still terminated would land on 2 or 4.
    val endRows = materialized.getOrElse(end.operatorIdentifier, -1L)
    assert(
      endRows == 3,
      s"single LoopEnd must accumulate all 3 iterations in its materialized " +
        s"result: expected 3, got $endRows (all: $materialized)"
    )
  }

  it should "run a nested loop for exactly 4 inner iterations (2 outer x 2 inner)" in {
    // TextInput -> OuterStart -> InnerStart -> InnerEnd -> OuterEnd.
    //
    // The outer LoopStart emits the WHOLE 2-row table on each outer iteration
    // (output = "table"), so the inner loop iterates over 2 rows; with 2 outer
    // iterations the inner body runs 2 x 2 = 4 times. Because every LoopEnd is
    // an identity pass-through on data, the same 4 rows flow out of the
    // terminal outer LoopEnd.
    //
    // This is the case that exercises the loop_counter increment/decrement and
    // the loop_start_id routing on the StateFrame envelope (write addresses:
    // see InitializeExecutorRequest.loopStartPortUris): the outer loop's state
    // passes THROUGH the inner LoopStart (+1) and inner LoopEnd (-1) untouched,
    // and is consumed only at the outer LoopEnd (counter == 0). A routing or
    // counter bug would change the 4, or mis-consume and hang.
    val src = textInput("1\n2")
    val outerStart = loopStart("i = 0", "table")
    val innerStart = loopStart("j = 0", "table.iloc[j]")
    val innerEnd = loopEnd("j += 1", "j < len(table)")
    val outerEnd = loopEnd("i += 1", "i < len(table)")
    val materialized = runAndGetMaterializedRowCounts(
      List(src, outerStart, innerStart, innerEnd, outerEnd),
      List(
        link(src, outerStart),
        link(outerStart, innerStart),
        link(innerStart, innerEnd),
        link(innerEnd, outerEnd)
      )
    )
    // The outer LoopEnd accumulates all 4 rows; the INNER LoopEnd resets once
    // per outer iteration (see main_loop's reset_output_storage call site), so
    // it holds only the last outer iteration's 2 rows. The inner == 2
    // assertion is the one that fails against the pre-fix code.
    val outerRows = materialized.getOrElse(outerEnd.operatorIdentifier, -1L)
    val innerRows = materialized.getOrElse(innerEnd.operatorIdentifier, -1L)
    assert(
      outerRows == 4,
      s"outer LoopEnd must accumulate all 4 inner-iteration rows: " +
        s"expected 4, got $outerRows (all: $materialized)"
    )
    assert(
      innerRows == 2,
      s"inner LoopEnd must reset per outer iteration (2 rows, not 4): " +
        s"expected 2, got $innerRows (all: $materialized)"
    )
  }

  it should "run a loop whose body contains a JVM (Scala) operator" in {
    // TextInput -> LoopStart -> Limit (a Scala built-in) -> LoopEnd.
    //
    // The loop envelope (loop_counter / loop_start_id) must survive the hop
    // through a JVM operator: the Scala worker replays the materialized state,
    // forwards it through the default processState, and re-materializes it for
    // the LoopEnd. Before the envelope was carried through on the Scala side
    // (StateFrame fields + reader/emit/save plumbing), this hop zeroed the
    // counter and blanked the id, so the LoopEnd captured loop_start_id = ""
    // and the back-jump failed with "no loop bookkeeping URI configured for
    // LoopStart ''" -- the loop never iterated. Limit(10) passes every row
    // through, so the loop semantics are identical to the single-loop test.
    val src = textInput("1\n2\n3")
    val start = loopStart("i = 0", "table.iloc[i]")
    val mid = limit(10)
    val end = loopEnd("i += 1", "i < len(table)")
    val materialized = runAndGetMaterializedRowCounts(
      List(src, start, mid, end),
      List(link(src, start), link(start, mid), link(mid, end))
    )
    val endRows = materialized.getOrElse(end.operatorIdentifier, -1L)
    assert(
      endRows == 3,
      s"LoopEnd must accumulate all 3 iterations with a Scala operator in the " +
        s"loop body: expected 3, got $endRows (all: $materialized)"
    )
  }

  it should "bind a $n loop-variable reference in a JVM operator's number setting on every iteration" in {
    // TextInput(3 rows) -> LoopStart(n = 1 / table) -> Limit($n)
    //   -> LoopEnd(n += 1 / n <= len(table)).
    // Iteration n passes n of the 3 rows: 1 + 2 + 3 = 6 at the LoopEnd. The placeholder 0 would
    // give 0, and an n that never rebinds to the new value 3.
    val src = textInput("a\nb\nc")
    val start = loopStart("n = 1", "table")
    val mid = limitReferringTo("n")
    val end = loopEnd("n += 1", "n <= len(table)")
    val materialized = runAndGetMaterializedRowCounts(
      List(src, start, mid, end),
      List(link(src, start), link(start, mid), link(mid, end))
    )
    val endRows = materialized.getOrElse(end.operatorIdentifier, -1L)
    assert(
      endRows == 6,
      s"Limit($$n) must pass n rows on iterations n = 1, 2, 3: expected 6, got $endRows " +
        s"(all: $materialized)"
    )
  }

  it should "bind the inner loop's variable where it shadows the outer loop's of the same name" in {
    // TextInput("0","1") -> OuterStart(i = 0 / table) -> InnerStart(i = 0 / table.iloc[i])
    //   -> Filter(line = $i) -> InnerEnd -> OuterEnd.
    // Both loops name their variable i. On each first inner iteration the Filter receives both
    // loops' states, the outer one's a loop further out (loopCounter 1), and must take the inner
    // i: inner iteration i emits the row "i", so all 2 x 2 rows pass. The outer i on the second
    // outer iteration (1, against the row "0") would drop one, and treating the two as one loop's
    // conflicting values would fail the run.
    val src = textInput("0\n1")
    val outerStart = loopStart("i = 0", "table")
    val innerStart = loopStart("i = 0", "table.iloc[i]")
    val mid = filterEquals("line", "$i")
    val innerEnd = loopEnd("i += 1", "i < len(table)")
    val outerEnd = loopEnd("i += 1", "i < len(table)")
    val materialized = runAndGetMaterializedRowCounts(
      List(src, outerStart, innerStart, mid, innerEnd, outerEnd),
      List(
        link(src, outerStart),
        link(outerStart, innerStart),
        link(innerStart, mid),
        link(mid, innerEnd),
        link(innerEnd, outerEnd)
      )
    )
    val outerRows = materialized.getOrElse(outerEnd.operatorIdentifier, -1L)
    assert(
      outerRows == 4,
      s"Filter(line = $$i) must bind the inner i on all 4 inner iterations: expected 4, got " +
        s"$outerRows (all: $materialized)"
    )
  }

  it should "bind a $i reference after two branches of the loop body meet again" in {
    // TextInput("0","1","2") -> LoopStart(i = 0 / table.iloc[i]) -> Limit(10) and Sleep(0)
    //   -> Union -> Filter(line = $i) -> LoopEnd.
    // The Filter receives the iteration's state once per branch, and the copies agree; the row
    // "i" arrives once per branch and passes against i: 2 rows on each of 3 iterations.
    val src = textInput("0\n1\n2")
    val start = loopStart("i = 0", "table.iloc[i]")
    val left = limit(10)
    val right = sleep(0)
    val union = new UnionOpDesc()
    val mid = filterEquals("line", "$i")
    val end = loopEnd("i += 1", "i < len(table)")
    val materialized = runAndGetMaterializedRowCounts(
      List(src, start, left, right, union, mid, end),
      List(
        link(src, start),
        link(start, left),
        link(start, right),
        link(left, union),
        link(right, union),
        link(union, mid),
        link(mid, end)
      )
    )
    val endRows = materialized.getOrElse(end.operatorIdentifier, -1L)
    assert(
      endRows == 6,
      s"Filter(line = $$i) after the branches meet must pass both copies of each iteration's " +
        s"row: expected 6, got $endRows (all: $materialized)"
    )
  }

  it should "run a nested loop whose inner body chains multiple JVM operators" in {
    // TextInput -> OuterStart -> InnerStart -> Limit -> Sleep(0) -> InnerEnd -> OuterEnd.
    //
    // The strongest combination: nested (the OUTER loop's state crosses the
    // JVM hops stamped with loop_counter = 1, so the counter MAGNITUDE must
    // survive both hops -- a zeroed counter would make the inner LoopEnd
    // mis-consume the outer state instead of passing it through with -1) x
    // multi-hop (the JVM-to-JVM state handoff: the first Scala worker WRITES
    // the envelope columns to its output state table and the second Scala
    // worker READS them back) x both operator kinds.
    //
    // This case strictly subsumes a nested loop with a single JVM operator
    // and a single loop with a JVM chain; those were removed as separate
    // tests because each loop iteration respawns every worker in the
    // re-executed regions (~2s per Python worker), making every extra e2e
    // workflow expensive (tens of seconds per nested case in CI).
    val src = textInput("1\n2")
    val outerStart = loopStart("i = 0", "table")
    val innerStart = loopStart("j = 0", "table.iloc[j]")
    val first = limit(10)
    val second = sleep(0)
    val innerEnd = loopEnd("j += 1", "j < len(table)")
    val outerEnd = loopEnd("i += 1", "i < len(table)")
    val materialized = runAndGetMaterializedRowCounts(
      List(src, outerStart, innerStart, first, second, innerEnd, outerEnd),
      List(
        link(src, outerStart),
        link(outerStart, innerStart),
        link(innerStart, first),
        link(first, second),
        link(second, innerEnd),
        link(innerEnd, outerEnd)
      )
    )
    val outerRows = materialized.getOrElse(outerEnd.operatorIdentifier, -1L)
    val innerRows = materialized.getOrElse(innerEnd.operatorIdentifier, -1L)
    assert(
      outerRows == 4,
      s"outer LoopEnd must accumulate all 4 inner-iteration rows with two " +
        s"chained JVM operators in the inner body: expected 4, got $outerRows " +
        s"(all: $materialized)"
    )
    assert(
      innerRows == 2,
      s"inner LoopEnd must reset per outer iteration (2 rows, not 4): " +
        s"expected 2, got $innerRows (all: $materialized)"
    )
  }

  it should "run a loop whose body operator emits its own boundary state" in {
    // TextInput -> LoopStart -> stateful Python UDF -> LoopEnd.
    //
    // The UDF emits boundary state via produce_state_on_finish (a public API
    // on both engine sides), which reaches the LoopEnd with the "no loop"
    // envelope (counter 0, no LoopStart stamp) AFTER the forwarded loop
    // state. The LoopEnd collects it with the loop state, and it must not
    // clobber the captured back-jump id with "": that would fail the deferred
    // consume's bookkeeping-URI lookup ("no loop bookkeeping URI configured
    // for LoopStart ''"). Regression test for #discussion_r3648708075 on
    // #6661.
    val src = textInput("1\n2\n3")
    val start = loopStart("i = 0", "table.iloc[i]")
    val mid = statefulPythonUDF()
    val end = loopEnd("i += 1", "i < len(table)")
    val materialized = runAndGetMaterializedRowCounts(
      List(src, start, mid, end),
      List(link(src, start), link(start, mid), link(mid, end))
    )
    val endRows = materialized.getOrElse(end.operatorIdentifier, -1L)
    assert(
      endRows == 3,
      s"LoopEnd must accumulate all 3 iterations with a state-emitting " +
        s"operator in the loop body: expected 3, got $endRows (all: $materialized)"
    )
  }

  it should "read a loop variable in a Python UDF on every iteration" in {
    // TextInput("a","b","c") -> LoopStart(i = 0 / table.iloc[i]) -> Python UDF -> LoopEnd.
    // Iteration i hands the UDF the row of index i, and the UDF adds the i it reads off
    // self.loop_variables in process_tuple, so the LoopEnd accumulates (a, 0), (b, 1), (c, 2).
    // A UDF that kept reading the first iteration's i would write 0 on every row.
    val src = textInput("a\nb\nc")
    val start = loopStart("i = 0", "table.iloc[i]")
    val mid = pythonUDFReadingLoopVariableInProcessTuple()
    val end = loopEnd("i += 1", "i < len(table)")
    val rows = runAndGetLinesWithLoopVariable(
      List(src, start, mid, end),
      List(link(src, start), link(start, mid), link(mid, end)),
      end
    )
    assert(
      rows == List(("a", 0), ("b", 1), ("c", 2)),
      s"each iteration's row must carry that iteration's i: got $rows"
    )
  }

  it should "read a loop variable in a Python UDF after another body operator's own state arrives" in {
    // TextInput("a","b","c") -> LoopStart(i = 0 / table.iloc[i]) -> stateful Python UDF
    //   -> Python UDF reading i in process_table -> LoopEnd.
    // The first UDF returns its own state, {"note": ...}, from produce_state_on_finish, so the
    // second receives that state after the loop's. process_table runs once the input ends, after
    // both arrived, and must still read the loop's i: that state names no loop variable, so it
    // must not hide one. Reading only the latest state message would fail with KeyError 'i'.
    val src = textInput("a\nb\nc")
    val start = loopStart("i = 0", "table.iloc[i]")
    val first = statefulPythonUDF()
    val second = pythonUDFReadingLoopVariableInProcessTable()
    val end = loopEnd("i += 1", "i < len(table)")
    val rows = runAndGetLinesWithLoopVariable(
      List(src, start, first, second, end),
      List(link(src, start), link(start, first), link(first, second), link(second, end)),
      end
    )
    assert(
      rows == List(("a", 0), ("b", 1), ("c", 2)),
      s"each iteration's row must carry that iteration's i: got $rows"
    )
  }

  it should "stop a loop once a score computed in its body stops improving, past a JVM operator" in {
    // TextInput("3","5","4","6") -> LoopStart(i = 0, prev = None, score = 0 / table.iloc[i])
    //   -> Limit(10) -> scoring UDF -> LoopEnd(again = prev is None or score > prev;
    //   prev = score; i += 1 / again).
    // The UDF publishes the row it saw as `score` after the loop's state has passed it. The scores
    // are 3, 5, 4: the third does not improve, so the loop stops after 3 iterations. A LoopEnd that
    // never sees the body's score compares the initial 0 with itself and stops after 2. Loop
    // Start's message passes the Limit, a JVM operator, on its way to Loop End, whose update reads
    // `prev` in the first iteration, so `prev = None` must still be there.
    val src = textInput("3\n5\n4\n6")
    val start = loopStart("i = 0\nprev = None\nscore = 0", "table.iloc[i]")
    val jvm = limit(10)
    val mid = scoringPythonUDF()
    val end = loopEnd("again = prev is None or score > prev\nprev = score\ni += 1", "again")
    val materialized = runAndGetMaterializedRowCounts(
      List(src, start, jvm, mid, end),
      List(link(src, start), link(start, jvm), link(jvm, mid), link(mid, end))
    )
    val endRows = materialized.getOrElse(end.operatorIdentifier, -1L)
    assert(
      endRows == 3,
      s"prev = None must reach Loop End through the Limit, and the loop must stop on the first " +
        s"score that does not improve (3, 5, 4): expected 3 iterations, got $endRows " +
        s"(all: $materialized)"
    )
  }

  it should "run a body operator's process_state once per message where two branches meet" in {
    // TextInput("0","1","2") -> LoopStart(i = 0 / table.iloc[i]) -> Limit(10) and Sleep(0)
    //   -> both into one input port of a message-counting UDF -> LoopEnd.
    // Loop Start's message reaches the UDF once per branch, as copies of one message. Each
    // iteration's row passes both branches (2 rows) and the UDF adds a row per message it
    // processed: 1 for the one message, so 3 rows on each of 3 iterations. Processing every copy
    // would add 2 and give 12.
    val src = textInput("0\n1\n2")
    val start = loopStart("i = 0", "table.iloc[i]")
    val left = limit(10)
    val right = sleep(0)
    val mid = messageCountingPythonUDF()
    val end = loopEnd("i += 1", "i < len(table)")
    val materialized = runAndGetMaterializedRowCounts(
      List(src, start, left, right, mid, end),
      List(
        link(src, start),
        link(start, left),
        link(start, right),
        link(left, mid),
        link(right, mid),
        link(mid, end)
      )
    )
    val endRows = materialized.getOrElse(end.operatorIdentifier, -1L)
    assert(
      endRows == 9,
      s"the UDF must process Loop Start's message once per iteration, not once per branch: " +
        s"expected 9, got $endRows (all: $materialized)"
    )
  }

  it should "give a JVM operator the value the loop body publishes after Loop Start's own value" in {
    // TextInput("1","2","3") -> LoopStart(i = 0, score = 0 / table.iloc[i]) -> scoring UDF
    //   -> Filter(line = $score) -> LoopEnd(score = int(score); i += 1 / i < len(table)).
    // Loop Start's message carries a score (0, then the score the update saved), and the UDF
    // publishes the row it saw, both from the same loop and before the Filter's first row: the
    // later value replaces the earlier one, so each iteration's row passes its Filter: 3 rows.
    // Refusing the second value would fail the Filter in the first iteration.
    val src = textInput("1\n2\n3")
    val start = loopStart("i = 0\nscore = 0", "table.iloc[i]")
    val mid = scoringPythonUDF()
    val filter = filterEquals("line", "$score")
    val end = loopEnd("score = int(score)\ni += 1", "i < len(table)")
    val materialized = runAndGetMaterializedRowCounts(
      List(src, start, mid, filter, end),
      List(link(src, start), link(start, mid), link(mid, filter), link(filter, end))
    )
    val endRows = materialized.getOrElse(end.operatorIdentifier, -1L)
    assert(
      endRows == 3,
      s"Filter(line = $$score) must bind the score the body published after Loop Start's: " +
        s"expected 3, got $endRows (all: $materialized)"
    )
  }

  it should "let a value the loop body changed win over Loop Start's unchanged message" in {
    // TextInput("a","b","c") -> LoopStart(i = 0, x = 1 / table.iloc[i]) -> a Python UDF whose
    //   process_state sets x = 2 -> LoopEnd(i += 1 / x == 2 and i < len(table)),
    //   and LoopStart -> Sleep(0) -> the same LoopEnd port.
    // Loop End receives the UDF's changed message and, through the Sleep, Loop Start's message
    // unchanged, in either order. The body's change wins, so the condition reads x = 2 and the
    // loop runs all 3 iterations, each row reaching Loop End once per branch: 6 rows. Taking the
    // message that arrives last reads x = 1 whenever the unchanged one comes last, and stops the
    // loop after its first iteration (2 rows).
    val src = textInput("a\nb\nc")
    val start = loopStart("i = 0\nx = 1", "table.iloc[i]")
    val changing = xSettingPythonUDF()
    val unchanged = sleep(0)
    val end = loopEnd("i += 1", "x == 2 and i < len(table)")
    val materialized = runAndGetMaterializedRowCounts(
      List(src, start, changing, unchanged, end),
      List(
        link(src, start),
        link(start, changing),
        link(start, unchanged),
        link(changing, end),
        link(unchanged, end)
      )
    )
    val endRows = materialized.getOrElse(end.operatorIdentifier, -1L)
    assert(
      endRows == 6,
      s"Loop End must read the body's x = 2 in every iteration: expected 6, got $endRows " +
        s"(all: $materialized)"
    )
  }

  it should "carry a change the outer body makes before an inner loop to the outer Loop End" in {
    // TextInput("1") -> OuterStart(i = 0, lr = 4 / table) -> a Python UDF whose process_state
    //   halves lr -> InnerStart(j = 0 / table) -> InnerEnd(j += 1 / j < 1)
    //   -> OuterEnd(i += 1 / lr > 1 and i < 3).
    // The UDF sends the outer Loop Start's message on before its change, so two messages of the
    // outer loop pass the inner loop in each outer iteration, and both must reach the outer Loop
    // End: lr goes 4, 2, 1, and the outer loop runs 2 iterations of one row each. An outer Loop
    // End without Loop Start's message saves only i, starts every outer iteration from lr = 4,
    // and runs 3.
    val src = textInput("1")
    val outerStart = loopStart("i = 0\nlr = 4", "table")
    val halve = lrHalvingPythonUDF()
    val innerStart = loopStart("j = 0", "table")
    val innerEnd = loopEnd("j += 1", "j < 1")
    val outerEnd = loopEnd("i += 1", "lr > 1 and i < 3")
    val materialized = runAndGetMaterializedRowCounts(
      List(src, outerStart, halve, innerStart, innerEnd, outerEnd),
      List(
        link(src, outerStart),
        link(outerStart, halve),
        link(halve, innerStart),
        link(innerStart, innerEnd),
        link(innerEnd, outerEnd)
      )
    )
    val outerRows = materialized.getOrElse(outerEnd.operatorIdentifier, -1L)
    assert(
      outerRows == 2,
      s"the outer loop must carry the halved lr into its next iteration and stop after 2: " +
        s"expected 2, got $outerRows (all: $materialized)"
    )
  }

  it should "route each iteration's row at an If whose condition comes from the loop's message" in {
    // TextInput("a","b") -> LoopStart(i = 0, take = False / table.iloc[i]) -> Limit(0)
    //   -> the If's Condition port (0); LoopStart -> the If's data port (1); the If's True port
    //   -> LoopEnd(i += 1; take = i == 1 / i < len(table)).
    // The If's data port depends on its Condition port, so the engine runs the Condition port
    // first, before the If has any output port. The first iteration's row "a" takes the False
    // port, which leads nowhere, and the second iteration's row "b" reaches Loop End. An If that
    // loses Loop Start's message on its data port stops the loop after its first iteration, with
    // no row at Loop End.
    val src = textInput("a\nb")
    val start = loopStart("i = 0\ntake = False", "table.iloc[i]")
    val noRows = limit(0)
    val branch = new IfOpDesc()
    branch.conditionName = "take"
    val end = loopEnd("i += 1\ntake = i == 1", "i < len(table)")
    val lines = runAndGetLines(
      List(src, start, noRows, branch, end),
      List(
        link(src, start),
        link(start, noRows),
        portLink(noRows, 0, branch, 0),
        portLink(start, 0, branch, 1),
        portLink(branch, 1, end, 0)
      ),
      end
    )
    assert(
      lines == List("b"),
      s"the If must route by take in both iterations and pass Loop Start's message on: " +
        s"expected List(b), got $lines"
    )
  }

  it should "run a loop whose body joins Loop Start's rows with what a body operator builds" in {
    // TextInput("a","b") -> LoopStart(i = 0 / table.iloc[i]) -> Limit(10) -> the HashJoin's build
    //   port (0); LoopStart -> its probe port (1), joined on line -> LoopEnd(i += 1 / i < len(table)).
    // The probe port depends on the build port, so the engine runs the build port first, before
    // the join has any output port. Each iteration's row joins itself: one row on each of 2
    // iterations. A join that loses Loop Start's message on its probe port stops the loop after
    // its first iteration (1 row).
    val src = textInput("a\nb")
    val start = loopStart("i = 0", "table.iloc[i]")
    val build = limit(10)
    val join = new HashJoinOpDesc[String]()
    join.buildAttributeName = "line"
    join.probeAttributeName = "line"
    join.joinType = JoinType.INNER
    val end = loopEnd("i += 1", "i < len(table)")
    val materialized = runAndGetMaterializedRowCounts(
      List(src, start, build, join, end),
      List(
        link(src, start),
        link(start, build),
        portLink(build, 0, join, 0),
        portLink(start, 0, join, 1),
        link(join, end)
      )
    )
    val endRows = materialized.getOrElse(end.operatorIdentifier, -1L)
    assert(
      endRows == 2,
      s"the join must pass Loop Start's message on in both iterations: expected 2, got " +
        s"$endRows (all: $materialized)"
    )
  }

}
