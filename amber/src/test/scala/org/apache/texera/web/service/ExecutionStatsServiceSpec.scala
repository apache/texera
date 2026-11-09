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

package org.apache.texera.web.service

import io.reactivex.rxjava3.disposables.Disposable
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.core.storage.model.VirtualDocument
import org.apache.texera.amber.core.storage.{DocumentFactory, VFSURIFactory}
import org.apache.texera.amber.core.tuple.Tuple
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ExecutionIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{PhysicalPlan, PortIdentity, WorkflowContext}
import org.apache.texera.amber.core.workflowruntimestate.FatalErrorType.EXECUTION_FAILURE
import org.apache.texera.amber.engine.architecture.coordinator.{
  CoordinatorConfig,
  ExecutionStateUpdate,
  ExecutionStatsUpdate,
  FatalError,
  RuntimeStatisticsPersist,
  WorkerAssignmentUpdate,
  WorkflowRecoveryStatus
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState._
import org.apache.texera.amber.engine.architecture.worker.statistics.{
  PortTupleMetricsMapping,
  TupleMetrics
}
import org.apache.texera.amber.engine.common.client.AmberClient
import org.apache.texera.amber.engine.common.executionruntimestate.{
  ExecutionStatsStore,
  OperatorMetrics,
  OperatorStatistics
}
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.WORKFLOW_EXECUTIONS
import org.apache.texera.dao.jooq.generated.tables.daos.{
  UserDao,
  WorkflowDao,
  WorkflowExecutionsDao,
  WorkflowVersionDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  User,
  Workflow,
  WorkflowExecutions,
  WorkflowVersion
}
import org.apache.texera.web.model.websocket.event.{
  ExecutionDurationUpdateEvent,
  OperatorAggregatedMetrics,
  OperatorStatisticsUpdateEvent,
  TexeraWebSocketEvent,
  WorkerAssignmentUpdateEvent
}
import org.apache.texera.web.storage.{ExecutionStateStore, StateStore}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.net.URI
import java.sql.Timestamp
import java.util.concurrent.ExecutorService
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
  * The service class was entirely uncovered. It owns two independent outputs: the websocket
  * diff handlers that tell the frontend what each operator is doing, and the runtime-statistics
  * iceberg table that the dashboard replays after the execution is over.
  *
  * Everything here runs against an empty-plan AmberClient, so no engine is involved: the test
  * fires the client events straight into the callbacks the service registered.
  *
  * Two things about the ids below are deliberate. They must not collide with the
  * runtime-statistics URI of any other suite, because the constructor's `createDocument`
  * truncates whatever table is already there and sbt runs amber suites in parallel in one JVM
  * (DefaultCostEstimatorSpec owns wid 1 / eid 1). They must also differ from each other, so a
  * URI built from the wrong identity is a visibly different URI.
  *
  * Not covered on purpose: the catch arm around `runtimeStatsWriter.close()` (iceberg's close is
  * a repeatable buffer flush and does not throw), and the catch arm in `storeRuntimeStatistics`
  * (it runs on the private persist executor, which swallows anything a test could observe).
  */
class ExecutionStatsServiceSpec
    extends TestKit(ActorSystem("ExecutionStatsServiceSpec"))
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MockTexeraDB {

  private val testUid: Integer = 9107
  private val testWid: Integer = 9107
  private val testEid: Integer = 9108

  private val statsUri: URI = VFSURIFactory.createRuntimeStatisticsURI(
    WorkflowIdentity(testWid.longValue()),
    ExecutionIdentity(testEid.longValue())
  )

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()

    val user = new User
    user.setUid(testUid)
    user.setName("execution-stats-test-user")
    user.setEmail(s"u$testUid@example.com")
    new UserDao(getDSLContext.configuration()).insert(user)

    val workflow = new Workflow
    workflow.setWid(testWid)
    workflow.setName(s"execution-stats-test-$testWid")
    workflow.setContent("{}")
    workflow.setDescription("")
    workflow.setCreationTime(new Timestamp(System.currentTimeMillis()))
    workflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))
    new WorkflowDao(getDSLContext.configuration()).insert(workflow)

    val version = new WorkflowVersion
    version.setWid(testWid)
    version.setContent("{}")
    version.setCreationTime(new Timestamp(System.currentTimeMillis()))
    new WorkflowVersionDao(getDSLContext.configuration()).insert(version)

    // The execution row is what the constructor stamps the statistics URI onto; it has to be
    // reachable from testWid through workflow_version, which is how the update is scoped.
    val execution = new WorkflowExecutions
    execution.setEid(testEid)
    execution.setVid(version.getVid)
    execution.setUid(testUid)
    execution.setStatus(0.toByte)
    execution.setStartingTime(new Timestamp(System.currentTimeMillis()))
    execution.setBookmarked(false)
    execution.setName("execution-stats-test-execution")
    execution.setEnvironmentVersion("test-env")
    new WorkflowExecutionsDao(getDSLContext.configuration()).insert(execution)
  }

  override protected def afterAll(): Unit = {
    try {
      TestKit.shutdownActorSystem(system)
      closeConnectionPool()
    } finally super.afterAll()
  }

  /** Empty-plan client that captures every callback the service registers, keyed by event type. */
  private final class TestAmberClient
      extends AmberClient(
        system,
        new WorkflowContext(),
        PhysicalPlan(Set.empty, Set.empty),
        CoordinatorConfig(None, None, None, None),
        _ => ()
      ) {
    var shutdownCount = 0
    private val callbacks = mutable.Map.empty[Class[_], Any => Unit]

    override def shutdown(): Unit = {
      shutdownCount += 1
      super.shutdown()
    }

    override def registerCallback[T](callback: T => Unit)(implicit ct: ClassTag[T]): Disposable = {
      callbacks(ct.runtimeClass) = callback.asInstanceOf[Any => Unit]
      Disposable.empty()
    }

    /** Delivers an engine event the way the client's observable would. */
    def fire[T <: AnyRef](event: T)(implicit ct: ClassTag[T]): Unit =
      callbacks(ct.runtimeClass)(event)

    def dispose(): Unit = super.shutdown()
  }

  private final class Fixture(
      val client: TestAmberClient,
      val stateStore: ExecutionStateStore,
      val service: ExecutionStatsService
  ) {
    def close(): Unit = {
      service.unsubscribeAll()
      client.dispose()
    }
  }

  private def withService(body: Fixture => Unit): Unit = {
    val client = new TestAmberClient
    val stateStore = new ExecutionStateStore
    val service = new ExecutionStatsService(
      client,
      stateStore,
      new WorkflowContext(
        workflowId = WorkflowIdentity(testWid.longValue()),
        executionId = ExecutionIdentity(testEid.longValue())
      )
    )
    val fixture = new Fixture(client, stateStore, service)
    try body(fixture)
    finally fixture.close()
  }

  /**
    * One batch of websocket events is published per state update, so a test that cares about
    * what a *second* update publishes has to subscribe before the first one. Subscribing later
    * would also replay the diff between the default state and the current one as a batch.
    */
  private def recordBatches(
      store: StateStore[ExecutionStatsStore]
  )(body: => Unit): Seq[Iterable[TexeraWebSocketEvent]] = {
    val batches = ListBuffer.empty[Iterable[TexeraWebSocketEvent]]
    val subscription = store.getWebsocketEventObservable
      .subscribe((batch: Iterable[TexeraWebSocketEvent]) => batches += batch)
    try body
    finally subscription.dispose()
    batches.toSeq
  }

  private def metrics(
      state: WorkflowAggregatedState,
      inputs: Seq[(Int, Long, Long)] = Seq.empty,
      outputs: Seq[(Int, Long, Long)] = Seq.empty,
      numWorkers: Int = 1,
      dataProcessingTime: Long = 0,
      controlProcessingTime: Long = 0,
      idleTime: Long = 0
  ): OperatorMetrics = {
    def portMetrics(ports: Seq[(Int, Long, Long)]): Seq[PortTupleMetricsMapping] =
      ports.map {
        case (port, count, size) =>
          PortTupleMetricsMapping(PortIdentity(port), TupleMetrics(count, size))
      }

    OperatorMetrics(
      state,
      OperatorStatistics(
        portMetrics(inputs),
        portMetrics(outputs),
        numWorkers,
        dataProcessingTime,
        controlProcessingTime,
        idleTime
      )
    )
  }

  private def statisticsEvents(
      batch: Iterable[TexeraWebSocketEvent]
  ): Map[String, OperatorAggregatedMetrics] =
    batch.collect { case e: OperatorStatisticsUpdateEvent => e.operatorStatistics }.flatten.toMap

  /**
    * The persist callback hands its work to a private single-threaded executor. A no-op task
    * queued behind it can only run once that work is done, which is the barrier a test needs
    * before it closes the writer and reads the table back.
    */
  private def awaitPersist(service: ExecutionStatsService): Unit = {
    val field = classOf[ExecutionStatsService].getDeclaredField("metricsPersistThread")
    field.setAccessible(true)
    field.get(service).asInstanceOf[ExecutorService].submit((() => ()): Runnable).get()
  }

  /** Reads the committed statistics rows. Opening (never re-creating) the document is what makes
    * this a read: `createDocument` truncates.
    */
  private def persistedRows(): Seq[Tuple] =
    DocumentFactory
      .openDocument(statsUri)
      ._1
      .asInstanceOf[VirtualDocument[Tuple]]
      .get()
      .toSeq

  "the operator statistics callback" should
    "publish per-port and aggregate metrics for every operator" in {
    // Input and output are two symmetrical groups of fields on the same event; the frontend
    // shows them as separate columns, so they are given distinct counts, sizes and port ids
    // here to make crossing them visible.
    withService { f =>
      val batches = recordBatches(f.stateStore.statsStore) {
        f.client.fire(
          ExecutionStatsUpdate(
            Map(
              "opA" -> metrics(
                RUNNING,
                inputs = Seq((0, 3L, 30L), (1, 4L, 40L)),
                outputs = Seq((2, 5L, 50L)),
                numWorkers = 2,
                dataProcessingTime = 11L,
                controlProcessingTime = 12L,
                idleTime = 13L
              ),
              // A second operator, because the handler maps over every reported operator and one
              // operator cannot show that: `operatorInfo.collect` could be reduced to
              // `operatorInfo.take(1).collect` and a single-operator fixture would not notice.
              "opB" -> metrics(
                COMPLETED,
                inputs = Seq((0, 9L, 90L)),
                outputs = Seq((1, 8L, 80L)),
                numWorkers = 4,
                dataProcessingTime = 21L,
                controlProcessingTime = 22L,
                idleTime = 23L
              )
            )
          )
        )
      }

      val published = statisticsEvents(batches.flatten)
      published.keySet shouldBe Set("opA", "opB")
      val opA = published("opA")
      opA.operatorState shouldBe "Running"
      opA.aggregatedInputRowCount shouldBe 7L
      opA.aggregatedInputSize shouldBe 70L
      opA.inputPortMetrics shouldBe Map("0" -> 3L, "1" -> 4L)
      opA.aggregatedOutputRowCount shouldBe 5L
      opA.aggregatedOutputSize shouldBe 50L
      opA.outputPortMetrics shouldBe Map("2" -> 5L)
      opA.numWorkers shouldBe 2L
      opA.aggregatedDataProcessingTime shouldBe 11L
      opA.aggregatedControlProcessingTime shouldBe 12L
      opA.aggregatedIdleTime shouldBe 13L

      val opB = published("opB")
      opB.operatorState shouldBe "Completed"
      opB.aggregatedInputRowCount shouldBe 9L
      opB.aggregatedOutputRowCount shouldBe 8L
      opB.numWorkers shouldBe 4L
    }
  }

  it should "publish nothing when the reported metrics repeat" in {
    // The frontend redraws the whole statistics panel per event, so re-announcing identical
    // numbers is pure churn on every heartbeat of a long-running execution.
    withService { f =>
      val reported = Map("opA" -> metrics(RUNNING, inputs = Seq((0, 1L, 10L))))

      val batches = recordBatches(f.stateStore.statsStore) {
        f.client.fire(ExecutionStatsUpdate(reported))
        // Same metrics, plus a change the duration handler reacts to, so a second batch is
        // published at all -- it just must not carry a statistics event.
        f.stateStore.statsStore.updateState(_.withOperatorInfo(reported).withStartTimeStamp(1L))
      }

      batches should have size 2
      statisticsEvents(batches.head).keySet shouldBe Set("opA")
      // Collected by event TYPE, not by payload: flattening the payload map would treat a
      // present-but-empty OperatorStatisticsUpdateEvent as "nothing published", when it is in fact
      // churn on the websocket. This matches the two sibling "publish nothing" tests below.
      batches.last.collect { case e: OperatorStatisticsUpdateEvent => e } shouldBe empty
    }
  }

  "the worker assignment callback" should
    "publish one event per operator carrying that operator's workers" in {
    withService { f =>
      val batches = recordBatches(f.stateStore.statsStore) {
        f.client.fire(
          WorkerAssignmentUpdate(
            Map("opA" -> Seq("worker-a-0", "worker-a-1"), "opB" -> Seq("worker-b-0"))
          )
        )
      }

      val assignments = batches.flatten.collect {
        case e: WorkerAssignmentUpdateEvent => e.operatorId -> e.workerIds
      }
      assignments.toMap shouldBe Map(
        "opA" -> Seq("worker-a-0", "worker-a-1"),
        "opB" -> Seq("worker-b-0")
      )
    }
  }

  it should "publish nothing when the assignment is unchanged" in {
    withService { f =>
      val batches = recordBatches(f.stateStore.statsStore) {
        f.client.fire(WorkerAssignmentUpdate(Map("opA" -> Seq("worker-a-0"))))
        // A later statistics update changes the state without touching the assignment.
        f.client.fire(ExecutionStatsUpdate(Map("opA" -> metrics(RUNNING))))
      }

      batches should have size 2
      batches.head.collect { case e: WorkerAssignmentUpdateEvent => e } should have size 1
      batches.last.collect { case e: WorkerAssignmentUpdateEvent => e } shouldBe empty
    }
  }

  "the execution duration handler" should "report the time elapsed so far while running" in {
    withService { f =>
      val startedAgo = 1500L
      val before = System.currentTimeMillis()
      val batches = recordBatches(f.stateStore.statsStore) {
        f.stateStore.statsStore.updateState(
          _.withStartTimeStamp(System.currentTimeMillis() - startedAgo)
        )
      }
      val elapsedDuringUpdate = System.currentTimeMillis() - before

      val durations = batches.flatten.collect { case e: ExecutionDurationUpdateEvent => e }
      durations should have size 1
      durations.head.isRunning shouldBe true
      // The frontend keeps ticking the clock forward from this value, so it is measured
      // against now rather than against the (still unset) end timestamp.
      //
      // Bounded on BOTH sides on purpose. A lower bound alone is satisfied by any arithmetic that
      // grows the number, so `currentTime - startTimeStamp` could become `currentTime +
      // startTimeStamp` -- roughly 111 years -- and still pass.
      durations.head.duration should be >= startedAgo
      durations.head.duration should be <= startedAgo + elapsedDuringUpdate
    }
  }

  it should "report the total duration once the execution has ended" in {
    withService { f =>
      val batches = recordBatches(f.stateStore.statsStore) {
        f.stateStore.statsStore.updateState(_.withStartTimeStamp(1000L).withEndTimeStamp(4200L))
      }

      val durations = batches.flatten.collect { case e: ExecutionDurationUpdateEvent => e }
      durations should have size 1
      durations.head.duration shouldBe 3200L
      durations.head.isRunning shouldBe false
    }
  }

  it should "publish nothing when neither timestamp moved" in {
    withService { f =>
      val batches = recordBatches(f.stateStore.statsStore) {
        f.stateStore.statsStore.updateState(_.withStartTimeStamp(1000L))
        f.client.fire(ExecutionStatsUpdate(Map("opA" -> metrics(RUNNING))))
      }

      batches should have size 2
      batches.head.collect { case e: ExecutionDurationUpdateEvent => e } should have size 1
      batches.last.collect { case e: ExecutionDurationUpdateEvent => e } shouldBe empty
    }
  }

  "the recovery status callback" should "mirror the reported flag onto the metadata store" in {
    withService { f =>
      f.client.fire(WorkflowRecoveryStatus(isRecovering = true))
      f.stateStore.metadataStore.getState.isRecovering shouldBe true

      // Both directions: the frontend leaves its recovery banner up until this flips back.
      f.client.fire(WorkflowRecoveryStatus(isRecovering = false))
      f.stateStore.metadataStore.getState.isRecovering shouldBe false
    }
  }

  "a fatal error" should "stop the client and record the failure against the reporting operator" in {
    withService { f =>
      val cause = new RuntimeException("stats-service-boom")

      f.client.fire(FatalError(cause, Some(ActorVirtualIdentity("Worker:WF1-udf1-main-0"))))

      // The engine is already lost when it reports a fatal error; leaving the client running
      // would keep the execution's actors alive behind a failed workflow.
      f.client.shutdownCount shouldBe 1
      f.stateStore.statsStore.getState.endTimeStamp should be > 0L
      f.stateStore.metadataStore.getState.state shouldBe FAILED

      val errors = f.stateStore.metadataStore.getState.fatalErrors
      errors should have size 1
      errors.head.`type` shouldBe EXECUTION_FAILURE
      // The error panel groups by operator and links to the worker; the two are separate
      // fields derived from the same actor id.
      errors.head.operatorId shouldBe "udf1"
      errors.head.workerId shouldBe "Worker:WF1-udf1-main-0"
      errors.head.message shouldBe cause.toString
      errors.head.details should include("stats-service-boom")
    }
  }

  "the service constructor" should "stamp the runtime statistics URI onto the execution row" in {
    // Spelled out rather than rebuilt from VFSURIFactory: the URI is what the dashboard later
    // opens, so a wid/eid mix-up has to show up as a different string here.
    getDSLContext
      .update(WORKFLOW_EXECUTIONS)
      .setNull(WORKFLOW_EXECUTIONS.RUNTIME_STATS_URI)
      .where(WORKFLOW_EXECUTIONS.EID.eq(testEid))
      .execute()

    withService { _ =>
      getDSLContext
        .select(WORKFLOW_EXECUTIONS.RUNTIME_STATS_URI)
        .from(WORKFLOW_EXECUTIONS)
        .where(WORKFLOW_EXECUTIONS.EID.eq(testEid))
        .fetchOne(WORKFLOW_EXECUTIONS.RUNTIME_STATS_URI) shouldBe
        s"vfs:///wid/$testWid/eid/$testEid/runtimestatistics"
    }
  }

  "the persistence callback" should
    "carry an operator that stopped reporting into the persisted statistics" in {
    withService { f =>
      val opBFinal = metrics(
        COMPLETED,
        inputs = Seq((0, 3L, 30L)),
        outputs = Seq((1, 4L, 40L)),
        numWorkers = 2,
        dataProcessingTime = 7L,
        controlProcessingTime = 8L,
        idleTime = 9L
      )

      f.client.fire(
        RuntimeStatisticsPersist(
          Map("opA" -> metrics(RUNNING, inputs = Seq((0, 1L, 10L))), "opB" -> opBFinal)
        )
      )
      awaitPersist(f.service)
      // opB is gone from the second report, as happens once an operator completes and stops
      // being polled.
      f.client.fire(RuntimeStatisticsPersist(Map("opA" -> metrics(COMPLETED))))
      awaitPersist(f.service)
      f.client.fire(ExecutionStateUpdate(COMPLETED))

      val rows = persistedRows()
      rows.map(_.getField[String]("operatorId")) should
        contain theSameElementsAs Seq("opA", "opA", "opB", "opB")

      // The dashboard replays these rows as a per-operator time series, so an operator that
      // goes quiet has to keep contributing its last known numbers instead of dropping out of
      // the chart at that point.
      val opBRows = rows.filter(_.getField[String]("operatorId") == "opB")
      opBRows should have size 2
      opBRows.foreach { row =>
        row.getField[Long]("inputTupleCnt") shouldBe 3L
        row.getField[Long]("inputTupleSize") shouldBe 30L
        row.getField[Long]("outputTupleCnt") shouldBe 4L
        row.getField[Long]("outputTupleSize") shouldBe 40L
        row.getField[Long]("dataProcessingTime") shouldBe 7L
        row.getField[Long]("controlProcessingTime") shouldBe 8L
        row.getField[Long]("idleTime") shouldBe 9L
        row.getField[Int]("numWorkers") shouldBe 2
        // COMPLETED, as a status code -- the column the dashboard colours the row by.
        row.getField[Int]("status") shouldBe 3
      }
    }
  }

  it should "hold the statistics back until the execution reaches a terminal state" in {
    // All three terminal states are meant to trigger the commit, and an execution reaches only
    // one of them, so each gets its own service -- and with it a fresh statistics table.
    Seq(COMPLETED, FAILED, KILLED).foreach { terminalState =>
      withClue(s"terminal state $terminalState: ") {
        withService { f =>
          f.client.fire(RuntimeStatisticsPersist(Map("opA" -> metrics(RUNNING))))
          awaitPersist(f.service)

          // Mid-execution state changes must not commit: the writer is closed once, and closing
          // it early would strand every later statistics row in the buffer.
          f.client.fire(ExecutionStateUpdate(RUNNING))
          persistedRows() shouldBe empty

          f.client.fire(ExecutionStateUpdate(terminalState))
          persistedRows() should have size 1
        }
      }
    }
  }
}
