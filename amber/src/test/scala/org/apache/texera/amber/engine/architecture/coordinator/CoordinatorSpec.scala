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

package org.apache.texera.amber.engine.architecture.coordinator

import org.apache.pekko.actor.{Actor, ActorSystem, Props}
import org.apache.pekko.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import org.apache.pekko.util.Timeout
import org.apache.texera.amber.clustering.SingleNodeListener
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  EmbeddedControlMessageIdentity
}
import org.apache.texera.amber.core.workflow.{PhysicalPlan, PortIdentity, WorkflowContext}
import org.apache.texera.amber.engine.architecture.common.WorkflowActor.{
  NetworkMessage,
  RegisterActorRef
}
import org.apache.texera.amber.engine.architecture.logreplay.ReplayLogRecord
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.StateRestoreConfig
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.storage.SequentialRecordStorage
import org.apache.texera.amber.engine.common.virtualidentity.util.{CLIENT, COORDINATOR}
import org.apache.texera.amber.engine.common.{CheckpointState, SerializedState}
import org.apache.texera.amber.engine.e2e.TestUtils.buildWorkflow
import org.apache.texera.amber.operator.TestOperators
import org.apache.texera.common.compiler.model.LogicalLink
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import java.net.URI
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

/**
  * Unit coverage for the `Coordinator` actor's recovery, checkpoint-restore and
  * supervision plumbing -- the parts the untagged e2e specs never reach because
  * they only ever run a workflow start-to-finish.
  *
  * The breakages this spec catches:
  *   - losing either `GlobalReplayManager` callback, which is how the client
  *     learns that a workflow entered / left recovery.
  *   - dropping `initState`'s state-restore branch, so a coordinator asked to
  *     recover silently starts clean instead.
  *   - `loadFromCheckpoint` forgetting to install the checkpointed processor, to
  *     re-attach its `@transient` runtime services, or to re-attach
  *     `cp.outputHandler` (which is `@transient` too, so the first send after a
  *     restore would NPE).
  *   - weakening the `AllForOneStrategy`, which is the only place a child's
  *     fatal error is reported to the client.
  *
  * A real `Coordinator` is constructible here: `WorkflowScheduler.updateSchedule`
  * needs no database (`DefaultCostEstimator` swallows an uninitialised
  * `SqlServer`), and `SessionState.getAllSessionStates` is empty in a unit JVM so
  * the region fan-out is a no-op. Replay/checkpoint storage is the in-memory
  * `ram://` VFS also used by `LoggingSpec`, with a distinct folder per case
  * because the VFS manager is a JVM-wide singleton.
  */
class CoordinatorSpec
    extends TestKit(ActorSystem("CoordinatorSpec"))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  implicit val timeout: Timeout = Timeout(5.seconds)
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  private val workerA = ActorVirtualIdentity("Worker:WF1-E1-op-layer-0")
  private val workerB = ActorVirtualIdentity("Worker:WF1-E1-op-layer-1")
  private val replayDestination = EmbeddedControlMessageIdentity("coordinator-spec-replay-to")

  /** A two-operator CSV plan -- the same fixture `WorkflowSchedulerSpec` uses. */
  private def newPhysicalPlan(): (WorkflowContext, PhysicalPlan) = {
    val csvOp = TestOperators.headerlessSmallCsvScanOpDesc()
    val keywordOp = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val workflow = buildWorkflow(
      List(csvOp, keywordOp),
      List(
        LogicalLink(
          csvOp.operatorIdentifier,
          PortIdentity(0),
          keywordOp.operatorIdentifier,
          PortIdentity(0)
        )
      ),
      new WorkflowContext()
    )
    (workflow.context, workflow.physicalPlan)
  }

  private def newCoordinator(
      parent: TestProbe,
      config: CoordinatorConfig,
      name: String
  ): TestActorRef[Coordinator] = {
    val (context, plan) = newPhysicalPlan()
    TestActorRef[Coordinator](Coordinator.props(context, plan, config), parent.ref, name)
  }

  /** Throws on any message, to drive the coordinator's supervisor strategy. */
  private class ExplodingChild extends Actor {
    override def receive: Receive = {
      case _ => throw new IllegalStateException("child exploded")
    }
  }

  // ---------------------------------------------------------------------------
  // handleReplayMessages + the two GlobalReplayManager callbacks (lines 105-113,
  // 187-190)
  // ---------------------------------------------------------------------------

  "Coordinator" should "report recovery start and completion to the client on ReplayStatusUpdate" in {
    val parent = TestProbe()
    val coordinator = newCoordinator(parent, CoordinatorConfig.default, "coordinator-replay-status")
    parent.expectMsgType[RegisterActorRef]

    // First worker entering recovery -> the onStart callback fires.
    coordinator ! ReplayStatusUpdate(workerA, status = true)
    parent.expectMsg(WorkflowRecoveryStatus(true))

    // A second worker joining an already-recovering workflow must not
    // re-announce the start, and the first one clearing must not announce the
    // end while the second is still recovering.
    coordinator ! ReplayStatusUpdate(workerB, status = true)
    coordinator ! ReplayStatusUpdate(workerA, status = false)
    parent.expectNoMessage(200.millis)

    // Last worker out -> the onComplete callback fires.
    coordinator ! ReplayStatusUpdate(workerB, status = false)
    parent.expectMsg(WorkflowRecoveryStatus(false))
  }

  // ---------------------------------------------------------------------------
  // initState's state-restore branch (lines 130-141)
  // ---------------------------------------------------------------------------

  it should "announce recovery around the replay it sets up at startup" in {
    val parent = TestProbe()
    val readFrom = new URI("ram:///coordinator-spec-init-state-restore/")
    // An EMPTY log named after the coordinator. The file itself has to exist:
    // SequentialRecordReader opens it through a `lazy val`, and on a missing
    // file the catch handler re-forces the failed lazy val and throws straight
    // out of the iterator.
    SequentialRecordStorage
      .getStorage[ReplayLogRecord](Some(readFrom))
      .getWriter(COORDINATOR.name)
      .close()

    newCoordinator(
      parent,
      CoordinatorConfig.default.copy(
        stateRestoreConfOpt = Some(StateRestoreConfig(readFrom, replayDestination))
      ),
      "coordinator-init-state-restore"
    )

    // Both callbacks fire inside initState, i.e. before preStart registers the
    // coordinator's own actor ref -- the ordering pins that this ran during
    // startup rather than being triggered by anything the test sent.
    parent.expectMsg(WorkflowRecoveryStatus(true))
    parent.expectMsg(WorkflowRecoveryStatus(false))
    parent.expectMsgType[RegisterActorRef]
  }

  it should "not announce recovery when no state-restore config is given" in {
    val parent = TestProbe()
    newCoordinator(parent, CoordinatorConfig.default, "coordinator-init-state-plain")

    // Only the actor-ref registration; the restore branch stayed shut.
    parent.expectMsgType[RegisterActorRef]
    parent.expectNoMessage(200.millis)
  }

  // ---------------------------------------------------------------------------
  // loadFromCheckpoint (lines 223-251)
  // ---------------------------------------------------------------------------

  it should "install the checkpointed processor, re-attach its services and report stats" in {
    val parent = TestProbe()
    val coordinator =
      newCoordinator(parent, CoordinatorConfig.default, "coordinator-load-checkpoint")
    val actor = coordinator.underlyingActor
    val liveProcessor = actor.cp
    parent.expectMsgType[RegisterActorRef]

    // Give the checkpointed processor output-FIFO state the live one does not
    // have, so "the restored cp came from the checkpoint" is distinguishable
    // from "the restored cp is some fresh processor".
    val markerChannel =
      ChannelIdentity(COORDINATOR, ActorVirtualIdentity("checkpointed-peer"), isControl = true)
    val checkpointed =
      new CoordinatorProcessor(
        new WorkflowContext(),
        CoordinatorConfig.default,
        COORDINATOR,
        _ => ()
      )
    (0 until 3).foreach(_ => checkpointed.outputGateway.getSequenceNumber(markerChannel))
    assert(!liveProcessor.outputGateway.getFIFOState.contains(markerChannel))

    val chkpt = new CheckpointState()
    chkpt.save(SerializedState.CP_STATE_KEY, checkpointed)
    // No un-acked output to resend, and a fresh WorkflowExecution has no running
    // region executions, so the worker-revival loop is a no-op here.
    chkpt.save(SerializedState.OUTPUT_MSG_KEY, Array.empty[WorkflowFIFOMessage])

    actor.loadFromCheckpoint(chkpt)

    assert(actor.cp ne liveProcessor)
    assert(actor.cp.outputGateway.getFIFOState.get(markerChannel).contains(3L))

    // Every service handle on CoordinatorProcessor is `@transient`, so Kryo
    // hands them back null; attachRuntimeServicesToCPState re-wires them to the
    // actor's live instances.
    assert(actor.cp.logManager eq actor.logManager)
    assert(actor.cp.actorService eq actor.actorService)
    assert(actor.cp.transferService eq actor.transferService)
    assert(actor.cp.actorRefService eq actor.actorRefMappingService)

    // `outputHandler` is `@transient` too. Without the
    // `cp.outputHandler = logManager.sendCommitted` re-attach, this very send
    // NPEs on a null handler, so seeing the stats event arrive at the client
    // pins that line.
    parent.fishForMessage(5.seconds) {
      case NetworkMessage(_, WorkflowFIFOMessage(channel, _, _: ExecutionStatsUpdate)) =>
        channel.toWorkerId == CLIENT
      case _ => false
    }
  }

  // ---------------------------------------------------------------------------
  // supervisorStrategy (lines 204-213)
  // ---------------------------------------------------------------------------

  it should "stop a failing child and report the failure to the client as a FatalError" in {
    val parent = TestProbe()
    val coordinator = newCoordinator(parent, CoordinatorConfig.default, "coordinator-supervisor")
    parent.expectMsgType[RegisterActorRef]

    val child = coordinator.underlyingActor.context.actorOf(Props(new ExplodingChild), "boom")
    watch(child)
    child ! "explode"

    // maxNrOfRetries = 0 with Stop: the child is not restarted, it is killed.
    expectTerminated(child, 5.seconds)

    // The child runs on the default dispatcher, so the FatalError arrives
    // asynchronously relative to this thread.
    parent.fishForMessage(5.seconds) {
      case NetworkMessage(_, WorkflowFIFOMessage(_, _, FatalError(e, _))) =>
        e.getMessage == "child exploded"
      case _ => false
    }
  }

  //  private val logicalPlan1 =
  //    """{
  //      |"operators":[
  //      |{"tableName":"D:\\large_input.csv","operatorId":"Scan","operatorType":"LocalScanSource","delimiter":","},
  //      |{"attributeName":0,"keyword":"Asia","operatorId":"KeywordSearch","operatorType":"KeywordMatcher"},
  //      |{"operatorId":"Count","operatorType":"Aggregation"},
  //      |{"operatorId":"Sink","operatorType":"Sink"}],
  //      |"links":[
  //      |{"origin":"Scan","destination":"KeywordSearch"},
  //      |{"origin":"KeywordSearch","destination":"Count"},
  //      |{"origin":"Count","destination":"Sink"}]
  //      |}""".stripMargin
  //
  //  private val logicalPlan2 =
  //    """{
  //      |"operators":[
  //      |{"tableName":"D:\\large_input.csv","operatorId":"Scan","operatorType":"LocalScanSource","delimiter":","},
  //      |{"operatorId":"Count","operatorType":"Aggregation"},
  //      |{"operatorId":"Sink","operatorType":"Sink"}],
  //      |"links":[
  //      |{"origin":"Scan","destination":"Count"},
  //      |{"origin":"Count","destination":"Sink"}]
  //      |}""".stripMargin
  //
  //  private val logicalPlan3 =
  //    """{
  //      |"operators":[
  //      |{"tableName":"D:\\test.txt","operatorId":"Scan","operatorType":"LocalScanSource","delimiter":"|"},
  //      |{"attributeName":15,"keyword":"package","operatorId":"KeywordSearch","operatorType":"KeywordMatcher"},
  //      |{"operatorId":"Count","operatorType":"Aggregation"},
  //      |{"operatorId":"Sink","operatorType":"Sink"}],
  //      |"links":[
  //      |{"origin":"Scan","destination":"KeywordSearch"},
  //      |{"origin":"KeywordSearch","destination":"Count"},
  //      |{"origin":"Count","destination":"Sink"}]
  //      |}""".stripMargin
  //
  //  private val logicalPlan4 =
  //    """{
  //      |"operators":[
  //      |{"tableName":"D:\\test.txt","operatorId":"Scan1","operatorType":"LocalScanSource","delimiter":"|","indicesToKeep":null},
  //      |{"tableName":"D:\\test.txt","operatorId":"Scan2","operatorType":"LocalScanSource","delimiter":"|","indicesToKeep":null},
  //      |{"attributeName":15,"keyword":"package","operatorId":"KeywordSearch","operatorType":"KeywordMatcher"},
  //      |{"operatorId":"Join","operatorType":"HashJoin","innerTableIndex":0,"outerTableIndex":0},
  //      |{"operatorId":"Count","operatorType":"Aggregation"},
  //      |{"operatorId":"Sink","operatorType":"Sink"}],
  //      |"links":[
  //      |{"origin":"Scan1","destination":"KeywordSearch"},
  //      |{"origin":"KeywordSearch","destination":"Join"},
  //      |{"origin":"Scan2","destination":"Join"},
  //      |{"origin":"Join","destination":"Count"},
  //      |{"origin":"Count","destination":"Sink"}]
  //      |}""".stripMargin
  //
  //  "A coordinator" should "be able to set and trigger count breakpoint in the workflow1" in {
  //    val parent = TestProbe()
  //    val coordinator = parent.childActorOf(COORDINATOR.props(logicalPlan1))
  //    coordinator ! AckedCoordinatorInitialization
  //    parent.expectMsg(30.seconds, ReportState(CoordinatorState.Ready))
  //    coordinator ! PassBreakpointTo("KeywordSearch", new CountGlobalBreakpoint("break1", 100000))
  //    coordinator ! Start
  //    parent.expectMsg(ReportState(CoordinatorState.Running))
  //    var isCompleted = false
  //    parent.receiveWhile(30.seconds, 10.seconds) {
  //      case ReportState(CoordinatorState.Paused) =>
  //        coordinator ! Resume
  //      case ReportState(CoordinatorState.Completed) =>
  //        isCompleted = true
  //      case _ =>
  //    }
  //    assert(isCompleted)
  //    parent.ref ! PoisonPill
  //  }
  //
  //  "A coordinator" should "execute the workflow1 normally" in {
  //    val parent = TestProbe()
  //    val coordinator = parent.childActorOf(COORDINATOR.props(logicalPlan1))
  //    coordinator ! AckedCoordinatorInitialization
  //    parent.expectMsg(30.seconds, ReportState(CoordinatorState.Ready))
  //    coordinator ! Start
  //    parent.expectMsg(ReportState(CoordinatorState.Running))
  //    parent.expectMsg(1.minute, ReportState(CoordinatorState.Completed))
  //    parent.ref ! PoisonPill
  //  }
  //
  //  "A coordinator" should "execute the workflow3 normally" in {
  //    val parent = TestProbe()
  //    val coordinator = parent.childActorOf(COORDINATOR.props(logicalPlan3))
  //    coordinator ! AckedCoordinatorInitialization
  //    parent.expectMsg(30.seconds, ReportState(CoordinatorState.Ready))
  //    coordinator ! Start
  //    parent.expectMsg(ReportState(CoordinatorState.Running))
  //    parent.expectMsg(1.minute, ReportState(CoordinatorState.Completed))
  //    parent.ref ! PoisonPill
  //  }
  //
  //  "A coordinator" should "execute the workflow2 normally" in {
  //    val parent = TestProbe()
  //    val coordinator = parent.childActorOf(COORDINATOR.props(logicalPlan2))
  //    coordinator ! AckedCoordinatorInitialization
  //    parent.expectMsg(ReportState(CoordinatorState.Ready))
  //    coordinator ! Start
  //    parent.expectMsg(ReportState(CoordinatorState.Running))
  //    parent.expectMsg(1.minute, ReportState(CoordinatorState.Completed))
  //    parent.ref ! PoisonPill
  //  }
  //
  //  "A coordinator" should "be able to pause/resume the workflow1" in {
  //    val parent = TestProbe()
  //    val coordinator = parent.childActorOf(COORDINATOR.props(logicalPlan1))
  //    coordinator ! AckedCoordinatorInitialization
  //    parent.expectMsg(ReportState(CoordinatorState.Ready))
  //    coordinator ! Start
  //    parent.expectMsg(ReportState(CoordinatorState.Running))
  //    coordinator ! Pause
  //    parent.expectMsg(ReportState(CoordinatorState.Pausing))
  //    parent.expectMsg(ReportState(CoordinatorState.Paused))
  //    coordinator ! Resume
  //    parent.expectMsg(ReportState(CoordinatorState.Resuming))
  //    parent.expectMsg(ReportState(CoordinatorState.Running))
  //    coordinator ! Pause
  //    parent.expectMsg(ReportState(CoordinatorState.Pausing))
  //    parent.expectMsg(ReportState(CoordinatorState.Paused))
  //    coordinator ! Resume
  //    parent.expectMsg(ReportState(CoordinatorState.Resuming))
  //    parent.expectMsg(ReportState(CoordinatorState.Running))
  //    coordinator ! Pause
  //    parent.expectMsg(ReportState(CoordinatorState.Pausing))
  //    parent.expectMsg(ReportState(CoordinatorState.Paused))
  //    coordinator ! Resume
  //    parent.expectMsg(ReportState(CoordinatorState.Resuming))
  //    parent.expectMsg(ReportState(CoordinatorState.Running))
  //    coordinator ! Pause
  //    parent.expectMsg(ReportState(CoordinatorState.Pausing))
  //    parent.expectMsg(ReportState(CoordinatorState.Paused))
  //    coordinator ! Resume
  //    parent.expectMsg(ReportState(CoordinatorState.Resuming))
  //    parent.expectMsg(ReportState(CoordinatorState.Running))
  //    parent.expectMsg(1.minute, ReportState(CoordinatorState.Completed))
  //    parent.ref ! PoisonPill
  //  }

  //  "A coordinator" should "be able to modify the logic after pausing the workflow1" in {
  //    val parent = TestProbe()
  //    val coordinator = parent.childActorOf(COORDINATOR.props(logicalPlan1))
  //    coordinator ! AckedCoordinatorInitialization
  //    parent.expectMsg(30.seconds, ReportState(CoordinatorState.Ready))
  //    coordinator ! Start
  //    parent.expectMsg(ReportState(CoordinatorState.Running))
  //    Thread.sleep(300)
  //    coordinator ! Pause
  //    parent.expectMsg(ReportState(CoordinatorState.Pausing))
  //    parent.expectMsg(ReportState(CoordinatorState.Paused))
  //    coordinator ! ModifyLogic(
  //      new KeywordSearchMetadata(
  //        OperatorTag("sample", "KeywordSearch"),
  //        Constants.currentWorkerNum,
  //        0,
  //        "asia"
  //      )
  //    )
  //    parent.expectMsg(Ack)
  //    Thread.sleep(10000)
  //    coordinator ! Resume
  //    parent.expectMsg(ReportState(CoordinatorState.Resuming))
  //    parent.expectMsg(ReportState(CoordinatorState.Running))
  //    parent.expectMsg(1.minute, ReportState(CoordinatorState.Completed))
  //    parent.ref ! PoisonPill
  //  }

  //  "A coordinator" should "be able to set and trigger conditional breakpoint in the workflow1" in {
  //    val parent = TestProbe()
  //    val coordinator = parent.childActorOf(COORDINATOR.props(logicalPlan1))
  //    coordinator ! AckedCoordinatorInitialization
  //    parent.expectMsg(30.seconds, ReportState(CoordinatorState.Ready))
  //    coordinator ! PassBreakpointTo(
  //      "KeywordSearch",
  //      new ConditionalGlobalBreakpoint("break2", x => x.getString(8).toInt == 9884)
  //    )
  //    coordinator ! Start
  //    parent.expectMsg(ReportState(CoordinatorState.Running))
  //    var isCompleted = false
  //    parent.receiveWhile(30.seconds, 10.seconds) {
  //      case ReportState(CoordinatorState.Paused) =>
  //        coordinator ! Resume
  //      case ReportState(CoordinatorState.Completed) =>
  //        isCompleted = true
  //      case _ =>
  //    }
  //    assert(isCompleted)
  //    parent.ref ! PoisonPill
  //  }
  //
  //  "A coordinator" should "be able to set and trigger count breakpoint on complete in the workflow1" in {
  //    val parent = TestProbe()
  //    val coordinator = parent.childActorOf(COORDINATOR.props(logicalPlan1))
  //    coordinator ! AckedCoordinatorInitialization
  //    parent.expectMsg(30.seconds, ReportState(CoordinatorState.Ready))
  //    coordinator ! PassBreakpointTo("KeywordSearch", new CountGlobalBreakpoint("break1", 146017))
  //    coordinator ! Start
  //    parent.expectMsg(ReportState(CoordinatorState.Running))
  //    var isCompleted = false
  //    parent.receiveWhile(30.seconds, 10.seconds) {
  //      case ReportState(CoordinatorState.Paused) =>
  //        coordinator ! Resume
  //      case ReportState(CoordinatorState.Completed) =>
  //        isCompleted = true
  //      case _ =>
  //    }
  //    assert(isCompleted)
  //    parent.ref ! PoisonPill
  //  }
  //
  //  "A coordinator" should "be able to pause/resume with conditional breakpoint in the workflow1" in {
  //    val parent = TestProbe()
  //    val coordinator = parent.childActorOf(COORDINATOR.props(logicalPlan1))
  //    coordinator ! AckedCoordinatorInitialization
  //    parent.expectMsg(30.seconds, ReportState(CoordinatorState.Ready))
  //    coordinator ! PassBreakpointTo(
  //      "KeywordSearch",
  //      new ConditionalGlobalBreakpoint("break2", x => x.getString(8).toInt == 9884)
  //    )
  //    coordinator ! Start
  //    parent.expectMsg(ReportState(CoordinatorState.Running))
  //    val random = new Random()
  //    for (i <- 0 until 100) {
  //      if (random.nextBoolean()) {
  //        coordinator ! Pause
  //      } else {
  //        coordinator ! Resume
  //      }
  //    }
  //    coordinator ! Resume
  //    var isCompleted = false
  //    parent.receiveWhile(30.seconds, 10.seconds) {
  //      case ReportState(CoordinatorState.Paused) =>
  //        coordinator ! Resume
  //      case ReportState(CoordinatorState.Completed) =>
  //        isCompleted = true
  //      case _ =>
  //    }
  //    assert(isCompleted)
  //    parent.ref ! PoisonPill
  //  }
  //
  //  "A coordinator" should "be able to pause/resume with count breakpoint in the workflow1" in {
  //    val parent = TestProbe()
  //    val coordinator = parent.childActorOf(COORDINATOR.props(logicalPlan1))
  //    coordinator ! AckedCoordinatorInitialization
  //    parent.expectMsg(30.seconds, ReportState(CoordinatorState.Ready))
  //    coordinator ! PassBreakpointTo("KeywordSearch", new CountGlobalBreakpoint("break1", 100000))
  //    coordinator ! Start
  //    parent.expectMsg(ReportState(CoordinatorState.Running))
  //    val random = new Random()
  //    for (i <- 0 until 100) {
  //      if (random.nextBoolean()) {
  //        coordinator ! Pause
  //      } else {
  //        coordinator ! Resume
  //      }
  //    }
  //    coordinator ! Resume
  //    var isCompleted = false
  //    parent.receiveWhile(30.seconds, 10.seconds) {
  //      case ReportState(CoordinatorState.Paused) =>
  //        coordinator ! Resume
  //      case ReportState(CoordinatorState.Completed) =>
  //        isCompleted = true
  //      case _ =>
  //    }
  //    assert(isCompleted)
  //    parent.ref ! PoisonPill
  //  }
  //
  //  "A coordinator" should "execute the workflow4 normally" in {
  //    val parent = TestProbe()
  //    val coordinator = parent.childActorOf(COORDINATOR.props(logicalPlan4))
  //    coordinator ! AckedCoordinatorInitialization
  //    parent.expectMsg(ReportState(CoordinatorState.Ready))
  //    coordinator ! Start
  //    parent.expectMsg(ReportState(CoordinatorState.Running))
  //    parent.expectMsg(1.minute, ReportState(CoordinatorState.Completed))
  //    parent.ref ! PoisonPill
  //  }

}
