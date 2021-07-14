package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.ActorContext
import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.messaginglayer.{
  BatchToTupleConverter,
  ControlOutputPort,
  DataOutputPort,
  TupleToBatchConverter
}
import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue._
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryStatisticsHandler.QueryStatistics
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ResumeHandler.ResumeWorker
import edu.uci.ics.amber.engine.common.ambermessage.ControlPayload
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.rpc.{AsyncRPCClient, AsyncRPCServer}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.{Completed, Running}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  LinkIdentity
}
import edu.uci.ics.amber.engine.common.{InputExhausted, WorkflowLogger}
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}

class DataProcessorSpec extends AnyFlatSpec with MockFactory with BeforeAndAfterEach {
  lazy val logger: WorkflowLogger = WorkflowLogger("testDP")
  lazy val pauseManager: PauseManager = wire[PauseManager]
  lazy val dataOutputPort: DataOutputPort = mock[DataOutputPort]
  lazy val batchProducer: TupleToBatchConverter = mock[TupleToBatchConverter]
  lazy val breakpointManager: BreakpointManager = mock[BreakpointManager]
  lazy val controlOutputPort: ControlOutputPort = mock[ControlOutputPort]
  val linkID: LinkIdentity =
    LinkIdentity(
      Option(LayerIdentity("testDP", "mockOp", "src")),
      Option(LayerIdentity("testDP", "mockOp", "dst"))
    )
  val tuples: Seq[ITuple] = (0 until 400).map(ITuple(_))

  def sendDataToDP(dp: DataProcessor, data: Seq[ITuple], interval: Long = -1): Future[_] = {
    Future {
      dp.appendElement(SenderChangeMarker(linkID))
      data.foreach { x =>
        dp.appendElement(InputTuple(x))
        if (interval > 0) {
          Thread.sleep(interval)
        }
      }
      dp.appendElement(EndMarker)
      dp.appendElement(EndOfAllMarker)
    }(ExecutionContext.global)
  }

  def sendControlToDP(
      dp: DataProcessor,
      control: Seq[ControlPayload],
      interval: Long = -1
  ): Future[_] = {
    Future {
      control.foreach { x =>
        dp.enqueueCommand(x, CONTROLLER)
        if (interval > 0) {
          Thread.sleep(interval)
        }
      }
    }(ExecutionContext.global)
  }

  def waitForDataProcessing(
      workerStateManager: WorkerStateManager,
      timeout: FiniteDuration = 5.seconds
  ): Unit = {
    val deadline = timeout.fromNow
    while (deadline.hasTimeLeft() && workerStateManager.getCurrentState != Completed) {
      //wait
    }
    assert(workerStateManager.getCurrentState == Completed)
  }

  def waitForControlProcessing(dp: DataProcessor, timeout: FiniteDuration = 5.seconds): Unit = {
    val deadline = timeout.fromNow
    while (deadline.hasTimeLeft() && !dp.isControlQueueEmpty) {
      //wait
    }
    assert(dp.isControlQueueEmpty)
  }

  case class DummyControl() extends ControlCommand[CommandCompleted]

  "data processor" should "process data messages" in {
    val asyncRPCClient: AsyncRPCClient = mock[AsyncRPCClient]
    val operator = mock[OperatorExecutor]
    val asyncRPCServer: AsyncRPCServer = null
    val workerStateManager: WorkerStateManager = new WorkerStateManager(Running)
    inAnyOrder {
      (batchProducer.emitEndOfUpstream _).expects().anyNumberOfTimes()
      (asyncRPCClient.send[CommandCompleted] _).expects(*, *).anyNumberOfTimes()
      inSequence {
        (operator.open _).expects().once()
        tuples.foreach { x =>
          (operator.processTuple _).expects(Left(x), linkID)
        }
        (operator.processTuple _).expects(Right(InputExhausted()), linkID)
        (operator.close _).expects().once()
      }
    }

    val dp = wire[DataProcessor]
    Await.result(sendDataToDP(dp, tuples), 3.seconds)
    waitForDataProcessing(workerStateManager)
    dp.shutdown()

  }

  "data processor" should "prioritize control messages" in {
    val asyncRPCClient: AsyncRPCClient = mock[AsyncRPCClient]
    val operator = mock[OperatorExecutor]
    val workerStateManager: WorkerStateManager = new WorkerStateManager(Running)
    val asyncRPCServer: AsyncRPCServer = mock[AsyncRPCServer]
    inAnyOrder {
      (asyncRPCServer.logControlInvocation _).expects(*, *).anyNumberOfTimes()
      (asyncRPCClient.send[CommandCompleted] _).expects(*, *).anyNumberOfTimes()
      inSequence {
        (operator.open _).expects().once()
        inAnyOrder {
          tuples.map { x =>
            (operator.processTuple _).expects(Left(x), linkID)
          }
          (asyncRPCServer.receive _).expects(*, *).atLeastOnce() //process controls during execution
        }
        (operator.processTuple _).expects(Right(InputExhausted()), linkID)
        (asyncRPCServer.receive _)
          .expects(*, *)
          .anyNumberOfTimes() // process controls before execution completes
        (batchProducer.emitEndOfUpstream _).expects().once()
        (asyncRPCServer.receive _)
          .expects(*, *)
          .anyNumberOfTimes() // process controls after execution
        (operator.close _).expects().once()
      }
    }
    val dp: DataProcessor = wire[DataProcessor]
    val f1 = sendDataToDP(dp, tuples, 2)
    val f2 = sendControlToDP(dp, (0 until 100).map(_ => ControlInvocation(0, DummyControl())), 3)
    Await.result(f1.zip(f2), 5.seconds)
    waitForDataProcessing(workerStateManager)
    waitForControlProcessing(dp)
    dp.shutdown()
  }

  "data processor" should "process control command without inputting data" in {
    val asyncRPCClient: AsyncRPCClient = mock[AsyncRPCClient]
    val operator = mock[OperatorExecutor]
    val workerStateManager: WorkerStateManager = new WorkerStateManager(Running)
    val asyncRPCServer: AsyncRPCServer = mock[AsyncRPCServer]
    inAnyOrder {
      (operator.open _).expects().once()
      (asyncRPCServer.logControlInvocation _).expects(*, *).anyNumberOfTimes()
      (asyncRPCClient.send[CommandCompleted] _).expects(*, *).anyNumberOfTimes()
      (asyncRPCServer.receive _).expects(*, *).repeat(3)
      (operator.close _).expects().once()
    }
    val dp = wire[DataProcessor]
    Await.result(
      sendControlToDP(dp, (0 until 3).map(_ => ControlInvocation(0, DummyControl()))),
      1.second
    )
    waitForControlProcessing(dp)
    dp.shutdown()
  }

  "data processor" should "process only control commands while paused" in {
    val id = ActorVirtualIdentity("test")
    val operator = mock[OperatorExecutor]
    (operator.open _).expects().once()
    val ctx: ActorContext = null
    val batchToTupleConverter = mock[BatchToTupleConverter]
    val asyncRPCClient: AsyncRPCClient = mock[AsyncRPCClient]
    (asyncRPCClient.send _).expects(*, *).anyNumberOfTimes()
    val asyncRPCServer: AsyncRPCServer = wire[AsyncRPCServer]
    val workerStateManager: WorkerStateManager = new WorkerStateManager(Running)
    val dp: DataProcessor = wire[DataProcessor]
    val handlerInitializer = wire[WorkerAsyncRPCHandlerInitializer]
    inSequence {
      (operator.processTuple _).expects(*, *).once()
      (controlOutputPort.sendTo _).expects(*, *).repeat(4)
      (operator.processTuple _).expects(*, *).repeat(4)
      (batchProducer.emitEndOfUpstream _).expects().once()
      (operator.close _).expects().once()
    }
    dp.appendElement(InputTuple(ITuple(1)))
    Thread.sleep(500)
    dp.enqueueCommand(ControlInvocation(0, PauseWorker()), CONTROLLER)
    dp.appendElement(InputTuple(ITuple(2)))
    dp.enqueueCommand(ControlInvocation(1, QueryStatistics()), CONTROLLER)
    Thread.sleep(1000)
    dp.appendElement(InputTuple(ITuple(3)))
    dp.enqueueCommand(ControlInvocation(2, QueryStatistics()), CONTROLLER)
    dp.appendElement(InputTuple(ITuple(4)))
    dp.enqueueCommand(ControlInvocation(3, ResumeWorker()), CONTROLLER)
    dp.appendElement(EndMarker)
    dp.appendElement(EndOfAllMarker)
    waitForControlProcessing(dp)
    waitForDataProcessing(workerStateManager)
    dp.shutdown()

  }

}
