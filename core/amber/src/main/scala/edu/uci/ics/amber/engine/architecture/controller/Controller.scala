package edu.uci.ics.amber.engine.architecture.controller

import akka.actor.{Address, Cancellable, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.softwaremill.macwire.wire
import edu.uci.ics.amber.clustering.ClusterListener.GetAvailableNodeAddresses
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.controller.Controller.recoveryDelay
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowRecoveryStatus
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage
import edu.uci.ics.amber.engine.architecture.logging.{
  DeterminantLogger,
  InMemDeterminant,
  ProcessControlMessage
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  NetworkMessage,
  NetworkSenderActorRef,
  RegisterActorRef
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkInputPort
import edu.uci.ics.amber.engine.architecture.recovery.GlobalRecoveryManager
import edu.uci.ics.amber.engine.architecture.scheduling.WorkflowScheduler
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker
import edu.uci.ics.amber.engine.common.{AmberUtils, Constants}
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.ambermessage._
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnInvocation}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.{CLIENT, CONTROLLER}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

object ControllerConfig {
  def default: ControllerConfig =
    ControllerConfig(
      monitoringIntervalMs = Option(Constants.monitoringIntervalInMs),
      skewDetectionIntervalMs = Option(Constants.reshapeSkewDetectionIntervalInMs),
      statusUpdateIntervalMs =
        Option(AmberUtils.amberConfig.getLong("constants.status-update-interval")),
      AmberUtils.amberConfig.getBoolean("fault-tolerance.enable-determinant-logging")
    )
}

final case class ControllerConfig(
    monitoringIntervalMs: Option[Long],
    skewDetectionIntervalMs: Option[Long],
    statusUpdateIntervalMs: Option[Long],
    var supportFaultTolerance: Boolean
)

object Controller {

  val recoveryDelay: Long = AmberUtils.amberConfig.getLong("fault-tolerance.delay-before-recovery")

  def props(
      workflow: Workflow,
      controllerConfig: ControllerConfig = ControllerConfig.default,
      parentNetworkCommunicationActorRef: NetworkSenderActorRef = NetworkSenderActorRef()
  ): Props =
    Props(
      new Controller(
        workflow,
        controllerConfig,
        parentNetworkCommunicationActorRef
      )
    )
}

class Controller(
    val workflow: Workflow,
    val controllerConfig: ControllerConfig,
    parentNetworkCommunicationActorRef: NetworkSenderActorRef
) extends WorkflowActor(
      CONTROLLER,
      parentNetworkCommunicationActorRef,
      controllerConfig.supportFaultTolerance
    ) {
  lazy val controlInputPort: NetworkInputPort[ControlPayload] =
    new NetworkInputPort[ControlPayload](this.actorId, this.handleControlPayload)
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout: Timeout = 5.seconds
  var statusUpdateAskHandle: Cancellable = _

  override def getLogName: String = "WF" + workflow.getWorkflowId().id + "-CONTROLLER"

  val determinantLogger: DeterminantLogger = logManager.getDeterminantLogger
  val controlMessagesToRecover: Iterator[InMemDeterminant] =
    logStorage.getReader.mkLogRecordIterator()
  val globalRecoveryManager: GlobalRecoveryManager = new GlobalRecoveryManager(
    () => {
      logger.info("Start global recovery")
      asyncRPCClient.sendToClient(WorkflowRecoveryStatus(true))
    },
    () => {
      logger.info("global recovery complete!")
      asyncRPCClient.sendToClient(WorkflowRecoveryStatus(false))
    }
  )

  def availableNodes: Array[Address] =
    Await
      .result(context.actorSelection("/user/cluster-info") ? GetAvailableNodeAddresses, 5.seconds)
      .asInstanceOf[Array[Address]]

  val workflowScheduler =
    new WorkflowScheduler(
      availableNodes,
      networkCommunicationActor,
      context,
      asyncRPCClient,
      logger,
      workflow,
      controllerConfig
    )

  val rpcHandlerInitializer: ControllerAsyncRPCHandlerInitializer =
    wire[ControllerAsyncRPCHandlerInitializer]

  // register controller itself and client
  networkCommunicationActor.waitUntil(RegisterActorRef(CONTROLLER, self))
  networkCommunicationActor.waitUntil(RegisterActorRef(CLIENT, context.parent))

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preRestart(reason, message)
    logger.error(s"Encountered fatal error, controller is shutting done.", reason)
    // report error to frontend
    asyncRPCClient.sendToClient(FatalError(reason))
  }

  def running: Receive = {
    forwardResendRequest orElse acceptRecoveryMessages orElse acceptDirectInvocations orElse {
      case NetworkMessage(id, WorkflowControlMessage(from, seqNum, payload)) =>
        controlInputPort.handleMessage(
          this.sender(),
          Constants.unprocessedBatchesSizeLimitInBytesPerWorkerPair, // Controller is assumed to have enough credits
          id,
          from,
          seqNum,
          payload
        )
      case other =>
        logger.info(s"unhandled message: $other")
    }
  }

  def acceptDirectInvocations: Receive = {
    case invocation: ControlInvocation =>
      this.handleControlPayload(CLIENT, invocation)
  }

  def acceptRecoveryMessages: Receive = {
    case recoveryMsg: WorkflowRecoveryMessage =>
      recoveryMsg.payload match {
        case UpdateRecoveryStatus(isRecovering) =>
          logger.info("recovery status for " + recoveryMsg.from + " is " + isRecovering)
          globalRecoveryManager.markRecoveryStatus(recoveryMsg.from, isRecovering)
        case ResendOutputTo(vid, ref) =>
          logger.warn(s"controller should not resend output to " + vid)
        case NotifyFailedNode(addr) =>
          if (!controllerConfig.supportFaultTolerance) {
            // do not support recovery
            throw new RuntimeException("Recovery not supported, abort.")
          }
          val deployNodes = availableNodes.filter(_ != self.path.address)
          if (deployNodes.isEmpty) {
            val error = new RuntimeException(
              "Cannot recover failed workers! No available worker machines!"
            )
            asyncRPCClient.sendToClient(FatalError(error))
            throw error
          }
          logger.info(
            "Global Recovery: move all worker from " + addr + " to " + deployNodes.head
          )
          val infoIter = workflow.getAllWorkerInfoOfAddress(addr)
          logger.info("Global Recovery: sent kill signal to workers on failed node")
          infoIter.foreach { info =>
            info.ref ! PoisonPill // in case we can still access the worker
          }
          globalRecoveryManager.markRecoveryStatus(CONTROLLER, true)
          logger.info("Global Recovery: triggering worker respawn")
          infoIter.foreach { info =>
            val ref = workflow.getOperator(info.id).recover(info.id, deployNodes.head, context)
            logger.info("Global Recovery: respawn " + info.id)
            val vidSet = infoIter.map(_.id).toSet
            // wait for some secs to re-send output
            logger.info("Global Recovery: triggering upstream resend for " + info.id)
            workflow
              .getDirectUpstreamWorkers(info.id)
              .filter(x => !vidSet.contains(x))
              .foreach { vid =>
                logger.info("Global Recovery: trigger resend from " + vid + " to " + info.id)
                workflow.getWorkerInfo(vid).ref ! ResendOutputTo(info.id, ref)
              }
            // let controller resend control messages immediately
            networkCommunicationActor ! ResendOutputTo(info.id, ref)
            Thread.sleep(recoveryDelay)
            globalRecoveryManager.markRecoveryStatus(CONTROLLER, false)
          }
      }
  }

  def handleControlPayload(
      from: ActorVirtualIdentity,
      controlPayload: ControlPayload
  ): Unit = {
    determinantLogger.logDeterminant(ProcessControlMessage(controlPayload, from))
    controlPayload match {
      // use control input port to pass control messages
      case invocation: ControlInvocation =>
        assert(from.isInstanceOf[ActorVirtualIdentity])
        asyncRPCServer.logControlInvocation(invocation, from)
        asyncRPCServer.receive(invocation, from)
      case ret: ReturnInvocation =>
        asyncRPCClient.logControlReply(ret, from)
        asyncRPCClient.fulfillPromise(ret)
      case other =>
        throw new WorkflowRuntimeException(s"unhandled control message: $other")
    }
  }

  def recovering: Receive = {
    case d: InMemDeterminant =>
      d match {
        case ProcessControlMessage(controlPayload, from) =>
          handleControlPayload(from, controlPayload)
          if (!controlMessagesToRecover.hasNext) {
            logManager.terminate()
            logStorage.cleanPartiallyWrittenLogFile()
            logManager.setupWriter(logStorage.getWriter)
            globalRecoveryManager.markRecoveryStatus(CONTROLLER, isRecovering = false)
            unstashAll()
            context.become(running)
          } else {
            self ! controlMessagesToRecover.next()
          }
        case otherDeterminant =>
          throw new RuntimeException(
            "Controller cannot handle " + otherDeterminant + " during recovery!"
          )
      }
    case NetworkMessage(
          _,
          WorkflowControlMessage(from, seqNum, ControlInvocation(_, FatalError(err)))
        ) =>
      // fatal error during recovery, fail
      asyncRPCClient.sendToClient(FatalError(err))
      // re-throw the error to fail the actor
      throw err
    case x: NetworkMessage =>
      stash()
    case invocation: ControlInvocation =>
      logger.info("Reject during recovery: " + invocation)
    case other =>
      logger.info("Ignore during recovery: " + other)
  }

  override def receive: Receive = {
    if (controlMessagesToRecover.hasNext) {
      globalRecoveryManager.markRecoveryStatus(CONTROLLER, isRecovering = true)
      val fifoState = recoveryManager.getFIFOState(logStorage.getReader.mkLogRecordIterator())
      controlInputPort.overwriteFIFOState(fifoState)
      self ! controlMessagesToRecover.next()
      forwardResendRequest orElse acceptRecoveryMessages orElse recovering
    } else {
      running
    }
  }

  override def postStop(): Unit = {
    if (statusUpdateAskHandle != null) {
      statusUpdateAskHandle.cancel()
    }
    logger.info("Controller start to shutdown")
    logManager.terminate()
    if (workflow.isCompleted) {
      workflow.getAllWorkers.foreach { workerId =>
        DeterminantLogStorage
          .getLogStorage(
            controllerConfig.supportFaultTolerance,
            WorkflowWorker.getWorkerLogName(workerId)
          )
          .deleteLog()
      }
      logStorage.deleteLog()
    }
    logger.info("stopped successfully!")
  }
}
