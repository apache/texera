package edu.uci.ics.texera

import akka.actor.ActorSystem
import com.twitter.util.{Await, Duration, Promise}
import edu.uci.ics.amber.core.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.core.workflow.{PortIdentity, WorkflowContext, WorkflowSettings}
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, ExecutionStateUpdate, Workflow}
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{EmptyRequest, RetrieveExecStateCLRequest, RetrieveExecStateFlinkAsyncRequest, RetrieveExecStateFlinkRequest, RetrieveExecStatePauseRequest, RetrieveOpStateReverseTopoRequest, RetrieveOpStateViaMarkerRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.COMPLETED
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.operator.{LogicalOp, TestOperators}
import edu.uci.ics.texera.web.model.websocket.request.LogicalPlanPojo
import edu.uci.ics.texera.workflow.{LogicalLink, WorkflowCompiler}

object ExpUtils {
  val workflowContext: WorkflowContext = new WorkflowContext(workflowSettings = WorkflowSettings(1))

  def buildWorkflow(
                     operators: List[LogicalOp],
                     links: List[LogicalLink],
                     context: WorkflowContext
                   ): Workflow = {
    val workflowCompiler = new WorkflowCompiler(
      context
    )
    workflowCompiler.compile(
      LogicalPlanPojo(operators, links, List(), List())
    )
  }

  def executeWorkflowAndCM(workflow: Workflow, system:ActorSystem):(AmberClient, Promise[Unit]) = {
    val client = new AmberClient(
      system,
      workflow.context,
      workflow.physicalPlan,
      ControllerConfig.default,
      error => {}
    )
    val completion = Promise[Unit]()
    client
      .registerCallback[ExecutionStateUpdate](evt => {
        if (evt.state == COMPLETED) {
          completion.setDone()
        }
      })
    Await.result(client.controllerInterface.startWorkflow(EmptyRequest(), ()))
    (client, completion)
  }


  def createWorkflow(numUDF: Int, delay:Int): (List[LogicalOp], Workflow) = {
    val headerlessCsvOpDesc = TestOperators.mediumCsvScanOpDesc()

    // Create a chain of UDF operators
    val udfOps = (1 to numUDF).map(i => {
      val udf = TestOperators.scalaUDFOpDesc(delay)
      udf.operatorId = s"scalaUDF-$i"
      udf
    }).toList

    // Link: CSV -> UDF1 -> UDF2 -> ... -> UDFn
    val allOps = headerlessCsvOpDesc +: udfOps

    val links = (headerlessCsvOpDesc +: udfOps.init).zip(udfOps).map {
      case (srcOp, dstOp) =>
        LogicalLink(
          srcOp.operatorIdentifier,
          PortIdentity(), // default input port
          dstOp.operatorIdentifier,
          PortIdentity()  // default output port
        )
    }

    val workflow = buildWorkflow(
      allOps,
      links,
      workflowContext
    )

    (allOps, workflow)
  }


  def selectTargetOp(opList:List[LogicalOp], indexes:Seq[Int]): Seq[OperatorIdentity] = {
    indexes.map(i => opList(i).operatorIdentifier)
  }

  val scatterReadMapping: Map[Int, Seq[Int]] = Map(
    2 -> Seq(1),
    10 -> Seq(2, 5, 8),
    50 -> Seq(2, 5, 8, 10, 25, 34, 37),
    100 -> Seq(2, 5, 13, 28, 39, 57, 89, 94)
  )


  def runExp(system: ActorSystem, numUDF:Int, isScattered:Boolean, method:String, processingDelayMs:Int): String = {
    val (opList, workflow) = createWorkflow(numUDF, processingDelayMs)
    val (client, promise) = executeWorkflowAndCM(workflow, system)
    val targetOps = if(isScattered){
      selectTargetOp(opList, scatterReadMapping(numUDF))
    }else{
      opList.map(_.operatorIdentifier)
    }
    val target = targetOps.flatMap(t => workflow.physicalPlan.getPhysicalOpsOfLogicalOp(t).map(_.id))
    Thread.sleep(1000)
    val start = System.nanoTime()
    val future = method match{
      case "reverse-topo" =>
        client.controllerInterface.retrieveOpStateReverseTopo(RetrieveOpStateReverseTopoRequest(target), ())
      case "icedtea" =>
        client.controllerInterface.retrieveOpStateViaMarker(RetrieveOpStateViaMarkerRequest(target), ())
      case "CL" =>
        client.controllerInterface.retrieveExecStateCL(RetrieveExecStateCLRequest(target), ())
      case "flink-sync" =>
        client.controllerInterface.retrieveExecStateFlink(RetrieveExecStateFlinkRequest(target), ())
      case "flink-async" =>
        client.controllerInterface.retrieveExecStateFlinkAsync(RetrieveExecStateFlinkAsyncRequest(target), ())
      case "pause" =>
        client.controllerInterface.retrieveExecStatePause(RetrieveExecStatePauseRequest(target), ())
    }
    Await.result(future)
    val end = System.nanoTime()
    val durationMillis = (end - start) / 1_000_000
    client.shutdown()
    promise.setDone()
    Await.result(promise, Duration.fromMinutes(100))
    s"$numUDF-$isScattered-$method: $durationMillis ms"
  }
}
