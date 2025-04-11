package edu.uci.ics.texera

import akka.actor.ActorSystem
import com.twitter.util.Duration.fromMinutes
import com.twitter.util.{Await, Promise}
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType}
import edu.uci.ics.amber.core.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.core.workflow.{PortIdentity, WorkflowContext, WorkflowSettings}
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, ExecutionStateUpdate, Workflow}
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands._
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.COMPLETED
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.operator.TestOperators.{EshopCsvPath, SNLICsvPath, getCsvScanOpDesc}
import edu.uci.ics.amber.operator.udf.scala.ScalaUDFOpDesc
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


  def createWorkflow(numUDF: Int, delay:Int, slow:Int = -1): (List[LogicalOp], Workflow) = {
    val headerlessCsvOpDesc = TestOperators.mediumCsvScanOpDesc()

    // Create a chain of UDF operators
    val udfOps = (1 to numUDF).map(i => {
      val actualDelay = if(slow != -1 && i == (numUDF/2).toInt){
        slow
      }else{
        delay
      }
      val udf = TestOperators.scalaUDFOpDesc(actualDelay)
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

  def createDiamondWorkflow(numBranches: Int, delay: Int, slow:Int = -1): (List[LogicalOp], Workflow) = {
    // Source operator
    val source = TestOperators.mediumCsvScanOpDesc()
    source.operatorId = "Source"

    // Sink operator (e.g., union/collect)
    val sink = TestOperators.scalaUDFOpDesc(0)
    sink.operatorId = "Sink"

    // Middle parallel UDF operators
    val udfOps = (1 to numBranches).map(i => {
      val actualDelay = if(slow != -1 && i == (numBranches/2).toInt){
        slow
      }else{
        delay
      }
      val udf = TestOperators.scalaUDFOpDesc(actualDelay)
      udf.operatorId = s"scalaUDF-$i"
      udf
    }).toList

    // All operators
    val allOps = List(source) ++ udfOps ++ List(sink)

    // Links: source -> all UDFs
    val fanOutLinks = udfOps.map { udf =>
      LogicalLink(
        source.operatorIdentifier,
        PortIdentity(),
        udf.operatorIdentifier,
        PortIdentity()
      )
    }

    // Links: all UDFs -> sink
    val fanInLinks = udfOps.map { udf =>
      LogicalLink(
        udf.operatorIdentifier,
        PortIdentity(),
        sink.operatorIdentifier,
        PortIdentity()
      )
    }

    val links = fanOutLinks ++ fanInLinks

    val workflow = buildWorkflow(allOps, links, workflowContext)
    (allOps, workflow)
  }


  def createECommerceWorkflowWithDJL(): (List[LogicalOp], Workflow) = {
    val sourceOp = getCsvScanOpDesc(EshopCsvPath, header = true)
    sourceOp.customDelimiter = Some(";")
    sourceOp.ingestionDelayInMS = 100
    sourceOp.operatorId = "Source"

    val cr = clickstreamReaderOpDesc()
    cr.operatorId = "Retrieve1"

    val fe = featureExtractorOpDesc()
    fe.operatorId = "FE"

    val rm = rankingModelOpDescWithSmile()
    rm.operatorId = "RM"

    val mo = rankModifierOpDesc()
    mo.operatorId = "Retrieve2"

    val ops = List(sourceOp, cr, fe, rm, mo)

    val links = List(
      LogicalLink(sourceOp.operatorIdentifier, PortIdentity(), cr.operatorIdentifier, PortIdentity()),
      LogicalLink(cr.operatorIdentifier, PortIdentity(), fe.operatorIdentifier, PortIdentity()),
      LogicalLink(fe.operatorIdentifier, PortIdentity(), rm.operatorIdentifier, PortIdentity()),
      LogicalLink(rm.operatorIdentifier, PortIdentity(), mo.operatorIdentifier, PortIdentity())
    )

    val workflow = buildWorkflow(ops, links, workflowContext)

    (ops, workflow)
  }


  def createSNLIWorkflowWithDJL(): (List[LogicalOp], Workflow) = {

    val sourceOp = getCsvScanOpDesc(SNLICsvPath, header = true)
    sourceOp.operatorId = "Source"

    val cr = snliReaderOpDesc()
    cr.operatorId = "Retrieve1"

    val tp = tokenizerPreprocessorOpDesc()
    tp.operatorId = "TP"

    val nli = nliModelOpDescWithDJL()
    nli.operatorId = "Retrieve2"

    val pp = resultPostprocessorOpDesc()
    pp.operatorId = "Retrieve3"

    val ops = List(sourceOp, cr,  tp, nli, pp)

    val links = List(
      LogicalLink(sourceOp.operatorIdentifier, PortIdentity(), cr.operatorIdentifier, PortIdentity()),
      LogicalLink(cr.operatorIdentifier, PortIdentity(), tp.operatorIdentifier, PortIdentity()),
      LogicalLink(tp.operatorIdentifier, PortIdentity(), nli.operatorIdentifier, PortIdentity()),
      LogicalLink(nli.operatorIdentifier, PortIdentity(), pp.operatorIdentifier, PortIdentity())
    )

    val workflow = buildWorkflow(ops, links, workflowContext)

    (ops, workflow)
  }



  def selectTargetOp(opList:List[LogicalOp], indexes:Seq[Int]): Seq[OperatorIdentity] = {
    indexes.map(i => opList(i).operatorIdentifier)
  }

  val scatterReadMapping: Map[Int, Seq[Int]] = Map(
    5 -> Seq(1, 3),
    10 -> Seq(2, 5, 8),
    20 -> Seq(2, 5, 8, 10, 15, 18),
    50 -> Seq(2, 5, 8, 10, 25, 34, 37),
    100 -> Seq(2, 5, 13, 28, 39, 57, 89, 94)
  )


  def applyStateRetrievalMethod(
                                 method: String,
                                 client: AmberClient,
                                 workflow: Workflow,
                                 opList: List[LogicalOp],
                                 isScattered: Boolean,
                                 numUDF: Int = -1
                               ): Long = {
    val targetOps = if (isScattered) {
      if (scatterReadMapping.contains(numUDF)){
        selectTargetOp(opList, scatterReadMapping(numUDF))
      }else{
        opList.filter(x => x.operatorId.startsWith("Retrieve")).map(x => x.operatorIdentifier)
      }
    } else
      opList.map(_.operatorIdentifier)

    val target = targetOps.flatMap(t => workflow.physicalPlan.getPhysicalOpsOfLogicalOp(t).map(_.id))

    Thread.sleep(1000) // give workflow time to settle

    val start = System.nanoTime()
    val future = method match {
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
    (end - start) / 1_000_000 // return ms
  }

  def runExp1(system: ActorSystem, method: String, isScattered: Boolean = false): String = {
    val (opList, workflow) = createECommerceWorkflowWithDJL()
    val (client, promise) = executeWorkflowAndCM(workflow, system)
    var durationMillis = 0L
    if(method != "none"){
      Thread.sleep(10000)
      durationMillis = applyStateRetrievalMethod(method, client, workflow, opList, isScattered)
      client.shutdown()
      promise.setDone()
    }
    Await.result(promise, fromMinutes(100))
    s"exp1-ecommerce-$method: $durationMillis ms"
  }

  def runExp2(system: ActorSystem, method: String, isScattered: Boolean = false): String = {
    val (opList, workflow) = createSNLIWorkflowWithDJL()
    val (client, promise) = executeWorkflowAndCM(workflow, system)

    var durationMillis = 0L
    if(method != "none"){
      Thread.sleep(15000)
      durationMillis = applyStateRetrievalMethod(method, client, workflow, opList, isScattered)
      client.shutdown()
      promise.setDone()
    }
    Await.result(promise, fromMinutes(100))
    s"exp2-snli-$method: $durationMillis ms"
  }

  def runExp3(system: ActorSystem, numUDF: Int, isScattered: Boolean, method: String, processingDelayMs: Int): String = {
    val (opList, workflow) = createWorkflow(numUDF, processingDelayMs)
    val (client, promise) = executeWorkflowAndCM(workflow, system)

    var durationMillis = 0L
    if(method != "none"){
      Thread.sleep(10000)
      durationMillis = applyStateRetrievalMethod(method, client, workflow, opList, isScattered, numUDF)
      client.shutdown()
      promise.setDone()
    }
    Await.result(promise, fromMinutes(100))
    s"$numUDF-$isScattered-$method: $durationMillis ms"
  }

  def runExp5(system: ActorSystem, numUDF: Int, isScattered: Boolean, method: String, processingDelayMs: Int, slow:Int): String = {
    val (opList, workflow) = createWorkflow(numUDF, processingDelayMs, slow)
    val (client, promise) = executeWorkflowAndCM(workflow, system)

    var durationMillis = 0L
    if(method != "none"){
      Thread.sleep(10000)
      durationMillis = applyStateRetrievalMethod(method, client, workflow, opList, isScattered, numUDF)
      client.shutdown()
      promise.setDone()
    }
    Await.result(promise, fromMinutes(100))
    s"$numUDF-$isScattered-$method: $durationMillis ms"
  }

  def runExp6(system: ActorSystem, numUDF: Int, isScattered: Boolean, method: String, processingDelayMs: Int, slow:Int): String = {
    val (opList, workflow) = createDiamondWorkflow(numUDF, processingDelayMs, slow)
    val (client, promise) = executeWorkflowAndCM(workflow, system)

    var durationMillis = 0L
    if(method != "none"){
      Thread.sleep(10000)
      durationMillis = applyStateRetrievalMethod(method, client, workflow, opList, isScattered, numUDF)
      client.shutdown()
      promise.setDone()
    }
    Await.result(promise, fromMinutes(100))
    s"$numUDF-$isScattered-$method: $durationMillis ms"
  }


  def clickstreamReaderOpDesc(): ScalaUDFOpDesc = {
    val udf = new ScalaUDFOpDesc()
    udf.retainInputColumns = true

    udf.code =
      s"""import edu.uci.ics.amber.core.executor.OperatorExecutor;
         |import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike};
         |import scala.Function1;
         |import java.io.BufferedWriter;
         |import java.io.FileWriter;
         |import java.time.LocalDateTime;
         |
         |class ScalaUDFOpExec extends OperatorExecutor {
         |  val writer = new BufferedWriter(new FileWriter("clickstream_log.txt", true))
         |
         |  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
         |    val logLine = LocalDateTime.now().toString+" | "+tuple.toString+"\\n"
         |    writer.write(logLine)
         |    writer.flush()
         |    Iterator(tuple)
         |  }
         |}
         |""".stripMargin

    udf
  }

  def featureExtractorOpDesc(): ScalaUDFOpDesc = {
    val udf = new ScalaUDFOpDesc()
    udf.retainInputColumns = false
    udf.outputColumns = List(
      new Attribute("sessionId", AttributeType.STRING),
      new Attribute("features", AttributeType.ANY)
    )
    udf.code =
      s"""import edu.uci.ics.amber.core.executor.OperatorExecutor;
         |import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike};
         |
         |class ScalaUDFOpExec extends OperatorExecutor {
         |  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
         |    val sessionId = tuple.getField("session ID").toString
         |    val features = Array(
         |      tuple.getField("page 1 (main category)").toString.toDouble,
         |      tuple.getField("colour").toString.toDouble,
         |      tuple.getField("price").toString.toDouble,
         |      tuple.getField("location").toString.toDouble,
         |      tuple.getField("model photography").toString.toDouble
         |    )
         |    Iterator(TupleLike("sessionId" -> sessionId, "features" -> features))
         |  }
         |}
         |""".stripMargin

    udf
  }

  def rankingModelOpDescWithSmile(): ScalaUDFOpDesc = {
    val udf = new ScalaUDFOpDesc()
    udf.retainInputColumns = false
    udf.outputColumns = List(
      new Attribute("sessionId", AttributeType.STRING),
      new Attribute("item", AttributeType.STRING),
      new Attribute("score", AttributeType.DOUBLE)
    )
    udf.code =
      s"""import edu.uci.ics.amber.core.executor.OperatorExecutor
         |import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
         |import smile.classification.KNN
         |
         |class ScalaUDFOpExec extends OperatorExecutor {
         |
         |  var model: KNN[Array[Double]] = _
         |
         |  override def open(): Unit = {
         |    // Dummy training data: features + binary label (liked or not)
         |    val x = Array(
         |      Array(0.9, 1.0, 0.8, 0.7, 0.6),
         |      Array(0.8, 0.9, 0.7, 0.7, 0.6),
         |      Array(0.1, 0.3, 0.2, 0.7, 0.6),
         |      Array(0.2, 0.1, 0.4, 0.7, 0.6)
         |    )
         |    val y = Array(1, 1, 0, 0)
         |
         |    model = KNN.fit(x,y)
         |  }
         |
         |  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
         |    val sessionId = tuple.getField("sessionId").toString
         |    val features = tuple.getField("features").asInstanceOf[Array[Double]]
         |
         |    // Simulated candidate items (perturb features slightly per item)
         |    val items = Array("itemA", "itemB", "itemC")
         |    val scoredItems = items.zipWithIndex.map { case (item, i) =>
         |      val perturbed = features.map(f => f + i * 0.05)
         |      val score = model.predict(perturbed).toDouble + scala.util.Random.nextDouble() * 0.1
         |      (item, score)
         |    }.sortBy(-_._2).take(3)
         |
         |    scoredItems.iterator.map { case (item, score) =>
         |      TupleLike(
         |        "sessionId" -> sessionId,
         |        "item" -> item,
         |        "score" -> score
         |      )
         |    }.toList.iterator
         |  }
         |
         |  override def close(): Unit = {
         |    model = null
         |  }
         |}
         |""".stripMargin

    udf
  }



  def rankModifierOpDesc(): ScalaUDFOpDesc = {
    val udf = new ScalaUDFOpDesc()
    udf.retainInputColumns = false
    udf.outputColumns = List(
      new Attribute("item", AttributeType.STRING),
      new Attribute("score", AttributeType.DOUBLE),
      new Attribute("sponsored", AttributeType.BOOLEAN),
      new Attribute("popular", AttributeType.BOOLEAN)
    )

    udf.code =
      s"""import edu.uci.ics.amber.core.executor.OperatorExecutor;
         |import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike};
         |import scala.io.Source
         |import java.io.File
         |
         |class ScalaUDFOpExec extends OperatorExecutor {
         |  def readPopularItems(path: String): Set[String] = {
         |    try {
         |      val lines = Source.fromFile(new File(path)).getLines().toList
         |      val items = lines.flatMap(_.split(",").find(_.contains("page 2 (clothing model) ->"))
         |        .map(_.split("->")(1).trim))
         |      items.groupBy(identity).mapValues(_.size).toSeq.sortBy(-_._2).take(3).map(_._1).toSet
         |    } catch {
         |      case _: Throwable => Set.empty
         |    }
         |  }
         |
         |  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
         |    val item = tuple.getField("item").toString
         |    val score = tuple.getField("score").asInstanceOf[Double]
         |    val isSponsored = item.startsWith("itemA")
         |    val popularItems = readPopularItems("clickstream_log.txt")
         |    val isPopular = popularItems.contains(item)
         |    val newScore = score + (if (isSponsored) 0.1 else 0.0) + (if (isPopular) 0.2 else 0.0)
         |    val out = Map(
         |      "item" -> item,
         |      "score" -> newScore,
         |      "sponsored" -> isSponsored,
         |      "popular" -> isPopular
         |    )
         |    Iterator(TupleLike(out))
         |  }
         |}
         |""".stripMargin

    udf
  }

  def snliReaderOpDesc(): ScalaUDFOpDesc = {
    val udf = new ScalaUDFOpDesc()
    udf.retainInputColumns = true
    udf.code =
      s"""import edu.uci.ics.amber.core.executor.OperatorExecutor;
         |import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike};
         |
         |class ScalaUDFOpExec extends OperatorExecutor {
         |  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
         |    // Just emit the tuple — data is already loaded
         |
         |    Iterator(tuple)
         |  }
         |}
         |""".stripMargin
    udf
  }


  def tokenizerPreprocessorOpDesc(): ScalaUDFOpDesc = {
    val udf = new ScalaUDFOpDesc()
    udf.retainInputColumns = false
    udf.outputColumns = List(
      new Attribute("premise", AttributeType.STRING),
      new Attribute("hypothesis", AttributeType.STRING),
      new Attribute("features", AttributeType.ANY), // Array[Double]
      new Attribute("label", AttributeType.STRING)
    )

    udf.code =
      s"""import edu.uci.ics.amber.core.executor.OperatorExecutor;
         |import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike};
         |
         |class ScalaUDFOpExec extends OperatorExecutor {
         |  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
         |    val premise = tuple.getField("sentence1").toString
         |    val hypothesis = tuple.getField("sentence2").toString
         |    val label = tuple.getField("gold_label").toString
         |
         |    val tokensPrem = premise.split(" ").length
         |    val tokensHypo = hypothesis.split(" ").length
         |    val features = Array(tokensPrem.toDouble, tokensHypo.toDouble)
         |
         |    Iterator(TupleLike(
         |      "premise" -> premise,
         |      "hypothesis" -> hypothesis,
         |      "features" -> features,
         |      "label" -> label
         |    ))
         |  }
         |}
         |""".stripMargin

    udf
  }



  def resultPostprocessorOpDesc(): ScalaUDFOpDesc = {
    val udf = new ScalaUDFOpDesc()
    udf.retainInputColumns = false
    udf.outputColumns = List(
      new Attribute("premise", AttributeType.STRING),
      new Attribute("hypothesis", AttributeType.STRING),
      new Attribute("predicted", AttributeType.STRING),
      new Attribute("label", AttributeType.STRING),
      new Attribute("correct", AttributeType.BOOLEAN)
    )

    udf.code =
      s"""import edu.uci.ics.amber.core.executor.OperatorExecutor;
         |import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike};
         |
         |class ScalaUDFOpExec extends OperatorExecutor {
         |  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
         |    val predicted = tuple.getField("predicted").toString
         |    val label = tuple.getField("label").toString
         |    val correct = if (predicted == label) true else false
         |
         |    Iterator(TupleLike(
         |      "premise" -> tuple.getField("premise"),
         |      "hypothesis" -> tuple.getField("hypothesis"),
         |      "predicted" -> predicted,
         |      "label" -> label,
         |      "correct" -> correct
         |    ))
         |  }
         |}
         |""".stripMargin
    udf
  }

  def nliModelOpDescWithDJL(): ScalaUDFOpDesc = {
    val udf = new ScalaUDFOpDesc()
    udf.retainInputColumns = false
    udf.outputColumns = List(
      new Attribute("premise", AttributeType.STRING),
      new Attribute("hypothesis", AttributeType.STRING),
      new Attribute("predicted", AttributeType.STRING),
      new Attribute("label", AttributeType.STRING)
    )

    udf.code =
      s"""import edu.uci.ics.amber.core.executor.OperatorExecutor
         |import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
         |
         |import ai.djl.Model
         |import ai.djl.ndarray.{NDArray, NDManager, NDList}
         |import ai.djl.nn.{Activation, SequentialBlock}
         |import ai.djl.nn.core.{Linear}
         |import ai.djl.translate.{Translator, TranslatorContext}
         |import ai.djl.inference.Predictor
         |import ai.djl.ndarray.types.Shape
         |
         |class ScalaUDFOpExec extends OperatorExecutor {
         |  var manager: NDManager = _
         |  var predictor: Predictor[NDArray, NDArray] = _
         |  val labelList = Array("entailment", "neutral", "contradiction")
         |
         |  override def open(): Unit = {
         |    manager = NDManager.newBaseManager()
         |
         |    val block = new SequentialBlock()
         |    for (_ <- 1 to 36) {
         |        block.add(Linear.builder().setUnits(512).build())
         |             .add(Activation.reluBlock())
         |    }
         |    block.add(Linear.builder().setUnits(3).build()) // logits for 3-class output
         |
         |    val model = Model.newInstance("deep-nli-mlp")
         |    model.setBlock(block)
         |    model.getBlock.initialize(manager, ai.djl.ndarray.types.DataType.FLOAT32, new Shape(2)) // features: premise + hypothesis length
         |
         |    val translator = new Translator[NDArray, NDArray] {
         |      override def processInput(ctx: TranslatorContext, input: NDArray): NDList = new NDList(input)
         |      override def processOutput(ctx: TranslatorContext, output: NDList): NDArray = output.singletonOrThrow()
         |      override def getBatchifier = null
         |    }
         |
         |    predictor = model.newPredictor(translator)
         |  }
         |
         |  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
         |    val premise = tuple.getField("premise").toString
         |    val hypothesis = tuple.getField("hypothesis").toString
         |    val label = tuple.getField("label").toString
         |    val features = tuple.getField("features").asInstanceOf[Array[Double]]
         |
         |    val inputArray = manager.create(features.map(_.toFloat))
         |    val prediction = predictor.predict(inputArray)
         |    val probs = prediction.toFloatArray()
         |    val maxIdx = probs.zipWithIndex.maxBy(_._1)._2
         |    val predicted = labelList(maxIdx)
         |
         |    Iterator(TupleLike(
         |      "premise" -> premise,
         |      "hypothesis" -> hypothesis,
         |      "predicted" -> predicted,
         |      "label" -> label
         |    ))
         |  }
         |
         |  override def close(): Unit = {
         |    if (predictor != null) predictor.close()
         |    if (manager != null) manager.close()
         |  }
         |}
         |""".stripMargin

    udf
  }




}
