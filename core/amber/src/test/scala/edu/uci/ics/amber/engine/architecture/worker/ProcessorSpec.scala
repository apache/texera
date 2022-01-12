//package Engine.Architecture.Worker
//
//import Engine.Architecture.Breakpoint.LocalBreakpoint.{ConditionalBreakpoint, CountBreakpoint, ExceptionBreakpoint}
//import Engine.Architecture.SendSemantics.DataTransferPolicy.OneToOnePartitioning
//import Engine.Architecture.SendSemantics.Routees.DirectRoutee
//import Engine.Common.AmberMessage.ControlMessage.{Pause, QueryState, Resume}
//import Engine.Common.AmberMessage.PrincipalMessage.ReportPrincipalPartialCompleted
//import Engine.Common.AmberMessage.WorkerMessage.{AckedWorkerInitialization, AssignBreakpoint, DataMessage, EndSending, QueryTriggeredBreakpoints, RemoveBreakpoint, ReportState, ReportWorkerPartialCompleted, ReportedTriggeredBreakpoints, UpdateInputLinking, UpdateOutputLinking}
//import Engine.Common.AmberTag.{LayerTag, LinkTag, OperatorTag, WorkerTag, WorkflowTag}
//import Engine.Common.tuple.Tuple
//import Engine.Common.tuple.amber.AmberTuple
//import Engine.Operators.SimpleCollection.SimpleTupleProcessor
//import akka.actor.{ActorSystem, PoisonPill}
//import akka.testkit.{ImplicitSender, TestKit, TestProbe}
//import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
//import akka.pattern.ask
//import akka.util.Timeout
//
//import scala.collection.mutable
//import scala.concurrent.Await
//import scala.concurrent.duration._
//import scala.util.Random
//
//class ProcessorSpec
//    extends TestKit(ActorSystem("ProcessorSpec"))
//    with ImplicitSender
//    with FlatSpecLike
//    with BeforeAndAfterAll {
//
//  val datasetSize = 100000
//  val batchSize = 2
//  val outputBatchSize = 1
//  val workflowTag = WorkflowTag("sample")
//  var index = 0
//  val opTag: () => OperatorTag = () => { index += 1; OperatorTag(workflowTag, index.toString) }
//  val layerTag: () => LayerTag = () => { index += 1; LayerTag(opTag(), index.toString) }
//  val workerTag: () => WorkerTag = () => { index += 1; WorkerTag(layerTag(), index) }
//  val linkTag: () => LinkTag = () => { LinkTag(layerTag(), layerTag()) }
//
//  val testDataset = new Array[Array[Tuple]](datasetSize)
//  val testSequence: mutable.Buffer[Int] = Random.shuffle((0 until datasetSize).toBuffer)
//
//  def resultValidation(expectedTupleCount: Int, idleTime: Duration = 2.seconds): Unit = {
//    var counter = 0
//    var receivedEnd = false
//    receiveWhile(5.minutes, idleTime) {
//      case DataMessage(seq, payload) => counter += payload.length;
//      case EndSending(seq)           => receivedEnd = true;
//      case msg                       =>
//    }
//    assert(counter == expectedTupleCount)
//    assert(receivedEnd)
//  }
//
//  override def beforeAll(): Unit = {
//    var currentNum = 1
//    for (i <- 0 until datasetSize) {
//      testDataset(i) = new Array[Tuple](batchSize)
//      for (j <- 0 until batchSize) {
//        testDataset(i)(j) = new AmberTuple(Array(currentNum))
//        currentNum += 1
//      }
//    }
//  }
//
//  override def afterAll: Unit = {
//    TestKit.shutdownActorSystem(system)
//  }
//
//  "A Processor" should "process pause/resume messages after receiving data messages within reasonable time" in {
//    implicit val timeout = Timeout(5.seconds)
//    val execActor = system.actorOf(Processor.props(new SimpleTupleProcessor, workerTag()))
//    execActor ? AckedWorkerInitialization
//    execActor ? UpdateInputLinking(testActor, null)
//    val output = new OneToOnePartitioning(1)
//    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
//    val start = System.nanoTime()
//    for (i <- 0 until datasetSize) {
//      execActor ! DataMessage(testSequence(i), testDataset(testSequence(i)))
//    }
//    for (i <- 0 until Math.max(datasetSize / 1000, 10)) {
//      execActor ! Pause
//      execActor ! Resume
//    }
//    execActor ! EndSending(datasetSize)
//    resultValidation(datasetSize * batchSize, 30.seconds)
//    Thread.sleep(1000)
//    execActor ! PoisonPill
//  }
//
//  "A Processor" should "process pause/resume messages in random order while receiving data messages within reasonable time" in {
//    implicit val timeout = Timeout(5.seconds)
//    val execActor = system.actorOf(Processor.props(new SimpleTupleProcessor, workerTag()))
//    execActor ? AckedWorkerInitialization
//    execActor ? UpdateInputLinking(testActor, null)
//    val output = new OneToOnePartitioning(1)
//    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
//    val start = System.nanoTime()
//    val generator = new Random()
//    for (i <- 0 until datasetSize) {
//      execActor ! DataMessage(testSequence(i), testDataset(testSequence(i)))
//      if (generator.nextBoolean()) {
//        execActor ! Pause
//      } else {
//        execActor ! Resume
//      }
//    }
//    execActor ! Resume
//    execActor ! EndSending(datasetSize)
//    resultValidation(datasetSize * batchSize, 30.seconds)
//    Thread.sleep(1000)
//    execActor ! PoisonPill
//  }
//
//  "A Processor" should "process data messages and generate output" in {
//    implicit val timeout = Timeout(5.seconds)
//    val execActor = system.actorOf(Processor.props(new SimpleTupleProcessor, workerTag()))
//    execActor ? AckedWorkerInitialization
//    execActor ? UpdateInputLinking(testActor, null)
//    val output = new OneToOnePartitioning(1)
//    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
//    val start = System.nanoTime()
//    var i = 0
//    while (i < datasetSize) {
//      execActor ! DataMessage(testSequence(i), testDataset(testSequence(i)))
//      i += 1
//    }
//    execActor ! EndSending(datasetSize)
//    resultValidation(datasetSize * batchSize)
//    Thread.sleep(3000)
//    execActor ! PoisonPill
//  }
//
//  "A Processor" should "process data messages with pause/resume within reasonable time" in {
//    implicit val timeout = Timeout(5.seconds)
//    val execActor = system.actorOf(Processor.props(new SimpleTupleProcessor, workerTag()))
//    execActor ? AckedWorkerInitialization
//    execActor ? UpdateInputLinking(testActor, null)
//    val output = new OneToOnePartitioning(1)
//    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
//    execActor ! Pause
//    execActor ! Resume
//    val start = System.nanoTime()
//    for (i <- 0 until datasetSize) {
//      execActor ! DataMessage(testSequence(i), testDataset(testSequence(i)))
//      execActor ! Pause
//      execActor ! Resume
//    }
//    execActor ! EndSending(datasetSize)
//    execActor ! Pause
//    execActor ! Resume
//    resultValidation(datasetSize * batchSize, 10.minutes)
//    Thread.sleep(3000)
//    execActor ! Pause
//    execActor ! Resume
//    Thread.sleep(2000)
//    execActor ! PoisonPill
//  }
//
//  "A Processor" should "be able to pause before it starts" in {
//    implicit val timeout = Timeout(5.seconds)
//    val probe = TestProbe()
//    val execActor = probe.childActorOf(Processor.props(new SimpleTupleProcessor, workerTag()))
//    execActor ? AckedWorkerInitialization
//    probe.expectMsg(ReportState(WorkerState.Ready))
//    execActor.tell(Pause, probe.ref)
//    probe.expectMsg(ReportState(WorkerState.Paused))
//    execActor.tell(Resume, probe.ref)
//    probe.expectMsg(ReportState(WorkerState.Ready))
//    execActor ? UpdateInputLinking(testActor, null)
//    val output = new OneToOnePartitioning(1)
//    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
//    val start = System.nanoTime()
//    var i = 0
//    while (i < datasetSize) {
//      execActor ! DataMessage(testSequence(i), testDataset(testSequence(i)))
//      i += 1
//    }
//    execActor ! EndSending(datasetSize)
//    resultValidation(datasetSize * batchSize)
//    Thread.sleep(3000)
//    execActor ! PoisonPill
//  }
//
//  "A Processor" should "be able to pause but reply completed after it completes" in {
//    implicit val timeout = Timeout(5.seconds)
//    val probe = TestProbe()
//    probe.ignoreMsg { case ReportWorkerPartialCompleted(x, y) => true }
//    val execActor = probe.childActorOf(Processor.props(new SimpleTupleProcessor, workerTag()))
//    execActor ? AckedWorkerInitialization
//    probe.expectMsg(ReportState(WorkerState.Ready))
//    execActor ? UpdateInputLinking(testActor, null)
//    val output = new OneToOnePartitioning(1)
//    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
//    val start = System.nanoTime()
//    var i = 0
//    while (i < datasetSize) {
//      execActor ! DataMessage(testSequence(i), testDataset(testSequence(i)))
//      i += 1
//    }
//    execActor ! EndSending(datasetSize)
//    probe.expectMsg(ReportState(WorkerState.Running))
//    resultValidation(datasetSize * batchSize)
//    Thread.sleep(3000)
//    execActor.tell(Pause, probe.ref)
//    probe.expectMsg(ReportState(WorkerState.Completed))
//    execActor.tell(Resume, probe.ref)
//    probe.expectMsg(ReportState(WorkerState.Completed))
//    execActor ! PoisonPill
//  }
//
//  "A Processor" should "be able to report current state when queried" in {
//    implicit val timeout = Timeout(5.seconds)
//    val probe = TestProbe()
//    val execActor = probe.childActorOf(Processor.props(new SimpleTupleProcessor, workerTag()))
//    execActor.tell(QueryState, probe.ref)
//    probe.ignoreMsg { case ReportWorkerPartialCompleted(x, y) => true }
//    probe.expectMsg(ReportState(WorkerState.Uninitialized))
//    execActor ? AckedWorkerInitialization
//    probe.expectMsg(ReportState(WorkerState.Ready))
//    execActor.tell(QueryState, probe.ref)
//    probe.expectMsg(ReportState(WorkerState.Ready))
//    execActor ? UpdateInputLinking(testActor, null)
//    val output = new OneToOnePartitioning(1)
//    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
//    val start = System.nanoTime()
//    var i = 0
//    execActor ! DataMessage(testSequence(i), testDataset(testSequence(i)))
//    i += 1
//    probe.expectMsg(ReportState(WorkerState.Running))
//    while (i < datasetSize) {
//      execActor ! DataMessage(testSequence(i), testDataset(testSequence(i)))
//      i += 1
//    }
//    execActor ! Pause
//    probe.expectMsgAnyOf(ReportState(WorkerState.Paused), ReportState(WorkerState.Pausing))
//    Thread.sleep(1000)
//    execActor.tell(QueryState, probe.ref)
//    probe.expectMsgAnyOf(ReportState(WorkerState.Paused))
//    execActor ! Resume
//    probe.expectMsgAnyOf(ReportState(WorkerState.Running))
//    execActor.tell(QueryState, probe.ref)
//    probe.expectMsgAnyOf(ReportState(WorkerState.Running))
//    execActor ! EndSending(datasetSize)
//    resultValidation(datasetSize * batchSize)
//    Thread.sleep(3000)
//    execActor.tell(QueryState, probe.ref)
//    probe.expectMsg(ReportState(WorkerState.Completed))
//    execActor ! PoisonPill
//  }
//
//  "A Processor" should "be able to set a conditional breakpoint and trigger it" in {
//    implicit val timeout = Timeout(5.seconds)
//    val probe = TestProbe()
//    val execActor = probe.childActorOf(Processor.props(new SimpleTupleProcessor, workerTag()))
//    execActor ? AckedWorkerInitialization
//    probe.ignoreMsg { case ReportWorkerPartialCompleted(x, y) => true }
//    probe.expectMsg(ReportState(WorkerState.Ready))
//    execActor ? UpdateInputLinking(testActor, null)
//    val output = new OneToOnePartitioning(1)
//    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
//    execActor ? AssignBreakpoint(new ExceptionBreakpoint()("ex", 0))
//    execActor ? AssignBreakpoint(new ConditionalBreakpoint(x => x.getInt(0) >= 5)("cond1", 0))
//    execActor ! DataMessage(0, Array(Tuple(1)))
//    probe.expectMsg(ReportState(WorkerState.Running))
//    execActor ! DataMessage(1, Array(Tuple(2)))
//    execActor ! DataMessage(2, Array(Tuple(3)))
//    execActor ! DataMessage(3, Array(Tuple(4)))
//    execActor ! DataMessage(4, Array(Tuple(5)))
//    probe.expectMsg(ReportState(WorkerState.LocalBreakpointTriggered))
//    val tmp = Await
//      .result(execActor ? QueryTriggeredBreakpoints, 5.seconds)
//      .asInstanceOf[ReportedTriggeredBreakpoints]
//      .bps(0)
//    system.log.info(
//      "received bad tuple: {}",
//      tmp.asInstanceOf[ConditionalBreakpoint].triggeredTuple
//    )
//    execActor ? AssignBreakpoint(new ConditionalBreakpoint(x => x.getInt(0) >= 5)("cond1", 1))
//    probe.expectMsg(ReportState(WorkerState.Paused))
//    execActor ! Resume
//    probe.expectMsg(ReportState(WorkerState.Running))
//    execActor ! DataMessage(5, Array(Tuple(6)))
//    probe.expectMsg(ReportState(WorkerState.LocalBreakpointTriggered))
//    val tmp1 = Await
//      .result(execActor ? QueryTriggeredBreakpoints, 5.seconds)
//      .asInstanceOf[ReportedTriggeredBreakpoints]
//      .bps(0)
//    system.log.info(
//      "received bad tuple: {}",
//      tmp1.asInstanceOf[ConditionalBreakpoint].triggeredTuple
//    )
//    execActor ? AssignBreakpoint(new ConditionalBreakpoint(x => x.getInt(0) >= 5)("cond1", 2))
//    probe.expectMsg(ReportState(WorkerState.Paused))
//    execActor ! Resume
//    probe.expectMsgAnyOf(ReportState(WorkerState.Running))
//    execActor ! EndSending(6)
//    probe.expectMsg(ReportState(WorkerState.Completed))
//    execActor ! PoisonPill
//
//  }
//
//  "A Processor" should "be able to set a count breakpoint and trigger it" in {
//    implicit val timeout = Timeout(5.seconds)
//    val probe = TestProbe()
//    val execActor = probe.childActorOf(Processor.props(new SimpleTupleProcessor, workerTag()))
//    execActor ? AckedWorkerInitialization
//    probe.ignoreMsg { case ReportWorkerPartialCompleted(x, y) => true }
//    probe.expectMsg(ReportState(WorkerState.Ready))
//    execActor ? UpdateInputLinking(testActor, null)
//    val output = new OneToOnePartitioning(1)
//    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
//    execActor ? AssignBreakpoint(new ExceptionBreakpoint()("ex", 0))
//    execActor ? AssignBreakpoint(new CountBreakpoint(5)("count1", 0))
//    execActor ! DataMessage(0, Array(Tuple(1)))
//    probe.expectMsg(ReportState(WorkerState.Running))
//    execActor ! DataMessage(1, Array(Tuple(2)))
//    execActor ! DataMessage(2, Array(Tuple(3)))
//    execActor ! DataMessage(3, Array(Tuple(4)))
//    execActor ! DataMessage(4, Array(Tuple(5)))
//    probe.expectMsg(ReportState(WorkerState.LocalBreakpointTriggered))
//    val tmp = Await
//      .result(execActor ? QueryTriggeredBreakpoints, 5.seconds)
//      .asInstanceOf[ReportedTriggeredBreakpoints]
//      .bps(0)
//    system.log.info(
//      "Count current = {}, target = {}",
//      tmp.asInstanceOf[CountBreakpoint].current,
//      tmp.asInstanceOf[CountBreakpoint].target
//    )
//    execActor ? RemoveBreakpoint("count1")
//    probe.expectMsg(ReportState(WorkerState.Paused))
//    execActor ! Resume
//    probe.expectMsg(ReportState(WorkerState.Running))
//    execActor ! EndSending(5)
//    probe.expectMsg(ReportState(WorkerState.Completed))
//    execActor ! PoisonPill
//  }
//
//  "A Processor" should "be able to set a count breakpoint and trigger it on complete" in {
//    implicit val timeout = Timeout(5.seconds)
//    val probe = TestProbe()
//    val execActor = probe.childActorOf(Processor.props(new SimpleTupleProcessor, workerTag()))
//    execActor ? AckedWorkerInitialization
//    probe.ignoreMsg { case ReportWorkerPartialCompleted(x, y) => true }
//    probe.expectMsg(ReportState(WorkerState.Ready))
//    execActor ? UpdateInputLinking(testActor, null)
//    val output = new OneToOnePartitioning(1)
//    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
//    execActor ? AssignBreakpoint(new ExceptionBreakpoint()("ex", 0))
//    execActor ? AssignBreakpoint(new CountBreakpoint(5)("count1", 0))
//    execActor ! DataMessage(0, Array(Tuple(1)))
//    probe.expectMsg(ReportState(WorkerState.Running))
//    execActor ! DataMessage(1, Array(Tuple(2)))
//    execActor ! DataMessage(2, Array(Tuple(3)))
//    execActor ! DataMessage(3, Array(Tuple(4)))
//    execActor ! EndSending(5)
//    execActor ! DataMessage(4, Array(Tuple(5)))
//    probe.expectMsg(ReportState(WorkerState.LocalBreakpointTriggered))
//    val tmp = Await
//      .result(execActor ? QueryTriggeredBreakpoints, 5.seconds)
//      .asInstanceOf[ReportedTriggeredBreakpoints]
//      .bps(0)
//    system.log.info(
//      "Count current = {}, target = {}",
//      tmp.asInstanceOf[CountBreakpoint].current,
//      tmp.asInstanceOf[CountBreakpoint].target
//    )
//    execActor ? RemoveBreakpoint("count1")
//    probe.expectMsg(ReportState(WorkerState.Paused))
//    execActor ! Resume
//    probe.expectMsg(ReportState(WorkerState.Running))
//    probe.expectMsg(ReportState(WorkerState.Completed))
//    execActor ! PoisonPill
//  }
//
//  "A Processor" should "be able to pause/resume with a count breakpoint" in {
//    implicit val timeout = Timeout(5.seconds)
//    val probe = TestProbe()
//    val execActor = probe.childActorOf(Processor.props(new SimpleTupleProcessor, workerTag()))
//    execActor ? AckedWorkerInitialization
//    probe.ignoreMsg { case ReportWorkerPartialCompleted(x, y) => true }
//    probe.expectMsg(ReportState(WorkerState.Ready))
//    execActor ? UpdateInputLinking(testActor, null)
//    val output = new OneToOnePartitioning(1)
//    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
//    execActor ? AssignBreakpoint(new ExceptionBreakpoint()("ex", 0))
//    execActor ? AssignBreakpoint(new CountBreakpoint(5)("count1", 0))
//    execActor ! DataMessage(0, Array(Tuple(1)))
//    probe.expectMsg(ReportState(WorkerState.Running))
//    execActor ! DataMessage(1, Array(Tuple(2)))
//    execActor ! Pause
//    probe.expectMsg(ReportState(WorkerState.Paused))
//    execActor ! Resume
//    probe.expectMsg(ReportState(WorkerState.Running))
//    execActor ! DataMessage(2, Array(Tuple(3)))
//    execActor ! DataMessage(3, Array(Tuple(4)))
//    execActor ! Pause
//    probe.expectMsg(ReportState(WorkerState.Paused))
//    execActor ! Resume
//    probe.expectMsg(ReportState(WorkerState.Running))
//    execActor ! EndSending(5)
//    execActor ! DataMessage(4, Array(Tuple(5)))
//    execActor ! Pause
//    probe.expectMsgPF(1.second) {
//      case ReportState(WorkerState.LocalBreakpointTriggered) =>
//      case ReportState(WorkerState.Paused) =>
//        execActor ! Resume
//        probe.expectMsg(ReportState(WorkerState.Running))
//    }
//    probe.expectMsg(ReportState(WorkerState.LocalBreakpointTriggered))
//    execActor ! Pause
//    probe.expectMsg(ReportState(WorkerState.LocalBreakpointTriggered))
//    val tmp = Await
//      .result(execActor ? QueryTriggeredBreakpoints, 5.seconds)
//      .asInstanceOf[ReportedTriggeredBreakpoints]
//      .bps(0)
//    system.log.info(
//      "Count current = {}, target = {}",
//      tmp.asInstanceOf[CountBreakpoint].current,
//      tmp.asInstanceOf[CountBreakpoint].target
//    )
//    execActor ? RemoveBreakpoint("count1")
//    execActor ! Pause
//    probe.expectMsg(ReportState(WorkerState.Paused))
//    probe.expectMsg(ReportState(WorkerState.Paused))
//    execActor ! Resume
//    probe.expectMsg(ReportState(WorkerState.Running))
//    probe.expectMsg(ReportState(WorkerState.Completed))
//    execActor ! PoisonPill
//  }
//
//}
