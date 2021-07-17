//package Engine.Architecture.Worker
//
//import Engine.Architecture.Breakpoint.LocalBreakpoint.{
//  ConditionalBreakpoint,
//  CountBreakpoint,
//  ExceptionBreakpoint,
//  LocalBreakpoint
//}
//import Engine.Architecture.SendSemantics.DataTransferPolicy.OneToOnePartitioning
//import Engine.Architecture.SendSemantics.Routees.DirectRoutee
//import Engine.Common.AmberMessage.ControlMessage.{
//  LocalBreakpointTriggered,
//  Pause,
//  QueryState,
//  Resume,
//  Start
//}
//import Engine.Common.AmberMessage.WorkerMessage.{
//  AckedWorkerInitialization,
//  AssignBreakpoint,
//  DataMessage,
//  EndSending,
//  QueryTriggeredBreakpoints,
//  RemoveBreakpoint,
//  ReportState,
//  ReportedTriggeredBreakpoints,
//  UpdateOutputLinking
//}
//import Engine.Common.AmberTag.{LayerTag, LinkTag, OperatorTag, WorkerTag, WorkflowTag}
//import akka.actor.{ActorSystem, PoisonPill}
//import akka.testkit.{ImplicitSender, TestKit, TestProbe}
//import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
//import akka.pattern.ask
//import akka.util.Timeout
//
//import scala.concurrent.Await
//import scala.concurrent.duration._
//import scala.util.Random
//
//class GeneratorSpec
//    extends TestKit(ActorSystem("GeneratorSpec"))
//    with ImplicitSender
//    with FlatSpecLike
//    with BeforeAndAfterAll {
//
//  val totalTuples = 200000
//  val outputBatchSize = 10
//  val workflowTag = WorkflowTag("sample")
//  var index = 0
//  val opTag: () => OperatorTag = () => { index += 1; OperatorTag(workflowTag, index.toString) }
//  val layerTag: () => LayerTag = () => { index += 1; LayerTag(opTag(), index.toString) }
//  val workerTag: () => WorkerTag = () => { index += 1; WorkerTag(layerTag(), index) }
//  val linkTag: () => LinkTag = () => { LinkTag(layerTag(), layerTag()) }
//  implicit val timeout: Timeout = Timeout(5.seconds)
//
//  def resultValidation(expectedTupleCount: Int, idleTime: Duration = 2.seconds): Unit = {
//    var counter = 0
//    var receivedEnd = false
//    receiveWhile(5.minutes, idleTime) {
//      case DataMessage(seq, payload) => counter += payload.length
//      case EndSending(seq)           => receivedEnd = true;
//      case msg                       =>
//    }
//    assert(counter == expectedTupleCount)
//    assert(receivedEnd)
//  }
//
//  override def afterAll: Unit = {
//    TestKit.shutdownActorSystem(system)
//  }
//
//  "A Generator" should "generate data messages and send out with random pause/resume" in {
//    val execActor =
//      system.actorOf(Generator.props(new SimpleTupleProducer(totalTuples), workerTag()))
//    execActor ? AckedWorkerInitialization
//    val output = new OneToOnePartitioning(1)
//    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
//    val start = System.nanoTime()
//    execActor ? Start
//    val generator = new Random()
//    for (i <- 0 until 100) {
//      if (generator.nextBoolean()) {
//        execActor ! Pause
//      } else {
//        execActor ! Resume
//      }
//    }
//    execActor ! Resume
//    resultValidation(totalTuples)
//    execActor ! PoisonPill
//  }
//
//  "A Generator" should "generate data messages and send out with pause/resume" in {
//    val execActor =
//      system.actorOf(Generator.props(new SimpleTupleProducer(totalTuples), workerTag()))
//    execActor ? AckedWorkerInitialization
//    val output = new OneToOnePartitioning(1)
//    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
//    val start = System.nanoTime()
//    execActor ? Start
//    for (i <- 0 until 100) {
//      execActor ! Pause
//      execActor ! Resume
//    }
//    resultValidation(totalTuples)
//    execActor ! PoisonPill
//  }
//
//  "A Generator" should "generate data messages and send out" in {
//    val execActor =
//      system.actorOf(Generator.props(new SimpleTupleProducer(totalTuples), workerTag()))
//    execActor ? AckedWorkerInitialization
//    val output = new OneToOnePartitioning(1)
//    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
//    val start = System.nanoTime()
//    execActor ? Start
//    resultValidation(totalTuples)
//    execActor ! PoisonPill
//  }
//
//  "A Generator" should "be able to pause before it starts" in {
//    val probe = TestProbe()
//    val execActor =
//      probe.childActorOf(Generator.props(new SimpleTupleProducer(totalTuples), workerTag()))
//    execActor ? AckedWorkerInitialization
//    probe.expectMsg(ReportState(WorkerState.Ready))
//    execActor.tell(Pause, probe.ref)
//    probe.expectMsg(ReportState(WorkerState.Paused))
//    execActor.tell(Resume, probe.ref)
//    probe.expectMsg(ReportState(WorkerState.Ready))
//    val output = new OneToOnePartitioning(1)
//    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
//    val start = System.nanoTime()
//    execActor ? Start
//    resultValidation(totalTuples)
//    execActor ! PoisonPill
//  }
//
//  "A Generator" should "be able to pause but response completed after it completes" in {
//    val probe = TestProbe()
//    val execActor =
//      probe.childActorOf(Generator.props(new SimpleTupleProducer(totalTuples), workerTag()))
//    execActor ? AckedWorkerInitialization
//    probe.expectMsg(ReportState(WorkerState.Ready))
//    val output = new OneToOnePartitioning(1)
//    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
//    val start = System.nanoTime()
//    execActor ? Start
//    probe.expectMsg(ReportState(WorkerState.Running))
//    resultValidation(totalTuples)
//    execActor.tell(Pause, probe.ref)
//    probe.expectMsg(ReportState(WorkerState.Completed))
//    execActor.tell(Resume, probe.ref)
//    probe.expectMsg(ReportState(WorkerState.Completed))
//    execActor ! PoisonPill
//  }
//
//  "A Generator" should "be able to report current state when queried" in {
//    val probe = TestProbe()
//    val execActor =
//      probe.childActorOf(Generator.props(new SimpleTupleProducer(totalTuples), workerTag()))
//    execActor.tell(QueryState, probe.ref)
//    probe.expectMsg(ReportState(WorkerState.Uninitialized))
//    execActor ? AckedWorkerInitialization
//    probe.expectMsg(ReportState(WorkerState.Ready))
//    val output = new OneToOnePartitioning(1)
//    execActor ? UpdateOutputLinking(output, linkTag(), Array(new DirectRoutee(testActor)))
//    val start = System.nanoTime()
//    execActor ? Start
//    probe.expectMsg(ReportState(WorkerState.Running))
//    execActor ! Pause
//    probe.expectMsgAnyOf(ReportState(WorkerState.Paused), ReportState(WorkerState.Pausing))
//    Thread.sleep(1000)
//    execActor.tell(QueryState, probe.ref)
//    probe.expectMsgAnyOf(ReportState(WorkerState.Paused))
//    execActor ! Resume
//    probe.expectMsgAnyOf(ReportState(WorkerState.Running))
//    execActor.tell(QueryState, probe.ref)
//    probe.expectMsgAnyOf(ReportState(WorkerState.Running))
//    execActor.tell(QueryState, probe.ref)
//    probe.expectMsgAnyOf(ReportState(WorkerState.Running))
//    resultValidation(totalTuples)
//    execActor.tell(QueryState, probe.ref)
//    probe.expectMsg(ReportState(WorkerState.Completed))
//    execActor ! PoisonPill
//  }
//
//  "A Generator" should "be able to set a conditional breakpoint and trigger it" in {
//    val probe = TestProbe()
//    val execActor = probe.childActorOf(Generator.props(new SimpleTupleProducer(7), workerTag()))
//    execActor.tell(QueryState, probe.ref)
//    probe.expectMsg(ReportState(WorkerState.Uninitialized))
//    execActor ? AckedWorkerInitialization
//    probe.expectMsg(ReportState(WorkerState.Ready))
//    execActor ? AssignBreakpoint(new ExceptionBreakpoint()("ex", 0))
//    execActor ? AssignBreakpoint(new ConditionalBreakpoint(x => x.getInt(0) >= 5)("cond1", 0))
//    execActor ? Start
//    probe.expectMsg(ReportState(WorkerState.Running))
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
//    probe.expectMsgAnyOf(ReportState(WorkerState.Running))
//    probe.expectMsgAnyOf(ReportState(WorkerState.LocalBreakpointTriggered))
//    val tmp2 = Await
//      .result(execActor ? QueryTriggeredBreakpoints, 5.seconds)
//      .asInstanceOf[ReportedTriggeredBreakpoints]
//      .bps(0)
//    system.log.info(
//      "received bad tuple: {}",
//      tmp2.asInstanceOf[ConditionalBreakpoint].triggeredTuple
//    )
//    execActor ? AssignBreakpoint(new ConditionalBreakpoint(x => x.getInt(0) >= 5)("cond1", 2))
//    probe.expectMsg(ReportState(WorkerState.Paused))
//    execActor ! Resume
//    probe.expectMsgAnyOf(ReportState(WorkerState.Running))
//    probe.expectMsgAnyOf(
//      ReportState(WorkerState.Paused),
//      ReportState(WorkerState.LocalBreakpointTriggered)
//    )
//    val tmp3 = Await
//      .result(execActor ? QueryTriggeredBreakpoints, 5.seconds)
//      .asInstanceOf[ReportedTriggeredBreakpoints]
//      .bps(0)
//    system.log.info(
//      "received bad tuple: {}",
//      tmp3.asInstanceOf[ConditionalBreakpoint].triggeredTuple
//    )
//    execActor ? AssignBreakpoint(new ConditionalBreakpoint(x => x.getInt(0) >= 5)("cond1", 3))
//    probe.expectMsg(ReportState(WorkerState.Paused))
//    execActor ! Resume
//    probe.expectMsgAnyOf(ReportState(WorkerState.Running))
//    probe.expectMsg(ReportState(WorkerState.Completed))
//    execActor ! PoisonPill
//  }
//
//  "A Generator" should "be able to set a count breakpoint and trigger it" in {
//    val probe = TestProbe()
//    val execActor = probe.childActorOf(Generator.props(new SimpleTupleProducer(7), workerTag()))
//    execActor.tell(QueryState, probe.ref)
//    probe.expectMsg(ReportState(WorkerState.Uninitialized))
//    execActor ? AckedWorkerInitialization
//    probe.expectMsg(ReportState(WorkerState.Ready))
//    execActor ? AssignBreakpoint(new ExceptionBreakpoint()("ex", 0))
//    execActor ? AssignBreakpoint(new CountBreakpoint(5)("count1", 0))
//    execActor ? Start
//    probe.expectMsg(ReportState(WorkerState.Running))
//    probe.expectMsgAnyOf(ReportState(WorkerState.LocalBreakpointTriggered))
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
//    probe.expectMsgAnyOf(ReportState(WorkerState.Running))
//    probe.expectMsg(ReportState(WorkerState.Completed))
//    execActor ! PoisonPill
//  }
//
//  "A Generator" should "be able to set a count breakpoint and trigger it on complete" in {
//    val probe = TestProbe()
//    val execActor = probe.childActorOf(Generator.props(new SimpleTupleProducer(50), workerTag()))
//    execActor.tell(QueryState, probe.ref)
//    probe.expectMsg(ReportState(WorkerState.Uninitialized))
//    execActor ? AckedWorkerInitialization
//    probe.expectMsg(ReportState(WorkerState.Ready))
//    execActor ? AssignBreakpoint(new ExceptionBreakpoint()("ex", 0))
//    execActor ? AssignBreakpoint(new CountBreakpoint(50)("count1", 0))
//    execActor ? Start
//    probe.expectMsg(ReportState(WorkerState.Running))
//    probe.expectMsgAnyOf(ReportState(WorkerState.LocalBreakpointTriggered))
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
//    probe.expectMsgAnyOf(ReportState(WorkerState.Running))
//    probe.expectMsg(ReportState(WorkerState.Completed))
//    execActor ! PoisonPill
//  }
//
//}
