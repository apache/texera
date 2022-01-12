//package Engine.Architecture.Principal
//
//import Clustering.SingleNodeListener
//import Engine.Architecture.Breakpoint.GlobalBreakpoint.{
//  ConditionalGlobalBreakpoint,
//  CountGlobalBreakpoint,
//  ExceptionGlobalBreakpoint
//}
//import Engine.Architecture.DeploySemantics.Layer.ActorLayer
//import Engine.Architecture.LinkSemantics.OperatorLink
//import Engine.Architecture.SendSemantics.DataTransferPolicy.{OneToOnePartitioning, RoundRobinPartitioning}
//import Engine.Architecture.SendSemantics.Routees.DirectRoutee
//import Engine.Architecture.Worker.Generator
//import Engine.Common.AmberMessage.ControlMessage.{Pause, Resume, Start}
//import Engine.Common.AmberMessage.ControllerMessage.ReportGlobalBreakpointTriggered
//import Engine.Common.AmberMessage.PrincipalMessage
//import Engine.Common.AmberMessage.PrincipalMessage.{
//  AckedPrincipalInitialization,
//  GetInputLayer,
//  GetOutputLayer,
//  ReportPrincipalPartialCompleted,
//  ReportState
//}
//import Engine.Common.AmberMessage.WorkerMessage.{
//  AckedWorkerInitialization,
//  AssignBreakpoint,
//  DataMessage,
//  EndSending,
//  UpdateInputLinking,
//  UpdateOutputLinking
//}
//import Engine.Common.AmberTag.{LayerTag, LinkTag, OperatorTag, WorkerTag, WorkflowTag}
//import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
//import akka.event.LoggingAdapter
//import akka.testkit.{ImplicitSender, TestKit, TestProbe}
//import akka.util.Timeout
//import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
//import akka.pattern.ask
//
//import scala.concurrent.{Await, ExecutionContextExecutor}
//import scala.concurrent.duration._
//import scala.util.{Random, Success}
//
//class PrincipalSpec
//    extends TestKit(ActorSystem("PrincipalSpec"))
//    with ImplicitSender
//    with FlatSpecLike
//    with BeforeAndAfterAll {
//
//  implicit val timeout: Timeout = Timeout(5.seconds)
//  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
//  implicit val log: LoggingAdapter = system.log
//
//  val workflowTag = WorkflowTag("sample")
//  var index = 0
//  val opTag: () => OperatorTag = () => { index += 1; OperatorTag(workflowTag, index.toString) }
//  val layerTag: () => LayerTag = () => { index += 1; LayerTag(opTag(), index.toString) }
//  val workerTag: () => WorkerTag = () => { index += 1; WorkerTag(layerTag(), index) }
//  val linkTag: () => LinkTag = () => { LinkTag(layerTag(), layerTag()) }
//
//  def resultValidation(expectedTupleCount: Int, idleTime: Duration = 2.seconds): Unit = {
//    var counter = 0
//    var receivedEnd = false
//    receiveWhile(5.minutes, idleTime) {
//      case DataMessage(seq, payload) => counter += payload.length
//      case EndSending(seq)           => receivedEnd = true
//      case msg                       =>
//    }
//    assert(counter == expectedTupleCount)
//    assert(receivedEnd)
//  }
//
//  override def beforeAll: Unit = {
//    system.actorOf(Props[SingleNodeListener], "cluster-info")
//  }
//
//  override def afterAll: Unit = {
//    TestKit.shutdownActorSystem(system)
//  }
//
//  "A Generator Principal" should "generate tuples without problem" in {
//    val parent = TestProbe()
//    ignoreMsg {
//      case UpdateInputLinking(_, _) => true
//    }
//    parent.ignoreMsg {
//      case ReportPrincipalPartialCompleted(x, y) => true
//    }
//    val principal =
//      parent.childActorOf(Principal.props(new SimpleSourceOperatorMetadata(opTag(), 5, 100)))
//    principal ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    val output = Await.result(principal ? GetOutputLayer, timeout.duration).asInstanceOf[ActorLayer]
//    output.layer.foreach(x =>
//      x ! UpdateOutputLinking(new OneToOnePartitioning(1), linkTag(), Array(new DirectRoutee(testActor)))
//    )
//    principal ! Start
//    parent.expectMsg(ReportState(PrincipalState.Running))
//    resultValidation(100)
//    parent.expectMsg(ReportState(PrincipalState.Completed))
//    principal ! PoisonPill
//  }
//
//  "A Processor Principal" should "process and output tuples without problem" in {
//    val parent = TestProbe()
//    ignoreMsg {
//      case UpdateInputLinking(_, _) => true
//    }
//    parent.ignoreMsg {
//      case ReportPrincipalPartialCompleted(x, y) => true
//    }
//    val execActor = system.actorOf(Generator.props(new SimpleTupleProducer(100000), workerTag()))
//    execActor ! AckedWorkerInitialization
//    val principal =
//      parent.childActorOf(Principal.props(new SimpleProcessOperatorMetadata(opTag(), 5)))
//    principal ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    val output = Await.result(principal ? GetOutputLayer, timeout.duration).asInstanceOf[ActorLayer]
//    val input = Await.result(principal ? GetInputLayer, timeout.duration).asInstanceOf[ActorLayer]
//    execActor ! UpdateOutputLinking(
//      new RoundRobinPartitioning(100),
//      linkTag(),
//      output.layer.map(new DirectRoutee(_))
//    )
//    output.layer.foreach(x =>
//      x ! UpdateOutputLinking(new OneToOnePartitioning(1), linkTag(), Array(new DirectRoutee(testActor)))
//    )
//    execActor ! Start
//    parent.expectMsg(ReportState(PrincipalState.Running))
//    resultValidation(100000)
//    parent.expectMsg(ReportState(PrincipalState.Completed))
//    principal ! PoisonPill
//  }
//
//  "A Generator Principal" should "not send any message after receive a pause message when processing" in {
//    val parent = TestProbe()
//    val receiver = TestProbe()
//    receiver.ignoreMsg {
//      case UpdateInputLinking(_, _) => true
//    }
//    parent.ignoreMsg {
//      case ReportPrincipalPartialCompleted(x, y) => true
//    }
//    val principal =
//      parent.childActorOf(Principal.props(new SimpleSourceOperatorMetadata(opTag(), 5, 100000)))
//    principal ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    val output = Await.result(principal ? GetOutputLayer, timeout.duration).asInstanceOf[ActorLayer]
//    output.layer.foreach(x =>
//      x ! UpdateOutputLinking(
//        new OneToOnePartitioning(1),
//        linkTag(),
//        Array(new DirectRoutee(receiver.ref))
//      )
//    )
//    //let receiver ignore all message
//    receiver.ignoreMsg { case _ => true }
//    principal ! Start
//    parent.expectMsg(ReportState(PrincipalState.Running))
//    principal ! Pause
//    parent.expectMsg(ReportState(PrincipalState.Paused))
//    //let receiver receive all message
//    receiver.ignoreMsg { case _ => false }
//    receiver.expectNoMessage(10.seconds)
//    principal ! Resume
//    parent.expectMsg(ReportState(PrincipalState.Running))
//    receiver.expectMsgType[DataMessage](10.seconds)
//    parent.expectMsg(10.seconds, ReportState(PrincipalState.Completed))
//    principal ! PoisonPill
//  }
//
//  "A Generator Principal" should "response correctly after receiving pause/resume when completed" in {
//    val parent = TestProbe()
//    ignoreMsg {
//      case UpdateInputLinking(_, _) => true
//    }
//    parent.ignoreMsg {
//      case ReportPrincipalPartialCompleted(x, y) => true
//    }
//    val principal =
//      parent.childActorOf(Principal.props(new SimpleSourceOperatorMetadata(opTag(), 5, 5)))
//    principal ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    val output = Await.result(principal ? GetOutputLayer, timeout.duration).asInstanceOf[ActorLayer]
//    output.layer.foreach(x =>
//      x ! UpdateOutputLinking(new OneToOnePartitioning(1), linkTag(), Array(new DirectRoutee(testActor)))
//    )
//    principal ! Start
//    parent.expectMsg(ReportState(PrincipalState.Running))
//    resultValidation(5)
//    parent.expectMsg(ReportState(PrincipalState.Completed))
//    principal.tell(Pause, parent.ref)
//    parent.expectMsg(ReportState(PrincipalState.Completed))
//    principal.tell(Resume, parent.ref)
//    parent.expectMsg(ReportState(PrincipalState.Completed))
//    principal ! PoisonPill
//  }
//
//  "A Generator Principal" should "output correct number of tuples after pause/resume" in {
//    val parent = TestProbe()
//    ignoreMsg {
//      case UpdateInputLinking(_, _) => true
//    }
//    parent.ignoreMsg {
//      case ReportPrincipalPartialCompleted(x, y) => true
//    }
//    val principal =
//      parent.childActorOf(Principal.props(new SimpleSourceOperatorMetadata(opTag(), 5, 100)))
//    principal ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    val output = Await.result(principal ? GetOutputLayer, timeout.duration).asInstanceOf[ActorLayer]
//    output.layer.foreach(x =>
//      x ! UpdateOutputLinking(new OneToOnePartitioning(1), linkTag(), Array(new DirectRoutee(testActor)))
//    )
//    principal ! Start
//    parent.expectMsg(ReportState(PrincipalState.Running))
//    for (i <- 0 until 20) {
//      principal ! Pause
//      principal ! Resume
//    }
//    resultValidation(100)
//    principal ! PoisonPill
//  }
//
//  "A Processor Principal" should "not send any message after receive a pause message" in {
//    val parent = TestProbe()
//    val receiver = TestProbe()
//    receiver.ignoreMsg {
//      case UpdateInputLinking(_, _) => true
//    }
//    parent.ignoreMsg {
//      case ReportPrincipalPartialCompleted(x, y) => true
//    }
//    val execActor = system.actorOf(Generator.props(new SimpleTupleProducer(100000), workerTag()))
//    val principal =
//      parent.childActorOf(Principal.props(new SimpleProcessOperatorMetadata(opTag(), 5)))
//    principal ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    val output = Await.result(principal ? GetOutputLayer, timeout.duration).asInstanceOf[ActorLayer]
//    val input = Await.result(principal ? GetInputLayer, timeout.duration).asInstanceOf[ActorLayer]
//    execActor ! UpdateOutputLinking(
//      new RoundRobinPartitioning(100),
//      linkTag(),
//      output.layer.map(new DirectRoutee(_))
//    )
//    //input.layer.foreach(x => x ! UpdateInputLinking(execActor,layerTag()))
//    output.layer.foreach(x =>
//      x ! UpdateOutputLinking(
//        new OneToOnePartitioning(100),
//        linkTag(),
//        Array(new DirectRoutee(receiver.ref))
//      )
//    )
//    //let testActor ignore all message
//    receiver.ignoreMsg { case _ => true }
//    execActor ! AckedWorkerInitialization
//    execActor ! Start
//    parent.expectMsg(ReportState(PrincipalState.Running))
//    principal ! Pause
//    parent.expectMsg(ReportState(PrincipalState.Paused))
//    //let testActor receive all message
//    receiver.ignoreMsg { case _ => false }
//    receiver.expectNoMessage(10.seconds)
//    principal ! Resume
//    parent.expectMsg(ReportState(PrincipalState.Running))
//    receiver.expectMsgType[DataMessage](10.seconds)
//    parent.expectMsg(50.seconds, ReportState(PrincipalState.Completed))
//    principal ! PoisonPill
//  }
//
//  "A Processor Principal" should "output correct result after pause/resume" in {
//    val parent = TestProbe()
//    ignoreMsg {
//      case UpdateInputLinking(_, _) => true
//    }
//    parent.ignoreMsg {
//      case ReportPrincipalPartialCompleted(x, y) => true
//    }
//    val execActor = system.actorOf(Generator.props(new SimpleTupleProducer(100000), workerTag()))
//    val principal =
//      parent.childActorOf(Principal.props(new SimpleProcessOperatorMetadata(opTag(), 5)))
//    principal ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    val output = Await.result(principal ? GetOutputLayer, timeout.duration).asInstanceOf[ActorLayer]
//    val input = Await.result(principal ? GetInputLayer, timeout.duration).asInstanceOf[ActorLayer]
//    execActor ! UpdateOutputLinking(
//      new RoundRobinPartitioning(100),
//      linkTag(),
//      output.layer.map(new DirectRoutee(_))
//    )
//    //input.layer.foreach(x => x ! UpdateInputLinking(execActor,layerTag()))
//    output.layer.foreach(x =>
//      x ! UpdateOutputLinking(
//        new OneToOnePartitioning(100),
//        linkTag(),
//        Array(new DirectRoutee(testActor))
//      )
//    )
//    execActor ! AckedWorkerInitialization
//    execActor ! Start
//    parent.expectMsg(ReportState(PrincipalState.Running))
//    for (i <- 0 until 20) {
//      principal ! Pause
//      principal ! Resume
//    }
//    resultValidation(100000)
//    principal ! PoisonPill
//  }
//
//  "Connected Principals" should "output correct result" in {
//    val parent = TestProbe()
//    ignoreMsg {
//      case UpdateInputLinking(_, _) => true
//    }
//    parent.ignoreMsg {
//      case ReportPrincipalPartialCompleted(x, y) => true
//    }
//    val meta1 = new SimpleSourceOperatorMetadata(opTag(), 5, 100000)
//    val scan = parent.childActorOf(Principal.props(meta1))
//    val meta2 = new SimpleProcessOperatorMetadata(opTag(), 5)
//    val process = parent.childActorOf(Principal.props(meta2))
//    scan ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    process ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    val edge = new OperatorLink((meta1, scan), (meta2, process))
//    edge.link()
//    val output = Await.result(process ? GetOutputLayer, timeout.duration).asInstanceOf[ActorLayer]
//    output.layer.foreach(x =>
//      x ! UpdateOutputLinking(
//        new OneToOnePartitioning(100),
//        linkTag(),
//        Array(new DirectRoutee(testActor))
//      )
//    )
//    scan ! Start
//    parent.expectMsg(ReportState(PrincipalState.Running))
//    resultValidation(100000)
//    parent.ref ! PoisonPill
//  }
//
//  "Connected Principals" should "output correct result with several pause/resume" in {
//    val parent = TestProbe()
//    ignoreMsg {
//      case UpdateInputLinking(_, _) => true
//    }
//    parent.ignoreMsg {
//      case ReportPrincipalPartialCompleted(x, y) => true
//    }
//    val meta1 = new SimpleSourceOperatorMetadata(opTag(), 5, 100000)
//    val scan = parent.childActorOf(Principal.props(meta1))
//    val meta2 = new SimpleProcessOperatorMetadata(opTag(), 5)
//    val process = parent.childActorOf(Principal.props(meta2))
//    scan ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    process ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    val edge = new OperatorLink((meta1, scan), (meta2, process))
//    edge.link()
//    val output = Await.result(process ? GetOutputLayer, timeout.duration).asInstanceOf[ActorLayer]
//    output.layer.foreach(x =>
//      x ! UpdateOutputLinking(
//        new OneToOnePartitioning(100),
//        linkTag(),
//        Array(new DirectRoutee(testActor))
//      )
//    )
//    scan ! Start
//    parent.expectMsg(ReportState(PrincipalState.Running))
//    for (i <- 0 until 20) {
//      scan ! Pause
//      scan ! Resume
//    }
//    for (i <- 0 until 20) {
//      process ! Pause
//      process ! Resume
//    }
//    resultValidation(100000)
//    parent.ref ! PoisonPill
//  }
//
//  "Connected Principals" should "output correct result with random pause/resume" in {
//    val parent = TestProbe()
//    ignoreMsg {
//      case UpdateInputLinking(_, _) => true
//    }
//    parent.ignoreMsg {
//      case ReportPrincipalPartialCompleted(x, y) => true
//    }
//    val meta1 = new SimpleSourceOperatorMetadata(opTag(), 5, 100000)
//    val scan = parent.childActorOf(Principal.props(meta1))
//    val meta2 = new SimpleProcessOperatorMetadata(opTag(), 5)
//    val process = parent.childActorOf(Principal.props(meta2))
//    scan ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    process ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    val edge = new OperatorLink((meta1, scan), (meta2, process))
//    edge.link()
//    val output = Await.result(process ? GetOutputLayer, timeout.duration).asInstanceOf[ActorLayer]
//    output.layer.foreach(x =>
//      x ! UpdateOutputLinking(
//        new OneToOnePartitioning(100),
//        linkTag(),
//        Array(new DirectRoutee(testActor))
//      )
//    )
//    scan ! Start
//    parent.expectMsg(ReportState(PrincipalState.Running))
//    val random = new Random()
//    for (i <- 0 until 100) {
//      if (random.nextBoolean()) {
//        if (random.nextBoolean()) scan ! Pause else scan ! Resume
//      } else {
//        if (random.nextBoolean()) process ! Pause else process ! Resume
//      }
//    }
//    scan ! Resume
//    process ! Resume
//    resultValidation(100000)
//    parent.ref ! PoisonPill
//  }
//
//  "Connected Principals of multiple workers" should "be able to set a count breakpoint and trigger it" in {
//    val parent = TestProbe()
//    ignoreMsg {
//      case UpdateInputLinking(_, _) => true
//    }
//    parent.ignoreMsg {
//      case ReportPrincipalPartialCompleted(x, y) => true
//    }
//    val meta1 = new SimpleSourceOperatorMetadata(opTag(), 5, 100000)
//    val scan = parent.childActorOf(Principal.props(meta1))
//    val meta2 = new SimpleProcessOperatorMetadata(opTag(), 5)
//    val process = parent.childActorOf(Principal.props(meta2))
//    scan ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    process ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    val edge = new OperatorLink((meta1, scan), (meta2, process))
//    edge.link()
//    val output = Await.result(process ? GetOutputLayer, timeout.duration).asInstanceOf[ActorLayer]
//    output.layer.foreach(x =>
//      x ! UpdateOutputLinking(
//        new OneToOnePartitioning(100),
//        linkTag(),
//        Array(new DirectRoutee(testActor))
//      )
//    )
//    scan ? PrincipalMessage.AssignBreakpoint(new ExceptionGlobalBreakpoint("ex1"))
//    scan ? PrincipalMessage.AssignBreakpoint(new CountGlobalBreakpoint("count1", 50000))
//    process ? PrincipalMessage.AssignBreakpoint(new ExceptionGlobalBreakpoint("ex2"))
//    process ? PrincipalMessage.AssignBreakpoint(new CountGlobalBreakpoint("count2", 50000))
//    scan ? Start
//    parent.receiveWhile(10.seconds, 5.seconds) {
//      case ReportState(state) =>
//        println(parent.sender() + " changes to " + state)
//        state match {
//          case PrincipalState.Paused =>
//            parent.sender() ! Resume
//          case _ =>
//        }
//      case ReportGlobalBreakpointTriggered(s, operatorID) =>
//        println(s)
//    }
//    resultValidation(100000, 5.seconds)
//    parent.ref ! PoisonPill
//  }
//
//  "Connected Principals of single worker" should "be able to set a count breakpoint and trigger it" in {
//    val parent = TestProbe()
//    ignoreMsg {
//      case UpdateInputLinking(_, _) => true
//    }
//    parent.ignoreMsg {
//      case ReportPrincipalPartialCompleted(x, y) => true
//    }
//    val meta1 = new SimpleSourceOperatorMetadata(opTag(), 1, 100000)
//    val scan = parent.childActorOf(Principal.props(meta1))
//    val meta2 = new SimpleProcessOperatorMetadata(opTag(), 1)
//    val process = parent.childActorOf(Principal.props(meta2))
//    scan ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    process ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    val edge = new OperatorLink((meta1, scan), (meta2, process))
//    edge.link()
//    val output = Await.result(process ? GetOutputLayer, timeout.duration).asInstanceOf[ActorLayer]
//    output.layer.foreach(x =>
//      x ! UpdateOutputLinking(
//        new OneToOnePartitioning(100),
//        linkTag(),
//        Array(new DirectRoutee(testActor))
//      )
//    )
//    scan ? PrincipalMessage.AssignBreakpoint(new ExceptionGlobalBreakpoint("ex1"))
//    scan ? PrincipalMessage.AssignBreakpoint(new CountGlobalBreakpoint("count1", 50000))
//    process ? PrincipalMessage.AssignBreakpoint(new ExceptionGlobalBreakpoint("ex2"))
//    process ? PrincipalMessage.AssignBreakpoint(new CountGlobalBreakpoint("count2", 50000))
//    scan ? Start
//    parent.receiveWhile(10.seconds, 5.seconds) {
//      case ReportState(state) =>
//        println(parent.sender() + " changes to " + state)
//        state match {
//          case PrincipalState.Paused =>
//            parent.sender() ! Resume
//          case _ =>
//        }
//      case ReportGlobalBreakpointTriggered(s, operatorID) =>
//        println(s)
//    }
//    resultValidation(100000, 30.seconds)
//    parent.ref ! PoisonPill
//  }
//
//  "Connected Principals of multiple workers" should "be able to set a conditional breakpoint and trigger it" in {
//    val parent = TestProbe()
//    ignoreMsg {
//      case UpdateInputLinking(_, _) => true
//    }
//    parent.ignoreMsg {
//      case ReportPrincipalPartialCompleted(x, y) => true
//    }
//    val meta1 = new SimpleSourceOperatorMetadata(opTag(), 5, 10)
//    val scan = parent.childActorOf(Principal.props(meta1))
//    val meta2 = new SimpleProcessOperatorMetadata(opTag(), 5)
//    val process = parent.childActorOf(Principal.props(meta2))
//    scan ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    process ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    val edge = new OperatorLink((meta1, scan), (meta2, process))
//    edge.link()
//    val output = Await.result(process ? GetOutputLayer, timeout.duration).asInstanceOf[ActorLayer]
//    output.layer.foreach(x =>
//      x ! UpdateOutputLinking(
//        new OneToOnePartitioning(100),
//        linkTag(),
//        Array(new DirectRoutee(testActor))
//      )
//    )
//    scan ? PrincipalMessage.AssignBreakpoint(new ExceptionGlobalBreakpoint("ex1"))
//    process ? PrincipalMessage.AssignBreakpoint(new ExceptionGlobalBreakpoint("ex2"))
//    scan ? PrincipalMessage.AssignBreakpoint(
//      new ConditionalGlobalBreakpoint("cond1", x => x.getInt(0) > 1)
//    )
//    process ? PrincipalMessage.AssignBreakpoint(
//      new ConditionalGlobalBreakpoint("cond2", x => x.getInt(0) > 1)
//    )
//    scan ? Start
//    parent.receiveWhile(10.seconds, 5.seconds) {
//      case ReportState(state) =>
//        println(parent.sender() + " changes to " + state)
//        state match {
//          case PrincipalState.Paused =>
//            parent.sender() ! Resume
//          case _ =>
//        }
//      case ReportGlobalBreakpointTriggered(s, operatorID) =>
//        println(s)
//    }
//    resultValidation(5, 30.seconds)
//    parent.ref ! PoisonPill
//  }
//
//  "Connected Principals of single worker" should "be able to set a conditional breakpoint and trigger it" in {
//    val parent = TestProbe()
//    ignoreMsg {
//      case UpdateInputLinking(_, _) => true
//    }
//    parent.ignoreMsg {
//      case ReportPrincipalPartialCompleted(x, y) => true
//    }
//    val meta1 = new SimpleSourceOperatorMetadata(opTag(), 1, 10)
//    val scan = parent.childActorOf(Principal.props(meta1))
//    val meta2 = new SimpleProcessOperatorMetadata(opTag(), 1)
//    val process = parent.childActorOf(Principal.props(meta2))
//    scan ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    process ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    val edge = new OperatorLink((meta1, scan), (meta2, process))
//    edge.link()
//    val output = Await.result(process ? GetOutputLayer, timeout.duration).asInstanceOf[ActorLayer]
//    output.layer.foreach(x =>
//      x ! UpdateOutputLinking(
//        new OneToOnePartitioning(100),
//        linkTag(),
//        Array(new DirectRoutee(testActor))
//      )
//    )
//    scan ? PrincipalMessage.AssignBreakpoint(new ExceptionGlobalBreakpoint("ex1"))
//    process ? PrincipalMessage.AssignBreakpoint(new ExceptionGlobalBreakpoint("ex2"))
//    scan ? PrincipalMessage.AssignBreakpoint(
//      new ConditionalGlobalBreakpoint("cond1", x => x.getInt(0) <= 4)
//    )
//    process ? PrincipalMessage.AssignBreakpoint(
//      new ConditionalGlobalBreakpoint("cond2", x => x.getInt(0) >= 8)
//    )
//    scan ? Start
//    parent.receiveWhile(10.seconds, 5.seconds) {
//      case ReportState(state) =>
//        println(parent.sender() + " changes to " + state)
//        state match {
//          case PrincipalState.Paused =>
//            parent.sender() ! Resume
//          case _ =>
//        }
//      case ReportGlobalBreakpointTriggered(s, operatorID) =>
//        println(s)
//    }
//    resultValidation(3, 30.seconds)
//    parent.ref ! PoisonPill
//  }
//
//  "Connected Principals of multiple workers" should "be able to set a count breakpoint and trigger it on complete" in {
//    val parent = TestProbe()
//    ignoreMsg {
//      case UpdateInputLinking(_, _) => true
//    }
//    parent.ignoreMsg {
//      case ReportPrincipalPartialCompleted(x, y) => true
//    }
//    val meta1 = new SimpleSourceOperatorMetadata(opTag(), 5, 100000)
//    val scan = parent.childActorOf(Principal.props(meta1))
//    val meta2 = new SimpleProcessOperatorMetadata(opTag(), 5)
//    val process = parent.childActorOf(Principal.props(meta2))
//    scan ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    process ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    val edge = new OperatorLink((meta1, scan), (meta2, process))
//    edge.link()
//    val output = Await.result(process ? GetOutputLayer, timeout.duration).asInstanceOf[ActorLayer]
//    output.layer.foreach(x =>
//      x ! UpdateOutputLinking(
//        new OneToOnePartitioning(100),
//        linkTag(),
//        Array(new DirectRoutee(testActor))
//      )
//    )
//    scan ? PrincipalMessage.AssignBreakpoint(new ExceptionGlobalBreakpoint("ex1"))
//    process ? PrincipalMessage.AssignBreakpoint(new ExceptionGlobalBreakpoint("ex2"))
//    scan ? PrincipalMessage.AssignBreakpoint(new CountGlobalBreakpoint("count1", 100000))
//    process ? PrincipalMessage.AssignBreakpoint(new CountGlobalBreakpoint("count2", 100000))
//    scan ? Start
//    parent.receiveWhile(10.seconds, 5.seconds) {
//      case ReportState(state) =>
//        println(parent.sender() + " changes to " + state)
//        state match {
//          case PrincipalState.Paused =>
//            parent.sender() ! Resume
//          case _ =>
//        }
//      case ReportGlobalBreakpointTriggered(s, operatorID) =>
//        println(s)
//    }
//    resultValidation(100000, 5.seconds)
//    parent.ref ! PoisonPill
//  }
//
//  "Connected Principals of single worker" should "be able to set a count breakpoint and trigger it on complete" in {
//    val parent = TestProbe()
//    ignoreMsg {
//      case UpdateInputLinking(_, _) => true
//    }
//    parent.ignoreMsg {
//      case ReportPrincipalPartialCompleted(x, y) => true
//    }
//    val meta1 = new SimpleSourceOperatorMetadata(opTag(), 1, 100000)
//    val scan = parent.childActorOf(Principal.props(meta1))
//    val meta2 = new SimpleProcessOperatorMetadata(opTag(), 1)
//    val process = parent.childActorOf(Principal.props(meta2))
//    scan ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    process ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    val edge = new OperatorLink((meta1, scan), (meta2, process))
//    edge.link()
//    val output = Await.result(process ? GetOutputLayer, timeout.duration).asInstanceOf[ActorLayer]
//    output.layer.foreach(x =>
//      x ! UpdateOutputLinking(
//        new OneToOnePartitioning(100),
//        linkTag(),
//        Array(new DirectRoutee(testActor))
//      )
//    )
//    scan ? PrincipalMessage.AssignBreakpoint(new ExceptionGlobalBreakpoint("ex1"))
//    process ? PrincipalMessage.AssignBreakpoint(new ExceptionGlobalBreakpoint("ex2"))
//    scan ? PrincipalMessage.AssignBreakpoint(new CountGlobalBreakpoint("count1", 100000))
//    process ? PrincipalMessage.AssignBreakpoint(new CountGlobalBreakpoint("count2", 100000))
//    scan ? Start
//    parent.receiveWhile(10.seconds, 5.seconds) {
//      case ReportState(state) =>
//        println(parent.sender() + " changes to " + state)
//        state match {
//          case PrincipalState.Paused =>
//            parent.sender() ! Resume
//          case _ =>
//        }
//      case ReportGlobalBreakpointTriggered(s, operatorID) =>
//        println(s)
//    }
//    resultValidation(100000, 30.seconds)
//    parent.ref ! PoisonPill
//  }
//
//  "Connected Principals of multiple worker" should "be able to pause/resume with a count breakpoint" in {
//    val parent = TestProbe()
//    ignoreMsg {
//      case UpdateInputLinking(_, _) => true
//    }
//    parent.ignoreMsg {
//      case ReportPrincipalPartialCompleted(x, y) => true
//    }
//    val meta1 = new SimpleSourceOperatorMetadata(opTag(), 5, 100000)
//    val scan = parent.childActorOf(Principal.props(meta1))
//    val meta2 = new SimpleProcessOperatorMetadata(opTag(), 5)
//    val process = parent.childActorOf(Principal.props(meta2))
//    scan ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    process ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    val edge = new OperatorLink((meta1, scan), (meta2, process))
//    edge.link()
//    val output = Await.result(process ? GetOutputLayer, timeout.duration).asInstanceOf[ActorLayer]
//    output.layer.foreach(x =>
//      x ! UpdateOutputLinking(
//        new OneToOnePartitioning(100),
//        linkTag(),
//        Array(new DirectRoutee(testActor))
//      )
//    )
//    scan ? PrincipalMessage.AssignBreakpoint(new ExceptionGlobalBreakpoint("ex1"))
//    process ? PrincipalMessage.AssignBreakpoint(new ExceptionGlobalBreakpoint("ex2"))
//    scan ? PrincipalMessage.AssignBreakpoint(new CountGlobalBreakpoint("count1", 80000))
//    process ? PrincipalMessage.AssignBreakpoint(new CountGlobalBreakpoint("count2", 80000))
//    scan ? Start
//    val random = new Random()
//    for (i <- 0 until 100) {
//      if (random.nextBoolean()) {
//        if (random.nextBoolean()) scan ! Pause else scan ! Resume
//      } else {
//        if (random.nextBoolean()) process ! Pause else process ! Resume
//      }
//    }
//    scan ! Resume
//    process ! Resume
//    parent.receiveWhile(10.seconds, 5.seconds) {
//      case ReportState(state) =>
//        state match {
//          case PrincipalState.Paused =>
//            parent.sender() ! Resume
//          case _ =>
//        }
//      case ReportGlobalBreakpointTriggered(s, operatorID) =>
//        println(s)
//    }
//    resultValidation(100000, 30.seconds)
//    parent.ref ! PoisonPill
//  }
//
//  "Connected Principals of multiple workers" should "be able to pause/resume with a conditional breakpoint" in {
//    val parent = TestProbe()
//    ignoreMsg {
//      case UpdateInputLinking(_, _) => true
//    }
//    parent.ignoreMsg {
//      case ReportPrincipalPartialCompleted(x, y) => true
//    }
//    val meta1 = new SimpleSourceOperatorMetadata(opTag(), 5, 100000)
//    val scan = parent.childActorOf(Principal.props(meta1))
//    val meta2 = new SimpleProcessOperatorMetadata(opTag(), 5)
//    val process = parent.childActorOf(Principal.props(meta2))
//    scan ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    process ! AckedPrincipalInitialization(Array())
//    parent.expectMsg(ReportState(PrincipalState.Ready))
//    val edge = new OperatorLink((meta1, scan), (meta2, process))
//    edge.link()
//    val output = Await.result(process ? GetOutputLayer, timeout.duration).asInstanceOf[ActorLayer]
//    output.layer.foreach(x =>
//      x ! UpdateOutputLinking(
//        new OneToOnePartitioning(100),
//        linkTag(),
//        Array(new DirectRoutee(testActor))
//      )
//    )
//    scan ? PrincipalMessage.AssignBreakpoint(new ExceptionGlobalBreakpoint("ex1"))
//    process ? PrincipalMessage.AssignBreakpoint(new ExceptionGlobalBreakpoint("ex2"))
//    scan ? PrincipalMessage.AssignBreakpoint(
//      new ConditionalGlobalBreakpoint("cond1", x => x.getInt(0) % 8973 == 0)
//    )
//    process ? PrincipalMessage.AssignBreakpoint(
//      new ConditionalGlobalBreakpoint("cond2", x => x.getInt(0) % 6462 == 0)
//    )
//    scan ? Start
//    val random = new Random()
//    for (i <- 0 until 100) {
//      if (random.nextBoolean()) {
//        if (random.nextBoolean()) scan ! Pause else scan ! Resume
//      } else {
//        if (random.nextBoolean()) process ! Pause else process ! Resume
//      }
//    }
//    scan ! Resume
//    process ! Resume
//    parent.receiveWhile(10.seconds, 5.seconds) {
//      case ReportState(state) =>
//        state match {
//          case PrincipalState.Paused =>
//            parent.sender() ! Resume
//          case _ =>
//        }
//      case ReportGlobalBreakpointTriggered(s, operatorID) =>
//        println(s)
//    }
//    resultValidation(99975, 30.seconds)
//    parent.ref ! PoisonPill
//  }
//
//}
