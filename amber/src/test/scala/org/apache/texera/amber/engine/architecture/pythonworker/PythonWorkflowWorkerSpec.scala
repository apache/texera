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

package org.apache.texera.amber.engine.architecture.pythonworker

import ch.qos.logback.classic.{Level, Logger => LogbackLogger}
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.twitter.util.{Promise, Return}
import org.apache.commons.lang3.SystemUtils
import org.apache.pekko.actor.{
  Actor,
  ActorRef,
  ActorSystem,
  OneForOneStrategy,
  Props,
  SupervisorStrategy
}
import org.apache.pekko.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import org.apache.texera.amber.clustering.SingleNodeListener
import org.apache.texera.amber.core.WorkflowRuntimeException
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.core.workflow.{PortIdentity, WorkflowContext}
import org.apache.texera.amber.engine.architecture.common.WorkflowActor
import org.apache.texera.amber.engine.architecture.coordinator.{
  Coordinator,
  CoordinatorConfig,
  FatalError
}
import org.apache.texera.amber.engine.architecture.common.WorkflowActor.{NetworkAck, NetworkMessage}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  ControlInvocation,
  EmptyRequest
}
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.ambermessage.WorkflowMessage.getInMemSize
import org.apache.texera.amber.engine.architecture.scheduling.config.WorkerConfig
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.e2e.TestUtils.buildWorkflow
import org.apache.texera.amber.operator.TestOperators
import org.apache.texera.common.compiler.model.LogicalLink
import org.apache.texera.common.config.{PythonUtils, UdfConfig}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

import java.io.{InputStream, OutputStream}
import java.net.ServerSocket
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.{ExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Using

/**
  * Unit tests for the Python worker actor.
  *
  * The worker's `initState` starts a proxy server, spawns a real Python interpreter and starts a
  * Flight client, none of which belongs in a unit test and none of which is guaranteed to exist on
  * a CI runner. The class offers no seam for that, but `initState` is an ordinary override, so the
  * suite subclasses the worker and makes it a no-op. Everything below then runs against a real
  * actor instance with nothing spawned.
  *
  * Two members are read by reflection because they are `private`: `choosePythonBin`, which has no
  * caller other than the process-spawning path, and the executors/port promise, which the teardown
  * assertions need. Reflection into `private` members is an established pattern in this module's
  * specs.
  *
  * The failure-reporting tests at the end are the exception: they need a real child process, so
  * they run `startProxyServer` and `startPythonProcess` with a stand-in interpreter, a two-line
  * shell script placed where the worker's virtual-environment lookup finds it. No Python is
  * involved. Those workers run under [[FailureCapturingParent]], which supervises them the way
  * the coordinator does, so a failure is observed exactly where the engine would observe it.
  */
class PythonWorkflowWorkerSpec
    extends TestKit(ActorSystem("PythonWorkflowWorkerSpec", AmberRuntime.pekkoConfig))
    with ImplicitSender
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  /** A cuid no other suite writes under, so the venv fixtures cannot collide. */
  private val FixtureCuid = 987654
  private val VenvRoot: Path = Paths.get("/tmp/texera-pve/venvs")

  // A real coordinator, used by one test below, looks this listener up.
  override def beforeAll(): Unit = system.actorOf(Props[SingleNodeListener](), "cluster-info")

  override def afterAll(): Unit = {
    deleteRecursively(VenvRoot.resolve(FixtureCuid.toString))
    TestKit.shutdownActorSystem(system)
  }

  /** The worker with its process/socket startup removed; see the class comment. */
  private class InertPythonWorkflowWorker(config: WorkerConfig)
      extends PythonWorkflowWorker(config) {
    override def initState(): Unit = ()
  }

  /** The parts of a `Process` that the worker never touches on a stand-in. */
  private abstract class StandInProcess extends Process {
    override def getOutputStream: OutputStream = OutputStream.nullOutputStream()
    override def getInputStream: InputStream = InputStream.nullInputStream()
    override def getErrorStream: InputStream = InputStream.nullInputStream()
    override def waitFor(): Int = exitValue()
  }

  /** Stands in for the OS process the worker would have spawned. */
  private class RecordingProcess(failOnDestroy: Boolean = false) extends StandInProcess {
    val destroyed = new AtomicBoolean(false)
    override def isAlive(): Boolean = !destroyed.get()
    override def exitValue(): Int = 0
    override def destroy(): Unit = {
      destroyed.set(true)
      if (failOnDestroy) throw new IllegalStateException("destroy failed")
    }
  }

  private def newWorker(config: WorkerConfig): TestActorRef[InertPythonWorkflowWorker] =
    TestActorRef[InertPythonWorkflowWorker](new InertPythonWorkflowWorker(config))

  private def config(pveName: String = "", cuid: Option[Int] = None): WorkerConfig =
    WorkerConfig(ActorVirtualIdentity("python-worker-spec"), pveName = pveName, cuid = cuid)

  private def readPrivate[T](actor: PythonWorkflowWorker, name: String): T = {
    // A private member that a closure uses gets a name-mangled accessor ending in `$$name`.
    val method = classOf[PythonWorkflowWorker].getDeclaredMethods
      .find(m => m.getParameterCount == 0 && (m.getName == name || m.getName.endsWith("$$" + name)))
      .getOrElse(throw new NoSuchMethodException(name))
    method.setAccessible(true)
    method.invoke(actor).asInstanceOf[T]
  }

  private def chosenPythonBin(actor: PythonWorkflowWorker): String =
    readPrivate[String](actor, "choosePythonBin")

  /** Gives the worker its port, as the Python process would report it. */
  private def openPort(actor: PythonWorkflowWorker, port: Int = 65535): Unit =
    readPrivate[Promise[Int]](actor, "portNumberPromise").setValue(port)

  private def setServerProcess(actor: PythonWorkflowWorker, process: Process): Unit = {
    val setter =
      classOf[PythonWorkflowWorker].getDeclaredMethod("pythonServerProcess_$eq", classOf[Process])
    setter.setAccessible(true)
    setter.invoke(actor, process)
  }

  /** The interpreter PveManager resolves for (cuid, name); POSIX and Windows differ. */
  private def venvPython(cuid: Int, pveName: String): Path = {
    val venv = VenvRoot.resolve(cuid.toString).resolve(pveName).resolve("pve")
    if (SystemUtils.IS_OS_WINDOWS) venv.resolve("Scripts").resolve("python.exe")
    else venv.resolve("bin").resolve("python")
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.exists(path)) {
      // Files.walk holds an open directory handle, so it has to be closed; on Windows an
      // open handle also blocks the deletion it is feeding.
      Using.resource(Files.walk(path)) { paths =>
        paths
          .sorted(java.util.Comparator.reverseOrder[Path]())
          .forEach(p => Files.deleteIfExists(p))
      }
    }
  }

  /**
    * The proxy client the worker builds for itself. Its accessor is name-mangled because the
    * field is captured by a closure, so it is found by suffix rather than spelled out.
    */
  private def proxyClient(actor: PythonWorkflowWorker): PythonProxyClient = {
    val accessors = classOf[PythonWorkflowWorker].getDeclaredMethods
      .filter(m => m.getName.endsWith("pythonProxyClient") && m.getParameterCount == 0)
    accessors should have size 1
    accessors.head.setAccessible(true)
    accessors.head.invoke(actor).asInstanceOf[PythonProxyClient]
  }

  /** The client's main-loop flag, which `close()` clears. */
  private def isClientRunning(client: PythonProxyClient): Boolean = {
    val field = classOf[PythonProxyClient].getDeclaredField("running")
    field.setAccessible(true)
    field.getBoolean(client)
  }

  // ---------------------------------------------------------------------------
  // Failure-reporting fixtures
  // ---------------------------------------------------------------------------

  /** Sent to the failure probe once the worker child exists. */
  private case class Spawned(worker: ActorRef)

  /**
    * Creates one worker as its child and supervises it the way the coordinator does: each failure
    * goes to `failures` and the child is stopped. The child's RegisterActorRef goes to
    * `registrations`; anything else it sends its parent is ignored.
    */
  private class FailureCapturingParent(
      worker: => PythonWorkflowWorker,
      failures: ActorRef,
      registrations: ActorRef
  ) extends Actor {
    override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
      case e: Throwable =>
        failures ! e
        SupervisorStrategy.Stop
    }
    override def preStart(): Unit = failures ! Spawned(context.actorOf(Props(worker)))
    override def receive: Receive = {
      case registration: WorkflowActor.RegisterActorRef => registrations ! registration
      case _                                            =>
    }
  }

  /** A real worker that hands itself to the test, so the test can release its port. */
  private class ObservedWorker(config: WorkerConfig, held: AtomicReference[PythonWorkflowWorker])
      extends PythonWorkflowWorker(config) {
    held.set(this)
  }

  /** Starts the proxy server and the (stand-in) interpreter, but no Flight client. */
  private class ProcessOnlyWorker(config: WorkerConfig, held: AtomicReference[PythonWorkflowWorker])
      extends ObservedWorker(config, held) {
    override def initState(): Unit = {
      startProxyServer()
      startPythonProcess()
    }
  }

  /**
    * Starts only the Flight client, against a port nothing listens on, so the client fails to
    * connect. `process` stands in for the interpreter the worker would have spawned.
    */
  private class ClientOnlyWorker(
      config: WorkerConfig,
      held: AtomicReference[PythonWorkflowWorker],
      process: Option[Process]
  ) extends ObservedWorker(config, held) {
    override def initState(): Unit = {
      process.foreach(setServerProcess(this, _))
      openPort(this, freePort)
      startProxyClient()
    }
  }

  /** A process that has already exited with `code`. */
  private class ExitedProcess(code: Int) extends StandInProcess {
    override def isAlive(): Boolean = false
    override def exitValue(): Int = code
    override def destroy(): Unit = ()
  }

  /** A process killed just as its client fails: it still looks alive until it is waited on. */
  private class DyingProcess(code: Int) extends StandInProcess {
    @volatile private var exited = false
    override def isAlive(): Boolean = !exited
    override def waitFor(timeout: Long, unit: TimeUnit): Boolean = {
      exited = true
      true
    }
    override def exitValue(): Int =
      if (exited) code else throw new IllegalThreadStateException("process has not exited")
    override def destroy(): Unit = ()
  }

  private case class Supervised(
      worker: ActorRef,
      instance: AtomicReference[PythonWorkflowWorker],
      failures: TestProbe,
      registrations: TestProbe
  )

  private def supervise(
      create: AtomicReference[PythonWorkflowWorker] => PythonWorkflowWorker
  ): Supervised = {
    val failures = TestProbe()
    val registrations = TestProbe()
    val held = new AtomicReference[PythonWorkflowWorker]()
    system.actorOf(
      Props(new FailureCapturingParent(create(held), failures.ref, registrations.ref))
    )
    val worker = failures.expectMsgType[Spawned](10.seconds).worker
    // The child is constructed asynchronously, after the parent has already reported it.
    awaitCond(held.get != null, 10.seconds)
    Supervised(worker, held, failures, registrations)
  }

  /** Threads waiting for a Python worker's port, with the frames that show where. */
  private def threadsWaitingForPort(): Seq[String] =
    Thread.getAllStackTraces.asScala.toSeq.collect {
      case (thread, frames)
          if frames.exists(_.getClassName.startsWith("com.twitter.util.Await")) &&
            frames.exists(f =>
              f.getClassName.startsWith(classOf[PythonProxyClient].getName) ||
                f.getClassName.startsWith(classOf[PythonWorkflowWorker].getName)
            ) =>
        s"${thread.getName}: " + frames.take(12).mkString(" < ")
    }

  /** Error messages logged by loggers whose name contains `fragment` while `body` runs. */
  private def errorsLoggedBy(fragment: String)(body: => Unit): Seq[String] = {
    val root =
      LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[LogbackLogger]
    val appender = new ListAppender[ILoggingEvent]
    appender.start()
    root.addAppender(appender)
    val events =
      try {
        body
        appender.synchronized(appender.list.asScala.toList)
      } finally {
        root.detachAppender(appender)
        appender.stop()
      }
    events
      .filter(e => e.getLevel == Level.ERROR && e.getLoggerName.contains(fragment))
      .map(_.getFormattedMessage)
  }

  /** A real coordinator, as `CoordinatorSpec` builds one, reporting to `client`. */
  private def newCoordinator(client: TestProbe, name: String): TestActorRef[Coordinator] = {
    val csv = TestOperators.headerlessSmallCsvScanOpDesc()
    val keyword = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val workflow = buildWorkflow(
      List(csv, keyword),
      List(
        LogicalLink(
          csv.operatorIdentifier,
          PortIdentity(0),
          keyword.operatorIdentifier,
          PortIdentity(0)
        )
      ),
      new WorkflowContext()
    )
    TestActorRef[Coordinator](
      Coordinator.props(workflow.context, workflow.physicalPlan, CoordinatorConfig.default),
      client.ref,
      name
    )
  }

  private def freePort: Int = Using.resource(new ServerSocket(0))(_.getLocalPort)

  /** Gives a worker its port if it never got one, so nothing waits on it any more. */
  private def releasePort(actor: PythonWorkflowWorker): Unit =
    readPrivate[Promise[Int]](actor, "portNumberPromise").updateIfEmpty(Return(freePort))

  /** Stops the worker if it is still running; never fails, so it cannot mask a test failure. */
  private def stopQuietly(s: Supervised): Unit =
    try {
      Option(s.instance.get).foreach(releasePort)
      val probe = TestProbe()
      probe.watch(s.worker)
      system.stop(s.worker)
      probe.expectTerminated(s.worker, 15.seconds)
    } catch { case _: Throwable => () }

  /**
    * Writes an executable shell script where the worker's virtual-environment lookup finds its
    * interpreter, so `startPythonProcess` runs it instead of Python.
    */
  private def standInInterpreter(pveName: String, body: String): Unit = {
    assume(!SystemUtils.IS_OS_WINDOWS, "the stand-in interpreter is a POSIX shell script")
    val python = venvPython(FixtureCuid, pveName)
    Files.createDirectories(python.getParent)
    Files.write(python, s"#!/bin/sh\n$body\n".getBytes(StandardCharsets.UTF_8))
    python.toFile.setExecutable(true)
    assume(Files.isExecutable(python), "the stand-in interpreter is not executable here")
  }

  /**
    * Shell lines that leave `sleep 60` running in the background. It inherits the interpreter's
    * stdout and stderr, so they stay open after the interpreter exits, the way a UDF's subprocess
    * would keep them. Its pid goes to `pidFile` so the test can stop it.
    */
  private def holdOutputPipes(pidFile: Path): String = s"sleep 60 &\necho $$! > '$pidFile'"

  private def stopPipeHolder(pidFile: Path): Unit =
    try {
      val pid = new String(Files.readAllBytes(pidFile), StandardCharsets.UTF_8).trim
      if (pid.nonEmpty)
        ProcessHandle.of(pid.toLong).ifPresent { holder => holder.destroyForcibly(); () }
    } catch { case _: Throwable => () }
    finally Files.deleteIfExists(pidFile)

  private def failureConfig(name: String, pveName: String = ""): WorkerConfig =
    WorkerConfig(ActorVirtualIdentity(name), pveName = pveName, cuid = Some(FixtureCuid))

  /** Every message in a failure's cause chain, so assertions do not depend on wrapping. */
  private def messagesOf(e: Throwable): String =
    Iterator
      .iterate(e)(_.getCause)
      .takeWhile(_ != null)
      .map(x => String.valueOf(x.getMessage))
      .mkString(" | ")

  private def workflowExceptionIn(e: Throwable): Option[WorkflowRuntimeException] =
    Iterator.iterate(e)(_.getCause).takeWhile(_ != null).collectFirst {
      case w: WorkflowRuntimeException => w
    }

  behavior of "PythonWorkflowWorker"

  it should "resolve the Python sources under the amber home" in {
    val worker = newWorker(config())
    val path = worker.underlyingActor.pythonSrcDirectory

    // Asserted by tail rather than as an absolute string: the amber home moves with the checkout.
    path.getFileName.toString shouldBe "python"
    path.getParent.getFileName.toString shouldBe "main"
    path.getParent.getParent.getFileName.toString shouldBe "src"
  }

  it should "expose the configured R path with its surrounding whitespace removed" in {
    val worker = newWorker(config())

    worker.underlyingActor.RENVPath shouldBe UdfConfig.rPath.trim
    worker.underlyingActor.RENVPath shouldBe worker.underlyingActor.RENVPath.trim
  }

  it should "fall back to the default interpreter when the worker has no computing unit" in {
    val worker = newWorker(config(pveName = "myenv", cuid = None))

    chosenPythonBin(worker.underlyingActor) shouldBe PythonUtils.getPythonExecutable
  }

  it should "fall back to the default interpreter when the environment name is blank" in {
    // The name is trimmed before the emptiness check, so whitespace alone must not select a venv.
    val worker = newWorker(config(pveName = "   ", cuid = Some(FixtureCuid)))

    chosenPythonBin(worker.underlyingActor) shouldBe PythonUtils.getPythonExecutable
  }

  it should "fall back to the default interpreter when the environment has no interpreter on disk" in {
    val worker = newWorker(config(pveName = "absent-env", cuid = Some(FixtureCuid)))

    chosenPythonBin(worker.underlyingActor) shouldBe PythonUtils.getPythonExecutable
  }

  it should "select the virtual environment's interpreter when one exists" in {
    val pveName = "present-env"
    val python = venvPython(FixtureCuid, pveName)
    Files.createDirectories(python.getParent)
    Files.deleteIfExists(python)
    Files.createFile(python)
    python.toFile.setExecutable(true)
    // A filesystem that ignores the executable bit (or a root user) would make the assertion
    // vacuous rather than failing, so skip instead of pretending to have covered the branch.
    assume(Files.isExecutable(python), "the fixture interpreter is not executable here")
    val worker = newWorker(config(pveName = pveName, cuid = Some(FixtureCuid)))

    chosenPythonBin(worker.underlyingActor) shouldBe python.toAbsolutePath.normalize().toString
  }

  it should "close the proxy client, stop both executors and destroy the process on shutdown" in {
    val worker = newWorker(config())
    val actor = worker.underlyingActor
    openPort(actor)
    val process = new RecordingProcess()
    setServerProcess(actor, process)
    val client = proxyClient(actor)
    val clientExecutor = readPrivate[ExecutorService](actor, "clientThreadExecutor")
    val serverExecutor = readPrivate[ExecutorService](actor, "serverThreadExecutor")
    isClientRunning(client) shouldBe true

    watch(worker)
    system.stop(worker)
    expectTerminated(worker, 10.seconds)

    // close() clears the client's main-loop flag, so this fails if the call is dropped.
    isClientRunning(client) shouldBe false
    clientExecutor.isShutdown shouldBe true
    serverExecutor.isShutdown shouldBe true
    process.destroyed.get() shouldBe true
  }

  it should "not let a failing teardown step escape postStop" in {
    val worker = newWorker(config())
    val actor = worker.underlyingActor
    openPort(actor)
    val process = new RecordingProcess(failOnDestroy = true)
    setServerProcess(actor, process)
    val client = proxyClient(actor)
    val clientExecutor = readPrivate[ExecutorService](actor, "clientThreadExecutor")
    val serverExecutor = readPrivate[ExecutorService](actor, "serverThreadExecutor")

    watch(worker)
    system.stop(worker)

    // The actor still terminates, and the steps that ran before the failure still took effect.
    expectTerminated(worker, 10.seconds)
    isClientRunning(client) shouldBe false
    clientExecutor.isShutdown shouldBe true
    serverExecutor.isShutdown shouldBe true
    process.destroyed.get() shouldBe true
  }

  it should "log no error when it is stopped before its Python process was started" in {
    val worker = newWorker(config())
    watch(worker)

    val errors = errorsLoggedBy(classOf[PythonWorkflowWorker].getSimpleName) {
      system.stop(worker)
      expectTerminated(worker, 10.seconds)
    }

    errors shouldBe empty
  }

  it should "answer a credit request with the credit currently queued for that channel" in {
    val worker = newWorker(config())
    openPort(worker.underlyingActor)
    val channel =
      ChannelIdentity(
        ActorVirtualIdentity("upstream"),
        ActorVirtualIdentity("downstream"),
        isControl = true
      )

    val client = proxyClient(worker.underlyingActor)

    worker ! WorkflowActor.CreditRequest(channel)

    // The request also pushes a CreditUpdate down to the Python side.
    client.getControlQueueLength shouldBe 1
    val response = expectMsgType[WorkflowActor.CreditResponse](10.seconds)
    response.channelId shouldBe channel
    // Nothing has been handed to the Python side, so it is holding nothing.
    response.credit shouldBe 0L
  }

  it should "stay responsive after backpressure is pushed to the Python side" in {
    val worker = newWorker(config())
    openPort(worker.underlyingActor)
    val channel =
      ChannelIdentity(
        ActorVirtualIdentity("upstream"),
        ActorVirtualIdentity("downstream"),
        isControl = true
      )

    val client = proxyClient(worker.underlyingActor)

    worker.underlyingActor.handleBackpressure(true)
    worker.underlyingActor.handleBackpressure(false)

    // Both commands are queued for the Python process rather than answered here.
    client.getControlQueueLength shouldBe 2
    // The actor also keeps serving its own protocol afterwards.
    worker ! WorkflowActor.CreditRequest(channel)
    expectMsgType[WorkflowActor.CreditResponse](10.seconds).channelId shouldBe channel
  }

  it should "hand a control payload to the Python side and acknowledge the message" in {
    val worker = newWorker(config())
    openPort(worker.underlyingActor)
    val channel =
      ChannelIdentity(
        ActorVirtualIdentity("upstream"),
        ActorVirtualIdentity("python-worker-spec"),
        isControl = true
      )
    val payload =
      ControlInvocation(
        "method",
        EmptyRequest(),
        AsyncRPCContext(channel.fromWorkerId, channel.toWorkerId),
        0
      )
    val message = WorkflowFIFOMessage(channel, 0, payload)
    val client = proxyClient(worker.underlyingActor)
    client.getControlQueueLength shouldBe 0

    worker ! NetworkMessage(7L, message)

    // The payload is handed to the Python side, not just acknowledged.
    client.getControlQueueLength shouldBe 1
    val ack = expectMsgType[NetworkAck](10.seconds)
    ack.messageId shouldBe 7L
    ack.ackedCredit shouldBe getInMemSize(message)
    // The payload is queued for the Python process, which holds no data of its own here.
    ack.queuedCredit shouldBe 0L
  }

  it should "refuse to load from a checkpoint" in {
    val worker = newWorker(config())

    a[NotImplementedError] should be thrownBy worker.underlyingActor.loadFromCheckpoint(null)
  }

  // ---------------------------------------------------------------------------
  // Failure reporting: a Python process that exits, a Flight client that stops, and an
  // interpreter that exits before connecting must each fail the worker, so the coordinator
  // can fail the run instead of letting it hang.
  // ---------------------------------------------------------------------------

  for ((code, how) <- Seq(3 -> "with an error code", 0 -> "cleanly")) {
    it should s"report a Python process that exits $how mid-run, naming the worker and the code" in {
      val name = s"py-exit-$code"
      standInInterpreter(name, s"exit $code")
      val s = supervise(held => new ProcessOnlyWorker(failureConfig(name, name), held))
      try {
        val failure = s.failures.expectMsgType[Throwable](15.seconds)
        val messages = messagesOf(failure)
        messages should include(name)
        messages should include(s"code $code")
        messages should not include "out-of-memory"
        workflowExceptionIn(failure).flatMap(_.relatedWorkerId) shouldBe
          Some(ActorVirtualIdentity(name))
      } finally stopQuietly(s)
    }
  }

  it should "report a Python process killed by SIGKILL with an out-of-memory hint" in {
    val name = "py-sigkill"
    standInInterpreter(name, "kill -9 $$")
    val s = supervise(held => new ProcessOnlyWorker(failureConfig(name, name), held))
    try {
      val messages = messagesOf(s.failures.expectMsgType[Throwable](15.seconds))
      messages should include(name)
      messages should include("code 137")
      messages should include("out-of-memory")
    } finally stopQuietly(s)
  }

  it should "report a Flight client that stops, with the client's error as the cause" in {
    val name = "py-client-stops"
    val s = supervise(held => new ClientOnlyWorker(failureConfig(name), held, process = None))
    try {
      val failure = s.failures.expectMsgType[Throwable](15.seconds)
      val messages = messagesOf(failure)
      messages should include(name)
      messages should include("Failed to connect to Flight Server")
      workflowExceptionIn(failure).flatMap(_.relatedWorkerId) shouldBe
        Some(ActorVirtualIdentity(name))
    } finally stopQuietly(s)
  }

  it should "report the exit code when its client stops because the Python process is gone" in {
    val name = "py-gone"
    val s = supervise(held =>
      new ClientOnlyWorker(failureConfig(name), held, process = Some(new ExitedProcess(137)))
    )
    try {
      val messages = messagesOf(s.failures.expectMsgType[Throwable](15.seconds))
      messages should include(name)
      messages should include("code 137")
      messages should include("out-of-memory")
    } finally stopQuietly(s)
  }

  it should "report the exit code when its client stops just before the killed process is seen to exit" in {
    val name = "py-dying"
    // A killed process's connection can close before the JVM records its exit, so the client's
    // failure may arrive while the process still looks alive.
    val s = supervise(held =>
      new ClientOnlyWorker(failureConfig(name), held, process = Some(new DyingProcess(137)))
    )
    try {
      val messages = messagesOf(s.failures.expectMsgType[Throwable](15.seconds))
      messages should include(name)
      messages should include("code 137")
      messages should include("out-of-memory")
    } finally stopQuietly(s)
  }

  it should "fail and stop when the Python process exits before it connects" in {
    val name = "py-early-exit"
    standInInterpreter(name, "exit 1")
    val s = supervise(held => new ObservedWorker(failureConfig(name, name), held))
    try {
      val failure = s.failures.expectMsgType[Throwable](15.seconds)
      // Reported like any other exit once start-up is over, not as a failure to start, so the
      // worker has registered with its parent, which can name it.
      failure shouldBe a[WorkflowRuntimeException]
      failure.getMessage should include(name)
      failure.getMessage should include("code 1 before connecting")
      failure.asInstanceOf[WorkflowRuntimeException].relatedWorkerId shouldBe
        Some(ActorVirtualIdentity(name))
      s.registrations.expectMsgType[WorkflowActor.RegisterActorRef](15.seconds).id shouldBe
        ActorVirtualIdentity(name)
      // The worker must not stay stuck waiting for a port that will never come.
      val probe = TestProbe()
      probe.watch(s.worker)
      probe.expectTerminated(s.worker, 15.seconds)
    } finally stopQuietly(s)
  }

  it should "stop without leaving a thread waiting when Python never reports its port" in {
    val name = "py-never-connects"
    standInInterpreter(name, "exec sleep 30")
    val s = supervise(held => new ObservedWorker(failureConfig(name, name), held))
    try {
      // Start-up does not wait for Python, so the worker registers while its client waits.
      s.registrations.expectMsgType[WorkflowActor.RegisterActorRef](10.seconds)
      awaitCond(threadsWaitingForPort().nonEmpty, 10.seconds)
      val probe = TestProbe()
      probe.watch(s.worker)
      system.stop(s.worker)
      probe.expectTerminated(s.worker, 10.seconds)
      awaitCond(
        threadsWaitingForPort().isEmpty,
        10.seconds,
        message = threadsWaitingForPort().mkString("\n")
      )
      s.failures.expectNoMessage(1.second)
    } finally stopQuietly(s)
  }

  it should "have its early exit attributed to it by a real coordinator" in {
    val name = "py-early-exit-coordinated"
    standInInterpreter(name, "exit 1")
    val client = TestProbe()
    val coordinator = newCoordinator(client, s"coordinator-$name")
    client.expectMsgType[WorkflowActor.RegisterActorRef](10.seconds)
    try {
      coordinator.underlyingActor.context.actorOf(
        Props(new PythonWorkflowWorker(failureConfig(name, name)))
      )
      var reported: Option[(Throwable, Option[ActorVirtualIdentity])] = None
      client.fishForMessage(15.seconds) {
        case NetworkMessage(_, WorkflowFIFOMessage(_, _, FatalError(error, fromActor))) =>
          reported = Some((error, fromActor))
          true
        case _ => false
      }
      val (error, fromActor) = reported.get
      fromActor shouldBe Some(ActorVirtualIdentity(name))
      messagesOf(error) should include("code 1 before connecting")
    } finally {
      watch(coordinator)
      system.stop(coordinator)
      expectTerminated(coordinator, 15.seconds)
    }
  }

  it should "report a Python process that exits mid-run while its child keeps the output open" in {
    val name = "py-exit-held-pipes"
    val holder = Files.createTempFile(name, ".pid")
    standInInterpreter(name, s"${holdOutputPipes(holder)}\nexit 3")
    val s = supervise(held => new ProcessOnlyWorker(failureConfig(name, name), held))
    try {
      // The exit must be seen when the process ends, not when every copy of its output closes.
      val messages = messagesOf(s.failures.expectMsgType[Throwable](15.seconds))
      messages should include(name)
      messages should include("code 3")
    } finally {
      stopPipeHolder(holder)
      stopQuietly(s)
    }
  }

  it should "fail and stop when the Python process exits before it connects while its child keeps the output open" in {
    val name = "py-early-exit-held-pipes"
    val holder = Files.createTempFile(name, ".pid")
    standInInterpreter(name, s"${holdOutputPipes(holder)}\nexit 1")
    val s = supervise(held => new ObservedWorker(failureConfig(name, name), held))
    try {
      val messages = messagesOf(s.failures.expectMsgType[Throwable](15.seconds))
      messages should include(name)
      messages should include("code 1")
      val probe = TestProbe()
      probe.watch(s.worker)
      probe.expectTerminated(s.worker, 15.seconds)
    } finally {
      stopPipeHolder(holder)
      stopQuietly(s)
    }
  }

  it should "not report its own Python process exiting when it is stopped" in {
    val name = "py-stopped"
    standInInterpreter(name, "exec sleep 30")
    val s = supervise(held => new ProcessOnlyWorker(failureConfig(name, name), held))
    try {
      val probe = TestProbe()
      probe.watch(s.worker)
      system.stop(s.worker)
      probe.expectTerminated(s.worker, 15.seconds)
      // Stopping the worker ends its interpreter; that exit is expected, not a failure.
      s.failures.expectNoMessage(1.second)
    } finally stopQuietly(s)
  }
}
