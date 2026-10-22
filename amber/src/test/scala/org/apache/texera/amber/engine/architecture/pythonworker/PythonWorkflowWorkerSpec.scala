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

import com.twitter.util.Promise
import org.apache.commons.lang3.SystemUtils
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.{ImplicitSender, TestActorRef, TestKit}
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.engine.architecture.common.WorkflowActor
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
import org.apache.texera.common.config.{PythonUtils, UdfConfig}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.ExecutorService
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration._
import scala.util.Using
import scala.sys.process.Process

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
  * specs. `portNumberPromise` in particular has to be fulfilled before anything touches
  * `pythonProxyClient`: that client blocks in its own constructor on `Await.result(promise)`, so a
  * worker whose proxy server never started would otherwise hang the moment `postStop` ran.
  *
  * Out of scope, deliberately: `startPythonProcess`, which runs a real interpreter, and
  * `startProxyServer`, which retries a socket bind until it succeeds.
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

  override def afterAll(): Unit = {
    deleteRecursively(VenvRoot.resolve(FixtureCuid.toString))
    TestKit.shutdownActorSystem(system)
  }

  /** The worker with its process/socket startup removed; see the class comment. */
  private class InertPythonWorkflowWorker(config: WorkerConfig)
      extends PythonWorkflowWorker(config) {
    override def initState(): Unit = ()
  }

  /** Stands in for the OS process the worker would have spawned. */
  private class RecordingProcess(failOnDestroy: Boolean = false) extends Process {
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
    val method = classOf[PythonWorkflowWorker].getDeclaredMethod(name)
    method.setAccessible(true)
    method.invoke(actor).asInstanceOf[T]
  }

  private def chosenPythonBin(actor: PythonWorkflowWorker): String =
    readPrivate[String](actor, "choosePythonBin")

  /** Releases the port the proxy client's constructor blocks on. */
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
}
