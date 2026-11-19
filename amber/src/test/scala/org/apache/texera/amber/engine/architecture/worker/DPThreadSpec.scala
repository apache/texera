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

package org.apache.texera.amber.engine.architecture.worker

import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.tuple.{AttributeType, Schema, Tuple, TupleLike}
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.engine.architecture.logreplay.{
  ProcessingStep,
  ReplayLogManager,
  ReplayLogRecord
}
import org.apache.texera.amber.engine.architecture.messaginglayer.WorkerTimerService
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  EmptyRequest
}
import org.apache.texera.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.{
  METHOD_PAUSE_WORKER,
  METHOD_RESUME_WORKER
}
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.{
  ActorCommandElement,
  DPInputQueueElement,
  FIFOMessageElement,
  MainThreadDelegateMessage,
  TimerBasedControlElement
}
import org.apache.texera.amber.engine.common.actormessage.{Backpressure, CreditUpdate}
import org.apache.texera.amber.engine.common.ambermessage.{
  DataFrame,
  DataPayload,
  WorkflowFIFOMessage
}
import org.apache.texera.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import org.apache.texera.amber.engine.common.storage.SequentialRecordStorage
import org.apache.texera.amber.engine.common.virtualidentity.util.SELF
import org.apache.texera.service.util.LargeBinaryManager
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URI
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import java.util.concurrent.{
  CompletableFuture,
  ConcurrentLinkedQueue,
  CountDownLatch,
  LinkedBlockingQueue,
  TimeUnit
}

class DPThreadSpec extends AnyFlatSpec with Matchers with MockFactory {

  private val workerId: ActorVirtualIdentity = ActorVirtualIdentity("DP mock")
  private val senderWorkerId: ActorVirtualIdentity = ActorVirtualIdentity("mock sender")
  private val dataChannelId = ChannelIdentity(senderWorkerId, workerId, isControl = false)
  private val controlChannelId = ChannelIdentity(senderWorkerId, workerId, isControl = true)
  private val executor = mock[OperatorExecutor]
  private val mockInputPortId = PortIdentity()

  private val schema: Schema = Schema().add("field1", AttributeType.INTEGER)
  private val tuples: Array[Tuple] = (0 until 5000)
    .map(i => TupleLike(i).enforceSchema(schema))
    .toArray
  private val logStorage = SequentialRecordStorage.getStorage[ReplayLogRecord](None)
  private val logManager: ReplayLogManager =
    ReplayLogManager.createLogManager(logStorage, "none", x => {})

  /** Poll `cond` until it holds or the budget runs out. Returns the final value. */
  private def awaitCond(cond: => Boolean, budgetMs: Int = 5000): Boolean = {
    val deadline = System.currentTimeMillis() + budgetMs
    while (!cond && System.currentTimeMillis() < deadline) {
      Thread.sleep(20)
    }
    cond
  }

  /** A DataProcessor wired to `dataChannelId` (plus `extraSender` if given), driven by `queue`. */
  private def newDataProcessor(
      queue: LinkedBlockingQueue[DPInputQueueElement],
      outputHandler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit = _ => {},
      executor: OperatorExecutor = null,
      extraSender: Option[ActorVirtualIdentity] = None,
      dp: DataProcessor = null
  ): DataProcessor = {
    val proc =
      if (dp != null) dp else new DataProcessor(workerId, outputHandler, inputMessageQueue = queue)
    if (executor != null) proc.executor = executor
    proc.inputManager.addPort(mockInputPortId, schema, List.empty, List.empty)
    proc.inputGateway.getChannel(dataChannelId).setPortId(mockInputPortId)
    extraSender.foreach(id =>
      proc.inputGateway
        .getChannel(ChannelIdentity(id, workerId, isControl = false))
        .setPortId(mockInputPortId)
    )
    proc.adaptiveBatchingMonitor = mock[WorkerTimerService]
    (proc.adaptiveBatchingMonitor.startAdaptiveBatching _).expects().anyNumberOfTimes()
    proc
  }

  private def withStartedDPThread(
      onTuple: (Tuple, Int) => Iterator[TupleLike] = null,
      dpFactory: LinkedBlockingQueue[DPInputQueueElement] => DataProcessor = null,
      logMgr: ReplayLogManager = logManager,
      baseUriOpt: Option[String] = None
  )(
      body: (LinkedBlockingQueue[DPInputQueueElement], DataProcessor, DPThread) => Unit
  ): Unit = {
    val inputQueue = new LinkedBlockingQueue[DPInputQueueElement]()
    val dp = if (dpFactory != null) dpFactory(inputQueue) else newDataProcessor(inputQueue)
    if (onTuple != null) {
      dp.executor = new OperatorExecutor {
        override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] =
          onTuple(tuple, port)
      }
    }
    val dpThread = baseUriOpt match {
      case Some(uri) => new DPThread(workerId, dp, logMgr, inputQueue, uri)
      case None      => new DPThread(workerId, dp, logMgr, inputQueue)
    }
    try {
      dpThread.start()
      body(inputQueue, dp, dpThread)
    } finally {
      dpThread.stop()
    }
  }

  private def awaitUntil(cond: => Boolean, msg: => String, budgetMs: Int = 5000): Unit =
    assert(awaitCond(cond, budgetMs), msg)

  private def dataFrameOf(count: Int, seq: Long = 0): FIFOMessageElement =
    FIFOMessageElement(WorkflowFIFOMessage(dataChannelId, seq, DataFrame(tuples.slice(0, count))))

  private def expectTuplesProcessed(): Unit = {
    tuples.foreach { x =>
      ((tuple: Tuple, input: Int) => executor.processTupleMultiPort(tuple, input)).expects(x, 0)
    }
  }

  private def runPauseResumeTest(
      pauseElement: DPInputQueueElement,
      resumeElement: DPInputQueueElement
  ): Unit = {
    expectTuplesProcessed()
    withStartedDPThread(dpFactory = q => newDataProcessor(q, executor = executor)) {
      (inputQueue, dp, _) =>
        inputQueue.put(FIFOMessageElement(WorkflowFIFOMessage(dataChannelId, 0, DataFrame(tuples))))
        inputQueue.put(pauseElement)
        Thread.sleep(1000)
        assert(dp.pauseManager.isPaused)
        inputQueue.put(resumeElement)
        Thread.sleep(1000)
        while (dp.inputManager.hasUnfinishedInput) {
          Thread.sleep(100)
        }
    }
  }

  private def feedMultiSourceAndWait(
      logMgr: ReplayLogManager,
      pauseBetweenBatches: Boolean
  )(waitFor: (DataProcessor, ChannelIdentity) => Unit): Unit = {
    val anotherSenderWorkerId = ActorVirtualIdentity("another")
    val dataChannelId2 = ChannelIdentity(anotherSenderWorkerId, workerId, isControl = false)
    expectTuplesProcessed()
    withStartedDPThread(
      dpFactory =
        q => newDataProcessor(q, executor = executor, extraSender = Some(anotherSenderWorkerId)),
      logMgr = logMgr
    ) { (inputQueue, dp, _) =>
      Seq(
        WorkflowFIFOMessage(dataChannelId, 0, DataFrame(tuples.slice(0, 100))),
        WorkflowFIFOMessage(dataChannelId, 1, DataFrame(tuples.slice(100, 200))),
        WorkflowFIFOMessage(dataChannelId2, 0, DataFrame(tuples.slice(300, 1000)))
      ).foreach(m => inputQueue.put(FIFOMessageElement(m)))
      if (pauseBetweenBatches) Thread.sleep(1000)
      Seq(
        WorkflowFIFOMessage(dataChannelId, 2, DataFrame(tuples.slice(200, 300))),
        WorkflowFIFOMessage(dataChannelId2, 1, DataFrame(tuples.slice(1000, 5000)))
      ).foreach(m => inputQueue.put(FIFOMessageElement(m)))
      Thread.sleep(1000)
      waitFor(dp, dataChannelId2)
    }
  }

  private def assertMidBatchStall(
      triggerStall: (DataProcessor, LinkedBlockingQueue[DPInputQueueElement]) => Unit,
      verifyStallPickedUp: (
          DataProcessor,
          DPThread,
          LinkedBlockingQueue[DPInputQueueElement]
      ) => Unit,
      liftStall: LinkedBlockingQueue[DPInputQueueElement] => Unit,
      stallLabel: String
  ): Unit = {
    val processed = new AtomicInteger(0)
    val firstTuple = new CountDownLatch(1)
    val gate = new CountDownLatch(1)
    withStartedDPThread(onTuple = (_, _) => {
      if (processed.getAndIncrement() == 0) {
        firstTuple.countDown()
        gate.await()
      }
      Iterator.empty
    }) { (inputQueue, dp, dpThread) =>
      inputQueue.put(dataFrameOf(2000))
      assert(firstTuple.await(5, TimeUnit.SECONDS), "batch must start")
      triggerStall(dp, inputQueue)
      gate.countDown()
      verifyStallPickedUp(dp, dpThread, inputQueue)

      Thread.sleep(400)
      assert(
        dp.inputManager.hasUnfinishedInput,
        s"a $stallLabel worker must not finish its batch, processed ${processed.get()} of 2000"
      )
      val stalledAt = processed.get()
      Thread.sleep(400)
      assert(
        processed.get() == stalledAt,
        s"no further tuple may be taken while $stallLabel, went $stalledAt -> ${processed.get()}"
      )

      liftStall(inputQueue)
      awaitUntil(processed.get() == 2000, s"drained ${processed.get()} of 2000")
    }
  }

  "DP Thread" should "handle pause/resume during processing" in {
    runPauseResumeTest(
      TimerBasedControlElement(
        ControlInvocation(METHOD_PAUSE_WORKER, EmptyRequest(), AsyncRPCContext(SELF, SELF), 0)
      ),
      TimerBasedControlElement(
        ControlInvocation(METHOD_RESUME_WORKER, EmptyRequest(), AsyncRPCContext(SELF, SELF), 1)
      )
    )
  }

  "DP Thread" should "handle pause/resume using fifo messages" in {
    runPauseResumeTest(
      FIFOMessageElement(
        WorkflowFIFOMessage(
          controlChannelId,
          0,
          ControlInvocation(METHOD_PAUSE_WORKER, EmptyRequest(), AsyncRPCContext(SELF, SELF), 0)
        )
      ),
      FIFOMessageElement(
        WorkflowFIFOMessage(
          controlChannelId,
          1,
          ControlInvocation(METHOD_RESUME_WORKER, EmptyRequest(), AsyncRPCContext(SELF, SELF), 1)
        )
      )
    )
  }

  "DP Thread" should "handle multiple batches from multiple sources" in {
    feedMultiSourceAndWait(logManager, pauseBetweenBatches = false) { (dp, _) =>
      while (dp.inputManager.hasUnfinishedInput) {
        Thread.sleep(100)
      }
    }
  }

  "DP Thread" should "write determinant logs to local storage while processing" in {
    val logStorage = SequentialRecordStorage.getStorage[ReplayLogRecord](
      Some(new URI("ram:///recovery-logs/tmp"))
    )
    logStorage.deleteStorage()
    val localLogManager: ReplayLogManager =
      ReplayLogManager.createLogManager(logStorage, "tmpLog", _ => {})
    var dataChannelId2: ChannelIdentity = null
    feedMultiSourceAndWait(localLogManager, pauseBetweenBatches = true) { (_, ch2) =>
      dataChannelId2 = ch2
      while (localLogManager.getStep < 4999) {
        Thread.sleep(100)
      }
      localLogManager.sendCommitted(null) // drain in-mem records to flush
      localLogManager.terminate()
    }
    val logs = logStorage.getReader("tmpLog").mkRecordIterator().toArray
    logStorage.deleteStorage()
    assert(logs.length > 1)
    // A determinant is only useful if it names the channel the step actually came
    // from -- that is the channel replay resumes on. Only these two data channels
    // were fed, so only these two ids may appear.
    val loggedChannels = logs.collect { case ProcessingStep(cid, _) => cid }.toSet
    assert(
      loggedChannels == Set(dataChannelId, dataChannelId2),
      s"determinants must name the channels actually processed, got $loggedChannels"
    )
  }

  "DP Thread" should "seed the base URI so create() yields execution-scoped keys" in {
    val eid = 7777L
    val baseUri = LargeBinaryManager.baseUriForExecution(eid)
    // create() runs on the DP thread; capture what it produces there.
    val capturedUri = new CompletableFuture[String]()
    withStartedDPThread(
      onTuple = (_, _) => {
        capturedUri.complete(LargeBinaryManager.create())
        Iterator.empty
      },
      baseUriOpt = Some(baseUri)
    ) { (inputQueue, _, _) =>
      inputQueue.put(
        FIFOMessageElement(WorkflowFIFOMessage(dataChannelId, 0, DataFrame(Array(tuples(0)))))
      )

      val uri = capturedUri.get(5, TimeUnit.SECONDS)
      assert(uri.startsWith(s"s3://${LargeBinaryManager.DEFAULT_BUCKET}/objects/$eid/"))
      // a unique suffix is appended to the execution-scoped base URI
      assert(uri.length > baseUri.length)
    }
  }

  "DP Thread" should "toggle backpressureStatus on Backpressure and ignore other ActorCommands" in {
    // Single-threaded: exercise handleActorCommand directly, without start(), so the
    // read of backpressureStatus is on the same thread that mutated it.
    val q = new LinkedBlockingQueue[DPInputQueueElement]()
    val dp = new DataProcessor(workerId, _ => {}, inputMessageQueue = q)
    val dpThread = new DPThread(workerId, dp, logManager, q)

    dpThread.handleActorCommand(Backpressure(enableBackpressure = true))
    assert(dpThread.backpressureStatus)

    dpThread.handleActorCommand(Backpressure(enableBackpressure = false))
    assert(!dpThread.backpressureStatus)

    // CreditUpdate is the other ActorCommand oneof subtype. It falls through to the
    // `case _ => // no op` arm and must leave backpressureStatus untouched.
    dpThread.handleActorCommand(CreditUpdate())
    assert(!dpThread.backpressureStatus)
  }

  "DP Thread" should "treat a second start() as a no-op" in {
    withStartedDPThread(dpFactory = q => {
      val dp = new DataProcessor(workerId, x => {}, inputMessageQueue = q)
      dp.adaptiveBatchingMonitor = mock[WorkerTimerService]
      dp
    }) { (_, _, dpThread) =>
      val executorAfterFirstStart = dpThread.dpThreadExecutor
      val futureAfterFirstStart = dpThread.dpThread
      // The second start() should log "already running" and change nothing:
      // no new executor, no second worker thread.
      dpThread.start()
      assert(dpThread.dpThreadExecutor eq executorAfterFirstStart)
      assert(dpThread.dpThread eq futureAfterFirstStart)
    }
  }

  "DP Thread" should "forward an uncaught processing error to the main thread" in {
    // A processing error escaping the main logic must be caught in run() and delegated
    // back to the main thread as a Left(MainThreadDelegateMessage). We throw from
    // DataProcessor.processDataPayload (not from a mock executor: DataProcessor swallows
    // executor exceptions in handleExecutorException, so they never reach DPThread).
    val captured =
      new CompletableFuture[Either[MainThreadDelegateMessage, WorkflowFIFOMessage]]()
    val outputHandler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit =
      e => captured.complete(e)
    withStartedDPThread(dpFactory =
      q =>
        newDataProcessor(
          q,
          dp = new DataProcessor(workerId, outputHandler, inputMessageQueue = q) {
            override def processDataPayload(
                channelId: ChannelIdentity,
                dataPayload: DataPayload
            ): Unit = throw new RuntimeException("boom")
          }
        )
    ) { (inputQueue, _, _) =>
      inputQueue.put(
        FIFOMessageElement(WorkflowFIFOMessage(dataChannelId, 0, DataFrame(Array(tuples(0)))))
      )
      captured.get(5, TimeUnit.SECONDS) match {
        case Left(MainThreadDelegateMessage(closure)) =>
          // Running the delegate on the main thread must re-throw the original processing error.
          val thrown = intercept[RuntimeException](closure(null))
          assert(thrown.getMessage == "boom")
        case other => fail(s"expected Left(MainThreadDelegateMessage), got $other")
      }
    }
  }

  "DP Thread" should "treat stop() before start() as a no-op" in {
    // stop() is also reached on the teardown path of a worker that never started
    // (e.g. an actor that fails during initialization). Neither the executor
    // service nor the thread future exists yet, so both guards must short-circuit
    // rather than dereference a null.
    val inputQueue = new LinkedBlockingQueue[DPInputQueueElement]()
    val dp = new DataProcessor(workerId, x => {}, inputMessageQueue = inputQueue)
    val dpThread = new DPThread(workerId, dp, logManager, inputQueue)

    // The entire contract on this path is that the call does not blow up. Asserting
    // that the two fields are null would assert nothing: stop() only ever reads them,
    // so `dpThread == null` is the field initializer talking, not production code, and
    // it would hold even if stop()'s body were deleted. Flipping either guard to
    // `== null` makes this call NPE, and that is what this test detects.
    noException should be thrownBy dpThread.stop()
  }

  "DP Thread" should "not return from stop() until the DP thread has left the main loop" in {
    // stop() must be a synchronous join: WorkflowWorker tears down the output gateway
    // and the statistics manager right after it returns, so the DP thread must already
    // be out of run() by then. The join is `endFuture.get()`, and nothing else in the
    // suite constrains it. We park the DP thread in a NON-interruptible spin (an
    // interruptible wait would be swallowed by DataProcessor.handleExecutorException
    // and let stop() legitimately return early) and check that stop() blocks.
    val release = new AtomicBoolean(false)
    val inSpin = new CountDownLatch(1)
    try {
      withStartedDPThread(onTuple = (_, _) => {
        inSpin.countDown()
        while (!release.get()) { Thread.onSpinWait() }
        Iterator.empty
      }) { (inputQueue, _, dpThread) =>
        inputQueue.put(dataFrameOf(4))
        assert(inSpin.await(5, TimeUnit.SECONDS), "the DP thread must be inside the executor")
        val stopReturned = new CountDownLatch(1)
        val stopper = new Thread(() => {
          try dpThread.stop()
          finally stopReturned.countDown()
        })
        stopper.setDaemon(true)
        stopper.start()
        assert(
          !stopReturned.await(500, TimeUnit.MILLISECONDS),
          "stop() must not return while the DP thread is still inside run()"
        )
        release.set(true)
        assert(
          stopReturned.await(10, TimeUnit.SECONDS),
          "stop() must return once the DP thread exits"
        )
      }
    } finally {
      release.set(true)
    }
  }

  "DP Thread" should "park when idle and exit quietly when stop() interrupts it there" in {
    // Two properties of the same moment. (1) An idle DP thread must PARK on
    // internalQueue.take rather than spin: input selection sets `waitingForInput = true`
    // when no channel can be picked, and that flag is the only thing that sends the loop
    // back into the blocking take. Thread.State is the only way to tell parking from
    // spinning from outside. (2) stop() then interrupts the DP thread out of that take,
    // and run() must treat the InterruptedException as an ordinary teardown -- log it and
    // nothing more. The other catch arm delegates the error back to the worker actor, and
    // with the usual swallowing output handler the two are indistinguishable, so record
    // what the handler receives instead of dropping it.
    val delegates = new ConcurrentLinkedQueue[MainThreadDelegateMessage]()
    // processTuple runs ON the DP thread, so this is how we get hold of that exact
    // thread. Every DPThread names its thread "DP-thread" and earlier tests in this
    // suite leave theirs alive, so matching by name would be ambiguous.
    val dpWorkerThread = new AtomicReference[Thread]()
    val idleState = () => Option(dpWorkerThread.get()).map(_.getState)
    withStartedDPThread(
      onTuple = (_, _) => {
        dpWorkerThread.set(Thread.currentThread())
        Iterator.empty
      },
      dpFactory = q =>
        newDataProcessor(
          q,
          {
            case Left(m) => delegates.add(m)
            case _       => ()
          }
        )
    ) { (inputQueue, _, dpThread) =>
      inputQueue.put(dataFrameOf(1))
      awaitUntil(
        idleState().contains(Thread.State.WAITING),
        s"an idle DP thread must park on take, was ${idleState()}"
      )
      // stop() joins the DP thread, so run() has returned by the time this returns.
      dpThread.stop()
      assert(
        delegates.isEmpty,
        s"an interrupted teardown must not delegate an error, got ${delegates.peek()}"
      )
    }
  }

  "DP Thread" should "hold back data intake while a queued Backpressure command is in effect" in {
    // Backpressure delivered through the internal queue as an ActorCommandElement,
    // which is how the worker actor actually sends it. While it is on, the DP
    // thread must stop taking data, so a data frame that arrives afterwards stays
    // queued in its channel and no tuple is processed until backpressure is lifted.
    val processed = new AtomicInteger(0)
    withStartedDPThread(onTuple = (_, _) => {
      processed.incrementAndGet()
      Iterator.empty
    }) { (inputQueue, _, dpThread) =>
      inputQueue.put(ActorCommandElement(Backpressure(enableBackpressure = true)))
      // `backpressureStatus` is a plain (non-volatile) var written by the DP thread, so
      // reading it from here needs a happens-before edge. CreditUpdate is inert -- it
      // falls through handleActorCommand's `case _ => // no op` arm -- and observing the
      // queue drained is a read of LinkedBlockingQueue's AtomicInteger count, which the
      // DP thread decremented after it had already handled the Backpressure element.
      inputQueue.put(ActorCommandElement(CreditUpdate()))
      awaitUntil(inputQueue.isEmpty, "the queued commands must be consumed")
      assert(dpThread.backpressureStatus, "backpressure must be picked up")

      inputQueue.put(dataFrameOf(200))
      assert(
        !awaitCond(processed.get() != 0, budgetMs = 1000),
        "no data may be taken while backpressured"
      )
      // Deliberately no assertion about control messages in this state: the
      // input-selection expression reads
      //   if (backpressureStatus) { tryPickControlChannel } else { tryPickChannel } match {...}
      // and Scala binds `match` to the else branch alone, so while backpressure is
      // on the picked channel is discarded. Asserting either outcome here would
      // freeze that behaviour in place, so this test only pins the data hold-back.

      inputQueue.put(ActorCommandElement(Backpressure(enableBackpressure = false)))
      awaitUntil(processed.get() == 200, s"drained ${processed.get()} of 200")
    }
  }

  "DP Thread" should "stall the remainder of a batch when backpressure arrives mid-batch" in {
    // Same switch, but flipped while a batch is half-consumed. Here the input
    // selection takes the "unfinished input" path, so it is the pause/backpressure
    // guard inside that path that has to hold the batch back.
    assertMidBatchStall(
      triggerStall = (_, inputQueue) => {
        // Queue the switch while the DP thread is parked inside the executor, so it
        // is consumed on the very next loop iteration, mid-batch.
        inputQueue.put(ActorCommandElement(Backpressure(enableBackpressure = true)))
        // Inert trailing command, as above: draining it gives the happens-before edge
        // that makes the non-volatile `backpressureStatus` read below well-defined.
        inputQueue.put(ActorCommandElement(CreditUpdate()))
      },
      verifyStallPickedUp = (_, dpThread, inputQueue) => {
        awaitUntil(inputQueue.isEmpty, "the queued commands must be consumed")
        assert(dpThread.backpressureStatus, "backpressure must be picked up")
      },
      liftStall = _.put(ActorCommandElement(Backpressure(enableBackpressure = false))),
      stallLabel = "backpressured"
    )
  }

  "DP Thread" should "stall the remainder of a batch when a pause arrives mid-batch" in {
    // The sibling of the backpressure case above, on the OTHER operand of the same
    // guard: `if (!dp.pauseManager.isPaused && !backpressureStatus)`. Nothing below the
    // DP thread re-checks the pause -- InputManager.getNextTuple, hasUnfinishedInput and
    // DataProcessor.continueDataProcessing have no pause check, and PauseManager.pause
    // only disables the data channels, which stops NEW frames, not the current one --
    // so this guard is the entire mid-batch pause contract.
    assertMidBatchStall(
      triggerStall = (_, inputQueue) =>
        // Queued while the DP thread is parked inside the executor, so it is picked up on
        // the very next loop iteration -- with 1999 tuples of the batch still to go.
        inputQueue.put(
          TimerBasedControlElement(
            ControlInvocation(METHOD_PAUSE_WORKER, EmptyRequest(), AsyncRPCContext(SELF, SELF), 0)
          )
        ),
      verifyStallPickedUp =
        (dp, _, _) => awaitUntil(dp.pauseManager.isPaused, "pause must be picked up"),
      liftStall = _.put(
        TimerBasedControlElement(
          ControlInvocation(METHOD_RESUME_WORKER, EmptyRequest(), AsyncRPCContext(SELF, SELF), 1)
        )
      ),
      stallLabel = "paused"
    )
  }

  "DP Thread" should "leave the main loop on the stopped flag without draining the batch" in {
    // The graceful exit: stop() sets `stopped` and interrupts, but the DP thread is
    // busy in the executor rather than blocked on the queue, so it never sees an
    // InterruptedException and has to fall out of the main loop on the flag alone,
    // abandoning the rest of the batch.
    val processed = new AtomicInteger(0)
    val firstTuple = new CountDownLatch(1)
    withStartedDPThread(onTuple = (_, _) => {
      processed.incrementAndGet()
      firstTuple.countDown()
      // A non-interruptible spin: ~0.5ms of work per tuple, so 2000 tuples take
      // about a second while stop() lands after ~100ms.
      val until = System.nanoTime() + 500000L
      while (System.nanoTime() < until) {}
      Iterator.empty
    }) { (inputQueue, dp, dpThread) =>
      inputQueue.put(dataFrameOf(2000))
      assert(firstTuple.await(5, TimeUnit.SECONDS), "batch must start")
      Thread.sleep(100)
      dpThread.stop()
      val done = processed.get()
      assert(done < 2000, s"stop() must abandon the rest of the batch, processed $done of 2000")
      assert(dp.inputManager.hasUnfinishedInput, "the abandoned batch is left unfinished")
    }
  }

  "DP Thread" should "keep draining a pending output backlog after the input batch is exhausted" in {
    // One input tuple fanning out to 50 output tuples: after the single tuple is
    // consumed there is no unfinished input and no pause, so only the pending
    // output backlog can keep the loop calling continueDataProcessing().
    val fanOut = 50
    withStartedDPThread(
      onTuple = (_, _) => (0 until fanOut).iterator.map(i => TupleLike(i)),
      dpFactory = q => {
        val dp = newDataProcessor(q)
        dp.outputManager.addPort(PortIdentity(1), schema, None)
        dp
      }
    ) { (inputQueue, dp, _) =>
      inputQueue.put(dataFrameOf(1))
      // Fixture guard, sampled DURING the drain: processDataPayload consumes the whole
      // one-tuple frame in the same step that sets up the output iterator, so input is
      // already exhausted when the first output tuple appears. Checked here rather than
      // at the end because at the end it holds for any frame size -- it would not
      // notice the frame growing and the output clause silently going untested.
      awaitUntil(
        dp.statisticsManager.getOutputTupleCount >= 1L,
        "the first output tuple must be emitted"
      )
      assert(
        !dp.inputManager.hasUnfinishedInput,
        "input must already be exhausted while output tuples are still pending"
      )
      awaitUntil(
        dp.statisticsManager.getOutputTupleCount == fanOut.toLong,
        s"emitted ${dp.statisticsManager.getOutputTupleCount} of $fanOut output tuples"
      )
    }
  }

}
