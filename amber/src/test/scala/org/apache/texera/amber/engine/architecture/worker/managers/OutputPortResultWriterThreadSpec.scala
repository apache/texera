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

package org.apache.texera.amber.engine.architecture.worker.managers

import org.apache.texera.amber.core.storage.model.BufferedItemWriter
import org.apache.texera.amber.core.tuple.Tuple
import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.engine.architecture.messaginglayer.{
  NetworkOutputGateway,
  OutputManager
}
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable

class OutputPortResultWriterThreadSpec extends AnyFlatSpec {

  private class StubWriter(
      onPutOne: () => Unit = () => (),
      onClose: () => Unit = () => ()
  ) extends BufferedItemWriter[Tuple] {
    val bufferSize: Int = 1024
    var closeCalled = false
    def open(): Unit = ()
    def putOne(item: Tuple): Unit = onPutOne()
    def removeOne(item: Tuple): Unit = ()
    def close(): Unit = {
      closeCalled = true
      onClose()
    }
  }

  private def throwing(msg: String): () => Unit = () => throw new RuntimeException(msg)

  "OutputPortResultWriterThread" should "leave getFailure empty on a clean run" in {
    val writer = new StubWriter()
    val thread = new OutputPortResultWriterThread(writer)
    thread.start()
    thread.queue.put(Right(PortStorageWriterTerminateSignal))
    thread.join()
    assert(thread.getFailure.isEmpty)
    assert(writer.closeCalled)
  }

  it should "capture a close() exception in getFailure so the worker can re-throw" in {
    val writer = new StubWriter(onClose = throwing("test close failure"))
    val thread = new OutputPortResultWriterThread(writer)
    thread.start()
    thread.queue.put(Right(PortStorageWriterTerminateSignal))
    thread.join()
    assert(thread.getFailure.exists(_.getMessage.contains("test close failure")))
    assert(writer.closeCalled)
  }

  it should "capture a putOne exception and still call close()" in {
    val writer = new StubWriter(onPutOne = throwing("test putOne failure"))
    val thread = new OutputPortResultWriterThread(writer)
    thread.start()
    thread.queue.put(Left(null.asInstanceOf[Tuple]))
    thread.queue.put(Right(PortStorageWriterTerminateSignal))
    thread.join()
    assert(thread.getFailure.exists(_.getMessage.contains("test putOne failure")))
    // The finally clause must run close() even after putOne threw, or
    // the underlying writer leaks file handles.
    assert(writer.closeCalled)
  }

  it should "preserve both errors when putOne and close() fail in the same run" in {
    val writer = new StubWriter(
      onPutOne = throwing("test putOne failure"),
      onClose = throwing("test close failure")
    )
    val thread = new OutputPortResultWriterThread(writer)
    thread.start()
    thread.queue.put(Left(null.asInstanceOf[Tuple]))
    thread.queue.put(Right(PortStorageWriterTerminateSignal))
    thread.join()
    val captured = thread.getFailure.getOrElse(fail("expected putOne failure"))
    assert(captured.getMessage.contains("test putOne failure"))
    assert(
      captured.getSuppressed.exists(_.getMessage.contains("test close failure")),
      "close() failure should be attached as suppressed on the original putOne failure"
    )
  }

  // Reach into OutputManager's private outputPortResultWriterThreads map to
  // install a writer thread whose close() has already failed. This pins the
  // contract that closeOutputStorageWriterIfNeeded re-throws the captured
  // failure, which is the bridge from the writer thread to the DP thread →
  // worker actor → controller supervisor → FatalError to client.
  private def installWriterThread(
      manager: OutputManager,
      portId: PortIdentity,
      thread: OutputPortResultWriterThread
  ): Unit = {
    val field = classOf[OutputManager]
      .getDeclaredField("outputPortResultWriterThreads")
    field.setAccessible(true)
    field
      .get(manager)
      .asInstanceOf[mutable.HashMap[PortIdentity, OutputPortResultWriterThread]]
      .put(portId, thread)
  }

  "OutputManager.closeOutputStorageWriterIfNeeded" should
    "re-throw the writer thread's captured failure" in {
    val identifier = ActorVirtualIdentity("test-worker")
    val outputManager = new OutputManager(
      identifier,
      new NetworkOutputGateway(identifier, (_: WorkflowFIFOMessage) => ())
    )
    val portId = PortIdentity()
    val failingWriter = new StubWriter(onClose = throwing("test close failure"))
    val failingThread = new OutputPortResultWriterThread(failingWriter)
    failingThread.start()
    installWriterThread(outputManager, portId, failingThread)
    val ex = intercept[RuntimeException] {
      outputManager.closeOutputStorageWriterIfNeeded(portId)
    }
    assert(ex.getMessage.contains("test close failure"))
  }

  it should "be a no-op when the port has no writer thread" in {
    val identifier = ActorVirtualIdentity("test-worker")
    val outputManager = new OutputManager(
      identifier,
      new NetworkOutputGateway(identifier, (_: WorkflowFIFOMessage) => ())
    )
    // No installWriterThread call — the port has never had a writer.
    outputManager.closeOutputStorageWriterIfNeeded(PortIdentity())
  }
}
