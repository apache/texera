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

package org.apache.texera.amber.engine.architecture.logreplay

import com.google.common.collect.Queues
import org.apache.texera.common.config.ApplicationConfig
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.MainThreadDelegateMessage
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.storage.SequentialRecordStorage.SequentialRecordWriter

import java.util
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters.ListHasAsScala

class AsyncReplayLogWriter(
    handler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit,
    writer: SequentialRecordWriter[ReplayLogRecord]
) extends Thread {
  private val drained =
    new util.ArrayList[
      Either[ReplayLogRecord, Either[MainThreadDelegateMessage, WorkflowFIFOMessage]]
    ]()
  private val writerQueue =
    Queues.newLinkedBlockingQueue[
      Either[ReplayLogRecord, Either[MainThreadDelegateMessage, WorkflowFIFOMessage]]
    ]()
  private val terminationEntry = Left(TerminateSignal)
  private var stopped = false
  private val logInterval =
    ApplicationConfig.faultToleranceLogFlushIntervalInMs
  private val gracefullyStopped = new CompletableFuture[Unit]()

  def putLogRecords(records: Array[ReplayLogRecord]): Unit = {
    assert(!stopped)
    records.foreach(x => {
      writerQueue.put(Left(x))
    })
  }

  def putOutput(output: Either[MainThreadDelegateMessage, WorkflowFIFOMessage]): Unit = {
    assert(!stopped)
    writerQueue.put(Right(output))
  }

  def terminate(): Unit = {
    stopped = true
    writerQueue.put(terminationEntry)
    gracefullyStopped.get()
  }

  override def run(): Unit = {
    var internalStop = false
    while (!internalStop) {
      if (logInterval > 0) {
        Thread.sleep(logInterval)
      }
      internalStop = drainWriterQueueAndProcess()
    }
    writer.close()
    gracefullyStopped.complete(())
  }

  private def drainWriterQueueAndProcess(): Boolean = {
    if (writerQueue.drainTo(drained) == 0) {
      drained.add(writerQueue.take())
    }
    val lastIndex = drained.size() - 1
    val stop = drained.get(lastIndex) == terminationEntry
    // Keep the drained entries intact until all writes and callbacks succeed.
    val batch = (if (stop) drained.subList(0, lastIndex) else drained).asScala

    batch.foreach {
      case Left(record) => writer.writeRecord(record)
      case Right(_)     => ()
    }
    writer.flush()
    batch.foreach {
      case Left(_)        => ()
      case Right(message) => handler(message)
    }

    drained.clear()
    stop
  }

}
