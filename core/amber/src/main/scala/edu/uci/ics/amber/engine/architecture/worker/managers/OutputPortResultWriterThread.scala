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

package edu.uci.ics.amber.engine.architecture.worker.managers

import com.google.common.collect.Queues
import edu.uci.ics.amber.core.storage.model.BufferedItemWriter
import edu.uci.ics.amber.core.tuple.Tuple

import java.util.concurrent.LinkedBlockingQueue

sealed trait TerminateSignal
case object PortStorageWriterTerminateSignal extends TerminateSignal

class OutputPortResultWriterThread(
    bufferedItemWriter: BufferedItemWriter[Tuple]
) extends Thread {

  val queue: LinkedBlockingQueue[Either[Tuple, TerminateSignal]] =
    Queues.newLinkedBlockingQueue[Either[Tuple, TerminateSignal]]()

  override def run(): Unit = {
    var internalStop = false
    while (!internalStop) {
      val queueContent = queue.take()
      queueContent match {
        case Left(tuple) => bufferedItemWriter.putOne(tuple)
        case Right(_)    => internalStop = true
      }
    }
    bufferedItemWriter.close()
  }
}
