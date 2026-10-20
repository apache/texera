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

package org.apache.texera.amber.engine.common

import com.twitter.util.{Duration, Time, Timer, TimerTask}

import scala.collection.mutable

/**
  * A `Timer` that records the delay of every task scheduled on it and then runs the task inline
  * instead of waiting.
  *
  * Use it inside `Time.withCurrentTimeFrozen` so that `when - Time.now` is exactly the delay
  * `Future.sleep` asked for. That makes a backoff schedule (e.g. `Utils.retry`'s) assertable
  * exactly, on the test thread, without the test spending that time asleep.
  */
class RecordingInlineTimer extends Timer {
  private val delays: mutable.ArrayBuffer[Duration] = mutable.ArrayBuffer()

  def recordedDelays: Seq[Duration] = delays.toSeq

  def recordedDelaysInMillis: Seq[Long] = delays.map(_.inMilliseconds).toSeq

  def scheduleOnce(when: Time)(f: => Unit): TimerTask = {
    delays += (when - Time.now)
    f
    new TimerTask { def cancel(): Unit = () }
  }

  def schedulePeriodically(when: Time, period: Duration)(f: => Unit): TimerTask =
    throw new AssertionError("retry backoff must not schedule periodic tasks")

  def stop(): Unit = ()
}
