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

package org.apache.texera.common.util

import scala.annotation.tailrec
import scala.util.control.NonFatal

/**
  * Retry with exponential backoff, for blocking work.
  *
  * Every module can reach this one, so a new retry loop should not be written by hand. If the work
  * returns a `Future` rather than blocking, use the non-blocking sibling
  * `org.apache.texera.amber.engine.common.Utils.retry` in `amber` instead: it takes the same
  * attempts-and-doubling-backoff knobs but waits on a `Timer`, which matters on an actor or
  * coordinator thread where `Thread.sleep` would stall unrelated work queued behind it.
  */
object RetryUtil {

  /**
    * One failed attempt that is about to be retried. Carries everything a caller needs to log the
    * retry itself; `message` is the standard wording, so retries read the same everywhere.
    */
  final case class RetryAttempt(
      description: String,
      attempt: Int,
      maxAttempts: Int,
      delayMillis: Long,
      cause: Throwable
  ) {
    def message: String =
      s"Failed to $description (attempt $attempt/$maxAttempts): ${cause.getMessage}. " +
        s"Retrying in ${delayMillis}ms..."
  }

  /**
    * Runs `operation`, retrying on failure with exponential backoff (the delay doubles after each
    * failed attempt) until it succeeds or `maxAttempts` is reached. The final failure is wrapped
    * with `description` and the last exception as its cause.
    *
    * Only `NonFatal` failures are treated as transient. An `InterruptedException` -- raised by the
    * operation or by the wait between attempts -- fails fast with the interrupt status restored,
    * so a caller shutting the thread down is never made to sit through the remaining backoff.
    *
    * @param description        verb phrase naming the work, e.g. "connect to lake fs server". It is
    *                           interpolated into every message: "Failed to $description after ...".
    * @param maxAttempts        total attempts; 1 means no retry at all.
    * @param initialDelayMillis wait before the first retry; doubled after each failed attempt.
    * @param onRetry            invoked before each wait. Log `RetryAttempt.message` through the
    *                           caller's own logger, so retries are attributed to the caller rather
    *                           than to this util.
    * @param sleep              how to wait; injectable so tests exercise the backoff without waiting.
    * @param operation          the work to run, re-evaluated on each attempt.
    * @tparam T whatever `operation` returns.
    * @return `operation`'s value from the first attempt that succeeds.
    */
  def withBackoff[T](
      description: String,
      maxAttempts: Int,
      initialDelayMillis: Long,
      onRetry: RetryAttempt => Unit,
      sleep: Long => Unit = Thread.sleep
  )(operation: => T): T = {
    // Restore the interrupt status and fail fast rather than retrying, whether the interrupt
    // arrives while running `operation` or while waiting between attempts.
    def failInterrupted(cause: InterruptedException): Nothing = {
      Thread.currentThread().interrupt()
      throw new RuntimeException(s"Interrupted while waiting to $description", cause)
    }

    @tailrec
    def attemptFrom(attempt: Int, delayMillis: Long): T = {
      val outcome: Either[Throwable, T] =
        try Right(operation)
        catch {
          case ie: InterruptedException => failInterrupted(ie)
          case NonFatal(cause)          => Left(cause)
        }

      outcome match {
        case Right(value) => value
        case Left(cause) =>
          if (attempt >= maxAttempts) {
            throw new RuntimeException(
              s"Failed to $description after $maxAttempts attempts: ${cause.getMessage}",
              cause
            )
          }
          onRetry(RetryAttempt(description, attempt, maxAttempts, delayMillis, cause))
          try sleep(delayMillis)
          catch { case ie: InterruptedException => failInterrupted(ie) }
          attemptFrom(attempt + 1, delayMillis * 2)
      }
    }

    attemptFrom(attempt = 1, delayMillis = initialDelayMillis)
  }
}
