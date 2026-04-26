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

package org.apache.texera.dao

import org.scalatest.{Args, Status, Suite}

import java.util.concurrent.Semaphore

/**
  * Mix into any test suite that touches a shared resource (the singleton
  * EmbeddedPostgres in `MockTexeraDB`, the CI's `texera_db_for_test_cases`
  * service, or the shared Iceberg JDBC catalog). Suites mixing this in run
  * one at a time relative to each other; suites that do not mix it in are
  * free to run in parallel.
  *
  * The lock is held around the entire `Suite.run` invocation, so it works
  * even for specs whose `beforeAll` overrides do not call `super.beforeAll`.
  */
trait SerializedSuite extends Suite {

  abstract override def run(testName: Option[String], args: Args): Status = {
    SerializedSuite.lock.acquire()
    val status =
      try super.run(testName, args)
      catch {
        case t: Throwable =>
          SerializedSuite.lock.release()
          throw t
      }
    status.whenCompleted(_ => SerializedSuite.lock.release())
    status
  }
}

object SerializedSuite {
  private val lock = new Semaphore(1, true)
}
