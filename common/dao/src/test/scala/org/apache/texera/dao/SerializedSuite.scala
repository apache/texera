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

import org.scalatest.{BeforeAndAfterAll, Suite}

import java.util.concurrent.Semaphore

/**
  * Mix into any test suite that touches a shared database (the singleton
  * EmbeddedPostgres in `MockTexeraDB`, the CI's `texera_db_for_test_cases`
  * service, or the shared Iceberg JDBC catalog). The trait acquires a
  * JVM-wide semaphore in `beforeAll` and releases it in `afterAll`, so all
  * suites mixing it in run one at a time relative to each other while
  * non-DB suites in the build are free to run in parallel.
  *
  * This is the alternative to setting `Global / concurrentRestrictions +=
  * Tags.limit(Tags.Test, 1)`, which serialised every test in the build.
  */
trait SerializedSuite extends BeforeAndAfterAll { this: Suite =>

  override protected def beforeAll(): Unit = {
    SerializedSuite.lock.acquire()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally SerializedSuite.lock.release()
  }
}

object SerializedSuite {
  private val lock = new Semaphore(1, true)
}
