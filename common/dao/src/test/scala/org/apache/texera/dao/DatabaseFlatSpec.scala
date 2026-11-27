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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.texera.dao

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Shared ScalaTest FlatSpec base managing [[MockTexeraDB]] suite initialization and cleanup hooks. */
abstract class DatabaseFlatSpec(
    private val cleanupBeforeEach: Boolean = true,
    private val cleanupAfterEach: Boolean = false,
    private val cleanupAfterAll: Boolean = false
) extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  protected def onBeforeAll(): Unit = ()
  protected def onBeforeEach(): Unit = ()
  protected def onAfterAll(): Unit = ()
  protected def cleanup(): Unit = ()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    initializeDBAndReplaceDSLContext()
    onBeforeAll()
  }

  override protected def afterAll(): Unit = {
    try {
      if (cleanupAfterAll) cleanup()
      onAfterAll()
    } finally {
      try closeConnectionPool()
      finally super.afterAll()
    }
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    if (cleanupBeforeEach) cleanup()
    onBeforeEach()
  }

  override protected def afterEach(): Unit = {
    try {
      if (cleanupAfterEach) cleanup()
    } finally super.afterEach()
  }
}
