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

package org.apache.texera.service.util

import org.scalatest.funsuite.AnyFunSuite

/**
  * Unit tests for [[LargeBinaryManager.deleteByExecution]] with the directory-delete op
  * injected, so success and the swallow-and-log error path run without a live S3 endpoint.
  */
class LargeBinaryManagerUnitSpec extends AnyFunSuite {

  test("deleteByExecution issues a delete scoped to the execution's object prefix") {
    var captured: Option[(String, String)] = None
    LargeBinaryManager.deleteByExecution(
      42L,
      (bucket, prefix) => captured = Some((bucket, prefix))
    )
    assert(captured.contains((LargeBinaryManager.DEFAULT_BUCKET, "objects/42")))
  }

  test("deleteByExecution swallows exceptions raised by the underlying delete") {
    // The error path logs and returns; it must not propagate the failure to callers.
    LargeBinaryManager.deleteByExecution(7L, (_, _) => throw new RuntimeException("boom"))
    succeed
  }

  test("create returns a URI under the current thread's base URI") {
    // create() reads a thread-local; run on a dedicated thread so the base URI is
    // isolated and does not leak into other tests.
    @volatile var uri: String = ""
    val thread = new Thread(() => {
      LargeBinaryManager.setCurrentBaseUri(LargeBinaryManager.baseUriForExecution(555L))
      uri = LargeBinaryManager.create()
    })
    thread.start()
    thread.join()
    val prefix = s"s3://${LargeBinaryManager.DEFAULT_BUCKET}/objects/555/"
    assert(uri.startsWith(prefix))
    // a unique (UUID) suffix follows the execution-scoped prefix
    assert(uri.stripPrefix(prefix).nonEmpty)
  }

  test("create throws when no base URI is set on the thread") {
    // A fresh thread starts with no base URI, so create() must fail fast.
    @volatile var caught: Option[Throwable] = None
    val thread = new Thread(() => {
      try LargeBinaryManager.create()
      catch { case e: Throwable => caught = Some(e) }
    })
    thread.start()
    thread.join()
    assert(caught.exists(_.isInstanceOf[IllegalStateException]))
  }
}
