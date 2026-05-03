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

package org.apache.texera.amber.core.storage.util

import org.scalatest.flatspec.AnyFlatSpec

import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}

class StorageUtilSpec extends AnyFlatSpec {

  "StorageUtil.withWriteLock" should "execute the block and return its value" in {
    val rwLock = new ReentrantReadWriteLock()
    val result = StorageUtil.withWriteLock(rwLock)("written")
    assert(result == "written")
    assert(!rwLock.writeLock().isHeldByCurrentThread)
  }

  it should "release the write lock even when the block throws" in {
    val rwLock = new ReentrantReadWriteLock()
    val ex = intercept[RuntimeException] {
      StorageUtil.withWriteLock(rwLock)(throw new RuntimeException("boom"))
    }
    assert(ex.getMessage == "boom")
    assert(!rwLock.isWriteLocked)
  }

  "StorageUtil.withReadLock" should "execute the block and release the read lock" in {
    val rwLock = new ReentrantReadWriteLock()
    val result = StorageUtil.withReadLock(rwLock)(42)
    assert(result == 42)
    assert(rwLock.getReadLockCount == 0)
  }

  it should "release the read lock even when the block throws" in {
    val rwLock = new ReentrantReadWriteLock()
    intercept[IllegalStateException] {
      StorageUtil.withReadLock(rwLock)(throw new IllegalStateException("boom"))
    }
    assert(rwLock.getReadLockCount == 0)
  }

  "StorageUtil.withLock" should "execute the block and release the lock" in {
    val lock = new ReentrantLock()
    val result = StorageUtil.withLock(lock)("done")
    assert(result == "done")
    assert(!lock.isHeldByCurrentThread)
  }

  it should "release the lock even when the block throws" in {
    val lock = new ReentrantLock()
    intercept[ArithmeticException] {
      StorageUtil.withLock(lock)(throw new ArithmeticException("boom"))
    }
    assert(!lock.isHeldByCurrentThread)
    assert(!lock.isLocked)
  }
}
