package edu.uci.ics.amber.core.storage.util

import java.util.concurrent.locks.{Lock, ReadWriteLock}

object StorageUtil {
  def withWriteLock[M](rwLock: ReadWriteLock)(block: => M): M = {
    rwLock.writeLock().lock()
    try block
    finally rwLock.writeLock().unlock()
  }

  def withReadLock[M](rwLock: ReadWriteLock)(block: => M): M = {
    rwLock.readLock().lock()
    try block
    finally rwLock.readLock().unlock()
  }

  def withLock[M](lock: Lock)(block: => M): M = {
    lock.lock()
    try block
    finally lock.unlock()
  }
}
