package edu.uci.ics.texera.workflow.common.storage.memory

import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import org.apache.commons.jcs3.JCS
import org.apache.commons.jcs3.access.CacheAccess
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import java.util.concurrent.locks.ReentrantLock

class JCSOpResultStorage extends OpResultStorage {

  private val lock = new ReentrantLock()

  private val cache: CacheAccess[String, List[Tuple]] = JCS.getInstance("texera")

  override def put(key: String, records: List[Tuple]): Unit = {
    lock.lock()
    try {
      logger.debug("put {} of length {} start", key, records.length)
      cache.put(key, records)
      logger.debug("put {} of length {} end", key, records.length)
    } finally {
      lock.unlock()
    }
  }

  override def get(key: String): List[Tuple] = {
    lock.lock()
    try {
      logger.debug("get {} start", key)
      var res = cache.get(key)
      if (res == null) {
        res = List[Tuple]()
      }
      logger.debug("get {} of length {} end", key, res.length)
      res
    } finally {
      lock.unlock()
    }
  }

  override def remove(key: String): Unit = {
    lock.lock()
    try {
      logger.debug("remove {} start", key)
      cache.remove(key)
      logger.debug("remove {} end", key)
    } finally {
      lock.unlock()
    }
  }

  override def dump(): Unit = {
    logger.error("Method not implemented.")
    throw new NotImplementedException()
  }

  override def load(): Unit = {
    logger.error("Method not implemented.")
    throw new NotImplementedException()
  }

  override def close(): Unit = {
    logger.error("Method not implemented.")
    throw new NotImplementedException()
  }
}
