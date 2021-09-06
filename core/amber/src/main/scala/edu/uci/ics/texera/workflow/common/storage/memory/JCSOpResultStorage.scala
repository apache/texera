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
      logger.debug(s"put $key of length ${records.length} start")
      cache.put(key, records)
      logger.debug(s"put $key of length ${records.length} end")
    } finally {
      lock.unlock()
    }
  }

  override def get(key: String): List[Tuple] = {
    lock.lock()
    try {
      logger.debug(s"get $key start")
      var res = cache.get(key)
      if (res == null) {
        res = List[Tuple]()
      }
      logger.debug(s"get $key of length ${res.length} end")
      res
    } finally {
      lock.unlock()
    }
  }

  override def remove(key: String): Unit = {
    lock.lock()
    try {
      logger.debug(s"remove $key start")
      cache.remove(key)
      logger.debug(s"remove $key end")
    } finally {
      lock.unlock()
    }
  }

  override def dump(): Unit = {
    throw new NotImplementedException()
  }

  override def load(): Unit = {
    throw new NotImplementedException()
  }

  override def close(): Unit = {
    throw new NotImplementedException()
  }
}
