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
    try {
      lock.lock()
      logger.debug("put {} of length {} start", key, records.length)
      cache.put(key, records)
      logger.debug("put {} of length {} end", key, records.length)
      lock.unlock()
    } catch {
      case e: Exception => logger.error(e.getMessage)
    } finally {
      if (lock.isLocked) {
        lock.unlock()
      }
    }
  }

  override def get(key: String): List[Tuple] = {
    try {
      lock.lock()
      logger.debug("get {} start", key)
      var res = cache.get(key)
      if (res == null) {
        res = List[Tuple]()
      }
      logger.debug("get {} of length {} end", key, res.length)
      lock.unlock()
      res
    } catch {
      case e: Exception =>
        logger.error(e.getMessage)
        List[Tuple]()
    } finally {
      if (lock.isLocked) {
        lock.unlock()
      }
    }
  }

  override def remove(key: String): Unit = {
    try {
      lock.lock()
      logger.debug("remove {} start", key)
      cache.remove(key)
      logger.debug("remove {} end", key)
      lock.unlock()
    } catch {
      case e: Exception => logger.error(e.getMessage)
    } finally {
      if (lock.isLocked) {
        lock.unlock()
      }
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
