package edu.uci.ics.texera.workflow.common.storage.memory

import com.typesafe.scalalogging.Logger
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import org.apache.commons.jcs3.JCS
import org.apache.commons.jcs3.access.CacheAccess
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import java.util.concurrent.locks.ReentrantLock

class JCSOpResultStorage extends OpResultStorage {

  private val logger = Logger(this.getClass.getName)

  private val lock = new ReentrantLock()

  private val cache: CacheAccess[String, List[Tuple]] = JCS.getInstance("texera")

  override def put(key: String, records: List[Tuple]): Unit = {
    lock.lock()
    logger.debug("put {} start", key)
    cache.put(key, records)
    logger.debug("put {} end", key)
    lock.unlock()
  }

  override def get(key: String): List[Tuple] = {
    lock.lock()
    logger.debug("get {} start", key)
    var res = cache.get(key)
    if (res == null) {
      res = List[Tuple]()
    }
    logger.debug("get {} end", key)
    lock.unlock()
    res
  }

  override def remove(key: String): Unit = {
    lock.lock()
    logger.debug("remove {} start", key)
    cache.remove(key)
    logger.debug("remove {} end", key)
    lock.unlock()
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
