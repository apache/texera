package edu.uci.ics.amber.engine.architecture.storage.memory

import com.typesafe.scalalogging.Logger
import edu.uci.ics.amber.engine.architecture.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import org.apache.commons.jcs3.JCS
import org.apache.commons.jcs3.access.CacheAccess
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import java.util.concurrent.locks.ReentrantLock

class JCSOpResultStorage extends OpResultStorage {

  private val logger = Logger(this.getClass.getName)

  private val lock = new ReentrantLock()

  private val cache: CacheAccess[String, List[Tuple]] = JCS.getInstance("texera")

  override def put(opID: String, records: List[Tuple]): Unit = {
    lock.lock()
    logger.debug("put {} start", opID)
    cache.put(opID, records)
    logger.debug("put {} end", opID)
    lock.unlock()
  }

  override def get(opID: String): List[Tuple] = {
    lock.lock()
    logger.debug("get {} start", opID)
    var res = cache.get(opID)
    if (res == null) {
      res = List[Tuple]()
    }
    logger.debug("get {} end", opID)
    lock.unlock()
    res
  }

  override def remove(opID: String): Unit = {
    lock.lock()
    logger.debug("remove {} start", opID)
    cache.remove(opID)
    logger.debug("remove {} end", opID)
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
