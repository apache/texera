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

  /**
    * Put the result of an operator to OpResultStorage.
    *
    * @param opID    The operator ID.
    * @param records The results.
    */
  override def put(opID: String, records: List[Tuple]): Unit = {
    lock.lock()
    logger.debug("put {} start", opID)
    cache.put(opID, records)
    logger.debug("put {} end", opID)
    lock.unlock()
  }

  /**
    * Retrieve the result of an operator from OpResultStorage
    *
    * @param opID The operator ID.
    * @return The result of this operator.
    */
  override def get(opID: String): List[Tuple] = {
    lock.lock()
    logger.debug("get {} start", opID)
    val res = cache.get(opID)
    logger.debug("get {} end", opID)
    lock.unlock()
    res
  }

  /**
    * Manually remove an entry from the cache.
    *
    * @param opID The key to remove.
    */
  override def remove(opID: String): Unit = {
    lock.lock()
    logger.debug("remove {} start", opID)
    cache.remove(opID)
    logger.debug("remove {} end", opID)
    lock.unlock()
  }

  /**
    * Dump everything in result storage. Called when the system exits.
    */
  override def dump(): Unit = {
    logger.error("Method not implemented.")
    throw new NotImplementedException()
  }

  /**
    * Load and initialize result storage. Called when the system init.
    */
  override def load(): Unit = {
    logger.error("Method not implemented.")
    throw new NotImplementedException()
  }

  /**
    * Close this storage. Used for system termination.
    */
  override def close(): Unit = {
    logger.error("Method not implemented.")
    throw new NotImplementedException()
  }
}
