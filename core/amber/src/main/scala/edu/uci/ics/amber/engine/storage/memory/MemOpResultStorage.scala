package edu.uci.ics.amber.engine.storage.memory

import com.typesafe.scalalogging.Logger
import edu.uci.ics.amber.engine.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import org.apache.commons.jcs3.JCS
import org.apache.commons.jcs3.access.CacheAccess
import sun.reflect.generics.reflectiveObjects.NotImplementedException

class MemOpResultStorage extends OpResultStorage {

  private val cache: CacheAccess[String, List[Tuple]] = JCS.getInstance("texera")

  private val logger = Logger(this.getClass.getName)

  /**
    * Put the result of an operator to OpResultStorage.
    *
    * @param opID    The operator ID.
    * @param records The results.
    */
  override def put(opID: String, records: List[Tuple]): Unit = {
    cache.put(opID, records)
  }

  /**
    * Retrieve the result of an operator from OpResultStorage
    *
    * @param opID The operator ID.
    * @return The result of this operator.
    */
  override def get(opID: String): List[Tuple] = {
    cache.get(opID)
  }

  /**
    * Manually remove an entry from the cache.
    *
    * @param opID The key to remove.
    */
  override def remove(opID: String): Unit = {
    cache.remove(opID)
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

}
