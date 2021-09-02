package edu.uci.ics.amber.engine.architecture.storage.memory

import com.typesafe.scalalogging.Logger
import edu.uci.ics.amber.engine.architecture.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable

class MemoryOpResultStorage extends OpResultStorage {

  private val logger = Logger(this.getClass.getName)

  private val lock = new ReentrantLock()

  val cache: mutable.Map[String, List[Tuple]] = mutable.HashMap[String, List[Tuple]]()

  override def put(opID: String, records: List[Tuple]): Unit = {
    lock.lock()
    logger.debug("put {} start", opID)
    cache(opID) = records
    logger.debug("put {} end", opID)
    lock.unlock()
  }

  override def get(opID: String): List[Tuple] = {
    lock.lock()
    logger.debug("get {} start", opID)
    var res: List[Tuple] = List[Tuple]()
    if (cache.contains(opID)) {
      res = cache(opID)
    }
    logger.debug("get {} end", opID)
    lock.unlock()
    res
  }

  override def remove(opID: String): Unit = {
    lock.lock()
    logger.debug("remove {} start", opID)
    if (cache.contains(opID)) {
      cache.remove(opID)
    }
    logger.debug("remove {} end", opID)
    lock.unlock()
  }

  override def dump(): Unit = {
    throw new Exception("not implemented")
  }

  override def load(): Unit = {
    throw new Exception("not implemented")
  }

  override def close(): Unit = {
    lock.lock()
    cache.clear()
    lock.unlock()
  }
}
