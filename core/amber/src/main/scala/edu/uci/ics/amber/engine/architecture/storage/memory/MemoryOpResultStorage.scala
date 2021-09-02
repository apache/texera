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

  override def put(key: String, records: List[Tuple]): Unit = {
    lock.lock()
    logger.debug("put {} start", key)
    cache(key) = records
    logger.debug("put {} end", key)
    lock.unlock()
  }

  override def get(key: String): List[Tuple] = {
    lock.lock()
    logger.debug("get {} start", key)
    var res: List[Tuple] = List[Tuple]()
    if (cache.contains(key)) {
      res = cache(key)
    }
    logger.debug("get {} end", key)
    lock.unlock()
    res
  }

  override def remove(key: String): Unit = {
    lock.lock()
    logger.debug("remove {} start", key)
    if (cache.contains(key)) {
      cache.remove(key)
    }
    logger.debug("remove {} end", key)
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
