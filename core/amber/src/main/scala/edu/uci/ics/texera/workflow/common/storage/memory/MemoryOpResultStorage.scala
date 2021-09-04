package edu.uci.ics.texera.workflow.common.storage.memory

import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import java.util.concurrent.ConcurrentHashMap

class MemoryOpResultStorage extends OpResultStorage {

  val cache: ConcurrentHashMap[String, List[Tuple]] = new ConcurrentHashMap[String, List[Tuple]]()

  override def put(key: String, records: List[Tuple]): Unit = {
    logger.debug("put {} of length {} start", key, records.length)
    // This is an atomic operation.
    cache.put(key, records)
    logger.debug("put {} of length {} end", key, records.length)
  }

  override def get(key: String): List[Tuple] = {
    logger.debug("get {} start", key)
    var res = cache.get(key)
    if (res == null) {
      res = List[Tuple]()
    }
    logger.debug("get {} of length {} end", key, res.length)
    res
  }

  override def remove(key: String): Unit = {
    logger.debug("remove {} start", key)
    cache.remove(key)
    logger.debug("remove {} end", key)
  }

  override def dump(): Unit = {
    throw new Exception("not implemented")
  }

  override def load(): Unit = {
    throw new Exception("not implemented")
  }

  override def close(): Unit = {
    cache.clear()
  }
}
