package edu.uci.ics.texera.workflow.common.storage.memory

import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import java.util.concurrent.ConcurrentHashMap

class MemoryOpResultStorage extends OpResultStorage {

  val cache: ConcurrentHashMap[String, List[Tuple]] = new ConcurrentHashMap[String, List[Tuple]]()

  override def put(key: String, records: List[Tuple]): Unit = {
    logger.debug(s"put $key of length ${records.length} start")
    // This is an atomic operation.
    cache.put(key, records)
    logger.debug(s"put $key of length ${records.length} end")
  }

  override def get(key: String): List[Tuple] = {
    logger.debug(s"get $key start")
    val res = cache.getOrDefault(key, List[Tuple]())
    logger.debug(s"get $key of length ${res.length} end")
    res
  }

  override def remove(key: String): Unit = {
    logger.debug(s"remove $key start")
    cache.remove(key)
    logger.debug(s"remove $key end")
  }

  override def dump(): Unit = {
    throw new NotImplementedError()
  }

  override def load(): Unit = {
    throw new NotImplementedError()
  }

  override def close(): Unit = {
    cache.clear()
  }
}
