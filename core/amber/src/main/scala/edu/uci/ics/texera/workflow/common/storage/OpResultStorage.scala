package edu.uci.ics.texera.workflow.common.storage

import java.util.concurrent.ConcurrentHashMap
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.operators.sink.storage.{
  MemoryStorage,
  MongoDBStorage,
  SinkStorageReader
}

/**
  * Public class of operator result storage.
  */
class OpResultStorage(mode: String = "memory") extends Serializable with LazyLogging {

  val cache: ConcurrentHashMap[String, SinkStorageReader] =
    new ConcurrentHashMap[String, SinkStorageReader]()

  /**
    * Retrieve the result of an operator from OpResultStorage
    * @param key The key used for storage and retrieval.
    *            Currently it is the uuid inside the cache source or cache sink operator.
    * @return The storage of this operator.
    */
  def get(key: String): SinkStorageReader = {
    cache.get(key)
  }

  def create(key: String, schema: Schema): SinkStorageReader = {
    val storage: SinkStorageReader =
      if (mode == "memory") {
        new MemoryStorage(schema)
      } else {
        try {
          new MongoDBStorage(key, schema)
        } catch {
          case t: Throwable =>
            t.printStackTrace()
            logger.info(s"Fall back to memory storage for $key")
            // fall back to memory
            new MemoryStorage(schema)
        }
      }
    cache.put(key, storage)
    storage
  }

  def contains(key: String): Boolean = {
    cache.containsKey(key)
  }

  /**
    * Manually remove an entry from the cache.
    * @param key The key used for storage and retrieval.
    *            Currently it is the uuid inside the cache source or cache sink operator.
    */
  def remove(key: String): Unit = {
    logger.debug(s"remove $key start")
    if (cache.contains(key)) {
      cache.get(key).clear()
    }
    cache.remove(key)
    logger.debug(s"remove $key end")
  }

  /**
    * Close this storage. Used for workflow cleanup.
    */
  def close(): Unit = {
    cache.forEach((_, sinkStorageReader) => sinkStorageReader.clear())
    cache.clear()
  }

}
