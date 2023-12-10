package edu.uci.ics.texera.workflow.common.storage

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.texera.workflow.operators.sink.storage.{
  MemoryStorage,
  MongoDBSinkStorage,
  SinkStorageReader
}

import java.util.concurrent.ConcurrentHashMap

object OpResultStorage {
  val defaultStorageMode: String = AmberConfig.sinkStorageMode.toLowerCase
  val MEMORY = "memory"
  val MONGODB = "mongodb"
}

/**
  * Public class of operator result storage.
  * One execution links one instance of OpResultStorage, both have the same lifecycle.
  */
class OpResultStorage extends Serializable with LazyLogging {

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

  def create(
      executionID: String = "",
      key: String,
      mode: String
  ): SinkStorageReader = {
    val storage: SinkStorageReader =
      if (mode == "memory") {
        new MemoryStorage
      } else {
        try {
          new MongoDBSinkStorage(executionID + key)
        } catch {
          case t: Throwable =>
            logger.warn("Failed to create mongo storage", t)
            logger.info(s"Fall back to memory storage for $key")
            // fall back to memory
            new MemoryStorage
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
    if (cache.contains(key)) {
      cache.get(key).clear()
    }
    cache.remove(key)
  }

  /**
    * Close this storage. Used for workflow cleanup.
    */
  def close(): Unit = {
    cache.forEach((_, sinkStorageReader) => sinkStorageReader.clear())
    cache.clear()
  }

}
