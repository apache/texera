package edu.uci.ics.amber.core.storage.result

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.amber.virtualidentity.OperatorIdentity

import java.util.concurrent.ConcurrentHashMap

object OpResultStorage {
  val defaultStorageMode: String = StorageConfig.resultStorageMode.toLowerCase
  val MEMORY = "memory"
  val MONGODB = "mongodb"
}

/**
  * Public class of operator result storage.
  * One execution links one instance of OpResultStorage, both have the same lifecycle.
  */
class OpResultStorage extends Serializable with LazyLogging {

  val cache: ConcurrentHashMap[OperatorIdentity, (VirtualDocument[Tuple], Option[Schema])] =
    new ConcurrentHashMap[OperatorIdentity, (VirtualDocument[Tuple], Option[Schema])]()

  /**
    * Retrieve the result of an operator from OpResultStorage
    * @param key The key used for storage and retrieval.
    *            Currently it is the uuid inside the cache source or cache sink operator.
    * @return The storage of this operator.
    */
  def get(key: OperatorIdentity): VirtualDocument[Tuple] = {
    cache.get(key)._1
  }

  def getSchema(key: OperatorIdentity): Schema = {
    cache.get(key)._2.get
  }

  def setSchema(key: OperatorIdentity, schema: Schema): Unit = {
    val storage = get(key)
    cache.put(key, (storage, Some(schema)))
  }

  def create(
      executionId: String = "",
      key: OperatorIdentity,
      mode: String,
      schema: Option[Schema] = None
  ): VirtualDocument[Tuple] = {
    val storage: VirtualDocument[Tuple] =
      if (mode == "memory") {
        new MemoryDocument[Tuple](key.id)
      } else {
        try {
          val fromDocument: Option[Document => Tuple] = {
            if (schema.isDefined)
              Some(Tuple.fromDocument(schema.get))
            else
              None
          }
          new MongoDocument[Tuple](executionId + key, Some(Tuple.toDocument), fromDocument)
        } catch {
          case t: Throwable =>
            logger.warn("Failed to create mongo storage", t)
            logger.info(s"Fall back to memory storage for $key")
            // fall back to memory
            new MemoryDocument[Tuple](key.id)
        }
      }
    cache.put(key, (storage, schema))
    storage
  }

  def contains(key: OperatorIdentity): Boolean = {
    cache.containsKey(key)
  }

  /**
    * Manually remove an entry from the cache.
    * @param key The key used for storage and retrieval.
    *            Currently it is the uuid inside the cache source or cache sink operator.
    */
  def remove(key: OperatorIdentity): Unit = {
    if (cache.contains(key)) {
      cache.get(key)._1.remove()
    }
    cache.remove(key)
  }

  /**
    * Close this storage. Used for workflow cleanup.
    */
  def clear(): Unit = {
    cache.forEach((_, document) => document._1.remove())
    cache.clear()
  }

}
