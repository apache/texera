package edu.uci.ics.amber.core.storage.result

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.amber.core.storage.model.VirtualDocument
import edu.uci.ics.amber.core.tuple.{Schema, Tuple}

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.IteratorHasAsScala


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

  // since some op need to get the schema from the OpResultStorage, the schema is stored as part of the OpResultStorage.cache
  // TODO: once we make the storage self-contained, i.e. storing Schema in the storage as metadata, we can remove it
  private val cache: ConcurrentHashMap[String, (VirtualDocument[Tuple], Schema)] =
    new ConcurrentHashMap[String, (VirtualDocument[Tuple], Schema)]()

  /**
    * Retrieve the result of an operator from OpResultStorage
    * @param key The key used for storage and retrieval.
    *            Currently it is the uuid inside the cache source or cache sink operator.
    * @return The storage object of this operator.
    */
  def get(key: String): VirtualDocument[Tuple] = {
    cache.get(key)._1
  }

  /**
    * Retrieve the schema of the result associate with target operator
    * @param key the uuid inside the cache source or cache sink operator.
    * @return The result schema of this operator.
    */
  def getSchema(key: String): Schema = {
    cache.get(key)._2
  }


  def create(
      executionId: String = "",
      key: String,
      mode: String,
      schema: Schema
  ): VirtualDocument[Tuple] = {

    val storage: VirtualDocument[Tuple] =
      if (mode == "memory") {
        new MemoryDocument[Tuple](key)
      } else {
        try {

          new MongoDocument[Tuple](executionId + key, Tuple.toDocument, Tuple.fromDocument(schema))
        } catch {
          case t: Throwable =>
            logger.warn("Failed to create mongo storage", t)
            logger.info(s"Fall back to memory storage for $key")
            // fall back to memory
            new MemoryDocument[Tuple](key)
        }
      }
    cache.put(key, (storage, schema))
    storage
  }

  def contains(key: String): Boolean = {
    cache.containsKey(key)
  }


  /**
    * Close this storage. Used for workflow cleanup.
    */
  def clear(): Unit = {
    cache.forEach((_, document) => document._1.clear())
    cache.clear()
  }

  def getAllKeys: Set[String] = {
    cache.keySet().iterator().asScala.toSet
  }

}
