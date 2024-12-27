package edu.uci.ics.amber.core.storage.result

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.amber.core.storage.model.VirtualDocument
import edu.uci.ics.amber.core.tuple.{Schema, Tuple}
import edu.uci.ics.amber.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.workflow.PortIdentity

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.IteratorHasAsScala

object OpResultStorage {
  val defaultStorageMode: String = StorageConfig.resultStorageMode.toLowerCase
  val MEMORY = "memory"
  val MONGODB = "mongodb"

  def storageKey(
      operatorId: OperatorIdentity,
      portIdentity: PortIdentity,
      isMaterialized: Boolean = false
  ): String = {
    s"${if (isMaterialized) "materialized_" else ""}${operatorId.id}_${portIdentity.id}_${portIdentity.internal}"
  }

  def decodeStorageKey(key: String): (OperatorIdentity, PortIdentity) = {
    var res = key
    if (key.startsWith("materialized_")) {
      res = key.substring(13)
    }

    res.split("_", 3) match {
      case Array(opId, portId, internal) => (OperatorIdentity(opId), PortIdentity(portId.toInt, internal.toBoolean))
      case _                   => throw new IllegalArgumentException(s"Invalid storage key: $key")
    }
  }
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
    if (!cache.containsKey(key)) {
      throw new NoSuchElementException(s"Storage with key $key not found")
    }
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
