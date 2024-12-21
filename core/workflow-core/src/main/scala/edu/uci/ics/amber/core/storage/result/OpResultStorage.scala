package edu.uci.ics.amber.core.storage.result

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.amber.core.storage.model.VirtualDocument
import edu.uci.ics.amber.core.storage.result.iceberg.IcebergDocument
import edu.uci.ics.amber.core.tuple.{Schema, Tuple}
import edu.uci.ics.amber.util.IcebergUtil
import edu.uci.ics.amber.virtualidentity.OperatorIdentity
import org.apache.iceberg.data.Record
import org.apache.iceberg.{Schema => IcebergSchema}

import java.util.concurrent.ConcurrentHashMap
import scala.collection.convert.ImplicitConversions.`iterator asScala`

object OpResultStorage {
  val defaultStorageMode: String = StorageConfig.resultStorageMode.toLowerCase
  val MEMORY = "memory"
  val MONGODB = "mongodb"
  val ICEBERG = "iceberg"
}

/**
  * Public class of operator result storage.
  * One execution links one instance of OpResultStorage, both have the same lifecycle.
  */
class OpResultStorage extends Serializable with LazyLogging {

  // since some op need to get the schema from the OpResultStorage, the schema is stored as part of the OpResultStorage.cache
  // TODO: once we make the storage self-contained, i.e. storing Schema in the storage as metadata, we can remove it
  val cache: ConcurrentHashMap[OperatorIdentity, (VirtualDocument[Tuple], Option[Schema])] =
    new ConcurrentHashMap[OperatorIdentity, (VirtualDocument[Tuple], Option[Schema])]()

  /**
    * Retrieve the result of an operator from OpResultStorage
    * @param key The key used for storage and retrieval.
    *            Currently it is the uuid inside the cache source or cache sink operator.
    * @return The storage object of this operator.
    */
  def get(key: OperatorIdentity): VirtualDocument[Tuple] = {
    cache.get(key)._1
  }

  /**
    * Retrieve the schema of the result associate with target operator
    * @param key the uuid inside the cache source or cache sink operator.
    * @return The result schema of this operator.
    */
  def getSchema(key: OperatorIdentity): Option[Schema] = {
    cache.get(key)._2
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
      } else if (mode == OpResultStorage.MONGODB) {
        try {
          val fromDocument = schema.map(Tuple.fromDocument)
          new MongoDocument[Tuple](executionId + key, Tuple.toDocument, fromDocument)
        } catch {
          case t: Throwable =>
            logger.warn("Failed to create mongo storage", t)
            logger.info(s"Fall back to memory storage for $key")
            // fall back to memory
            new MemoryDocument[Tuple](key.id)
        }
      } else {
        val icebergCatalog = IcebergUtil.createJdbcCatalog(
          "operator-result",
          StorageConfig.fileStorageDirectoryUri,
          StorageConfig.icebergCatalogUrl,
          StorageConfig.icebergCatalogUsername,
          StorageConfig.icebergCatalogPassword
        )
        val icebergSchema = IcebergUtil.toIcebergSchema(schema.get)
        val serde: Tuple => Record = tuple => IcebergUtil.toGenericRecord(tuple)
        val deserde: (IcebergSchema, Record) => Tuple = (_, record) =>
          IcebergUtil.fromRecord(record, schema.get)

        new IcebergDocument[Tuple](
          icebergCatalog,
          StorageConfig.icebergTableNamespace,
          executionId + key,
          icebergSchema,
          serde,
          deserde
        )
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
      cache.get(key)._1.clear()
    }
    cache.remove(key)
  }

  /**
    * Close this storage. Used for workflow cleanup.
    */
  def clear(): Unit = {
    cache.forEach((_, document) => document._1.clear())
    cache.clear()
  }

  def getAllKeys: Set[OperatorIdentity] = {
    cache.keySet().iterator().toSet
  }

}
