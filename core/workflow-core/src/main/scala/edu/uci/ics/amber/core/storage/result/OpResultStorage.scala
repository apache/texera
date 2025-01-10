package edu.uci.ics.amber.core.storage.result

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.amber.core.storage.model.VirtualDocument
import edu.uci.ics.amber.core.storage.result.iceberg.IcebergDocument
import edu.uci.ics.amber.core.tuple.{Schema, Tuple}
import edu.uci.ics.amber.core.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.core.workflow.PortIdentity
import edu.uci.ics.amber.util.IcebergUtil
import org.apache.iceberg.data.Record
import org.apache.iceberg.{Schema => IcebergSchema}

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.IteratorHasAsScala

/**
  * Companion object for `OpResultStorage`, providing utility functions
  * for key generation, decoding, and storage modes.
  */
object OpResultStorage {
  val defaultStorageMode: String = StorageConfig.resultStorageMode.toLowerCase
  val MONGODB: String = "mongodb"
  val ICEBERG = "iceberg"

  /**
    * Creates a unique storage key by combining operator and port identities.
    *
    * @param operatorId     The unique identifier of the operator.
    * @param portIdentity   The unique identifier of the port.
    * @param isMaterialized Indicates whether the storage is materialized (e.g., persisted).
    * @return A string representing the generated storage key, formatted as:
    *         "materialized_<operatorId>_<portId>_<isInternal>" if materialized,
    *         otherwise "<operatorId>_<portId>_<isInternal>".
    */
  def createStorageKey(
      operatorId: OperatorIdentity,
      portIdentity: PortIdentity,
      isMaterialized: Boolean = false
  ): String = {
    val prefix = if (isMaterialized) "materialized_" else ""
    s"$prefix${operatorId.id}_${portIdentity.id}_${portIdentity.internal}"
  }

  /**
    * Decodes a storage key back into its original components.
    *
    * @param key The storage key to decode.
    * @return A tuple containing the operator identity and port identity.
    * @throws IllegalArgumentException If the key format is invalid.
    */
  def decodeStorageKey(key: String): (OperatorIdentity, PortIdentity) = {
    val processedKey = if (key.startsWith("materialized_")) key.substring(13) else key
    processedKey.split("_", 3) match {
      case Array(opId, portId, internal) =>
        (OperatorIdentity(opId), PortIdentity(portId.toInt, internal.toBoolean))
      case _ =>
        throw new IllegalArgumentException(s"Invalid storage key: $key")
    }
  }
}

/**
  * Handles the storage of operator results during workflow execution.
  * Each `OpResultStorage` instance is tied to the lifecycle of a single execution.
  */
class OpResultStorage extends Serializable with LazyLogging {

  /**
    * In-memory cache for storing results and their associated schemas.
    * TODO: Once the storage is self-contained (i.e., stores schemas as metadata),
    *       this can be removed.
    */
  private val cache: ConcurrentHashMap[String, (VirtualDocument[Tuple], Schema)] =
    new ConcurrentHashMap()

  /**
    * Retrieves the result of an operator from the storage.
    *
    * @param key The storage key associated with the result.
    * @return The result stored as a `VirtualDocument[Tuple]`.
    * @throws NoSuchElementException If the key is not found in the cache.
    */
  def get(key: String): VirtualDocument[Tuple] = {
    Option(cache.get(key)) match {
      case Some((document, _)) => document
      case None                => throw new NoSuchElementException(s"Storage with key $key not found")
    }
  }

  /**
    * Retrieves the schema associated with an operator's result.
    *
    * @param key The storage key associated with the schema.
    * @return The schema of the result.
    */
  def getSchema(key: String): Schema = {
    cache.get(key)._2
  }

  /**
    * Creates a new storage object for an operator result.
    *
    * @param executionId An optional execution ID for unique identification.
    * @param key         The storage key for the result.
    * @param mode        The storage mode (e.g., "memory" or "mongodb").
    * @param schema      The schema of the result.
    * @return A `VirtualDocument[Tuple]` instance for storing results.
    */
  def create(
      executionId: String = "",
      key: String,
      mode: String,
      schema: Schema
  ): VirtualDocument[Tuple] = {
    val storage: VirtualDocument[Tuple] =
      if (mode == OpResultStorage.MONGODB) {
        try {
          new MongoDocument[Tuple](
            executionId + key,
            Tuple.toDocument,
            Tuple.fromDocument(schema)
          )
        } catch {
          case t: Throwable =>
            logger.warn("Failed to create MongoDB storage", t)
            logger.info(s"Falling back to Iceberg storage for $key")
            createIcebergDocument(executionId, key, schema)
        }
      } else if (mode == OpResultStorage.ICEBERG) {
        createIcebergDocument(executionId, key, schema)
      } else {
        throw new IllegalArgumentException(s"Unsupported storage mode: $mode")
      }
    cache.put(key, (storage, schema))
    storage
  }

  /**
    * Checks if a storage key exists in the cache.
    *
    * @param key The storage key to check.
    * @return True if the key exists, false otherwise.
    */
  def contains(key: String): Boolean = cache.containsKey(key)

  /**
    * Clears all stored results. Typically used during workflow cleanup.
    */
  def clear(): Unit = {
    cache.forEach((_, document) => document._1.clear())
    cache.clear()
  }

  /**
    * Retrieves all storage keys currently in the cache.
    *
    * @return A set of all keys in the cache.
    */
  def getAllKeys: Set[String] = {
    cache.keySet().iterator().asScala.toSet
  }

  private def createIcebergDocument(
      executionId: String,
      key: String,
      schema: Schema
  ): IcebergDocument[Tuple] = {
    val icebergSchema = IcebergUtil.toIcebergSchema(schema)
    val serde: (IcebergSchema, Tuple) => Record = IcebergUtil.toGenericRecord
    val deserde: (IcebergSchema, Record) => Tuple = (_, record) =>
      IcebergUtil.fromRecord(record, schema)

    new IcebergDocument[Tuple](
      StorageConfig.icebergTableNamespace,
      executionId + key,
      icebergSchema,
      serde,
      deserde
    )
  }
}
