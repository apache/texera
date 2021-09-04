package edu.uci.ics.texera.workflow.common.storage

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.texera.workflow.common.tuple.Tuple

/**
  * Public interface of operator result storage.
  */
trait OpResultStorage extends Serializable with LazyLogging {

  /**
    * Put the result of an operator to OpResultStorage.
    * @param key The key used for storage and retrieval.
    *            Currently it is the uuid inside the cache source or cache sink operator.
    * @param records The results.
    */
  def put(key: String, records: List[Tuple]): Unit

  /**
    * Retrieve the result of an operator from OpResultStorage
    * @param key The key used for storage and retrieval.
    *            Currently it is the uuid inside the cache source or cache sink operator.
    * @return The result of this operator.
    */
  def get(key: String): List[Tuple]

  /**
    * Manually remove an entry from the cache.
    * @param key The key used for storage and retrieval.
    *            Currently it is the uuid inside the cache source or cache sink operator.
    */
  def remove(key: String): Unit

  /**
    * Dump everything in result storage. Called when the system exits.
    */
  def dump(): Unit

  /**
    * Load and initialize result storage. Called when the system init.
    */
  def load(): Unit

  /**
    * Close this storage. Used for system termination.
    */
  def close(): Unit

}
