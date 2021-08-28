package edu.uci.ics.amber.engine.architecture.storage

import edu.uci.ics.texera.workflow.common.tuple.Tuple

/**
  * Public interface of operator result storage.
  */
trait OpResultStorage extends Serializable {

  /**
    * Put the result of an operator to OpResultStorage.
    * @param opID The operator ID.
    * @param records The results.
    */
  def put(opID: String, records: List[Tuple]): Unit

  /**
    * Retrieve the result of an operator from OpResultStorage
    * @param opID The operator ID.
    * @return The result of this operator.
    */
  def get(opID: String): List[Tuple]

  /**
    * Manually remove an entry from the cache.
    * @param opID The key to remove.
    */
  def remove(opID: String): Unit

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
