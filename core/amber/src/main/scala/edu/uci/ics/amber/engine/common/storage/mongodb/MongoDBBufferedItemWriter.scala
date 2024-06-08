package edu.uci.ics.amber.engine.common.storage.mongodb

import edu.uci.ics.amber.engine.common.storage.BufferedItemWriter
import edu.uci.ics.texera.web.storage.{MongoCollectionManager, MongoDatabaseManager}
import org.bson.Document

import scala.collection.mutable

/**
  * MongoDBBufferedItemWriter provides a buffered writer implementation for MongoDB.
  * It buffers items in memory and writes them to the MongoDB collection in batches.
  * @param _bufferSize the size of the buffer.
  * @param id the identifier for the MongoDB collection.
  * @param toDocument a function that converts an item of type T to a MongoDB Document.
  * @tparam T the type of data items to be written.
  */
class MongoDBBufferedItemWriter[T >: Null <: AnyRef](
    _bufferSize: Int,
    id: String,
    toDocument: T => Document
) extends BufferedItemWriter[T] {

  /**
    * The size of the buffer.
    */
  override val bufferSize: Int = _bufferSize

  /**
    * A buffer for storing items before they are written to the MongoDB collection.
    */
  var uncommittedInsertions: mutable.ArrayBuffer[T] = _

  /**
    * Lazy initialization of the MongoDB collection manager.
    */
  @transient lazy val collection: MongoCollectionManager = MongoDatabaseManager.getCollection(id)

  /**
    * Open the writer, initializing the buffer.
    * This method should be called before any write operations.
    */
  override def open(): Unit = {
    uncommittedInsertions = new mutable.ArrayBuffer[T]()
  }

  /**
    * Close the writer, flushing any remaining items in the buffer to the MongoDB collection.
    * This method should be called after all write operations are completed.
    */
  override def close(): Unit = {
    if (uncommittedInsertions.nonEmpty) {
      collection.insertMany(uncommittedInsertions.map(toDocument))
      uncommittedInsertions.clear()
    }
  }

  /**
    * Put one item into the buffer. If the buffer is full, it is flushed to the MongoDB collection.
    * @param item the data item to be written.
    */
  override def putOne(item: T): Unit = {
    uncommittedInsertions.append(item)
    if (uncommittedInsertions.size == bufferSize) {
      collection.insertMany(uncommittedInsertions.map(toDocument))
      uncommittedInsertions.clear()
    }
  }

  /**
    * Remove one item from the buffer. If the item is not found in the buffer, it is deleted from the MongoDB collection.
    * @param item the data item to be removed.
    */
  override def removeOne(item: T): Unit = {
    val index = uncommittedInsertions.indexOf(item)
    if (index != -1) {
      uncommittedInsertions.remove(index)
    } else {
      collection.deleteMany(toDocument(item))
    }
  }
}
