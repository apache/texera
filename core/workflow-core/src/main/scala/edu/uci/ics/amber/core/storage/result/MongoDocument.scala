package edu.uci.ics.amber.core.storage.result

import com.mongodb.client.MongoCursor
import com.mongodb.client.model.Sorts
import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.amber.core.storage.model.{BufferedItemWriter, VirtualDocument}
import edu.uci.ics.amber.core.storage.util.mongo.{MongoCollectionManager, MongoDatabaseManager}
import org.bson.Document

import java.net.URI
import java.util.Date

/**
  * MongoDocument provides an implementation of VirtualDocument for MongoDB.
  * It supports various operations like read, write, and remove on a MongoDB collection.
  * @param id the identifier for the MongoDB collection.
  * @param toDocument a function that converts an item of type T to a MongoDB Document.
  * @param fromDocument a function that converts a MongoDB Document to an item of type T.
  * @tparam T the type of data items stored in the document.
  */
class MongoDocument[T >: Null <: AnyRef](
    id: String,
    var toDocument: T => Document,
    var fromDocument: Document => T
) extends VirtualDocument[T] {

  /**
    * The batch size for committing items to the MongoDB collection.
    */
  val commitBatchSize: Int = StorageConfig.mongodbBatchSize

  /**
    * Drops the existing MongoDB collection with the given identifier.
    */
  MongoDatabaseManager.dropCollection(id)

  /**
    * Lazy initialization of the MongoDB collection manager.
    */
  @transient lazy val collectionMgr: MongoCollectionManager = MongoDatabaseManager.getCollection(id)

  /**
    * This method is not supported for MongoDocument.
    * @throws UnsupportedOperationException always thrown when this method is called.
    * @return nothing, this method always throws an exception.
    */
  override def getURI: URI =
    throw new UnsupportedOperationException("getURI is not supported for MongoDocument")

  /**
    * Remove the MongoDB collection.
    */
  override def clear(): Unit = MongoDatabaseManager.dropCollection(id)

  /**
    * Return a buffered item writer for the MongoDB collection.
    * @return a new instance of MongoDBBufferedItemWriter.
    */
  override def writer(): BufferedItemWriter[T] = {
    new MongoDBBufferedItemWriter[T](
      commitBatchSize,
      id,
      toDocument
    )
  }

  /**
    * Create an iterator that wraps a MongoCursor and converts each Document to an item of type T.
    * @param cursor the MongoCursor to wrap.
    * @return an iterator of items of type T.
    */
  private[this] def mkTIterator(cursor: MongoCursor[Document]): Iterator[T] = {
    new Iterator[T] {
      override def hasNext: Boolean = cursor.hasNext

      override def next(): T = {
        fromDocument(cursor.next())
      }
    }.iterator
  }

  /**
    * Get an iterator that iterates over all items in the MongoDB collection.
    * @return an iterator of items.
    */
  override def get(): Iterator[T] = {
    val cursor = collectionMgr.accessDocuments.sort(Sorts.ascending("_id")).cursor()
    mkTIterator(cursor)
  }

  /**
    * Get an iterator of items in the specified range.
    * @param from the starting index (inclusive).
    * @param until the ending index (exclusive).
    * @return an iterator of items in the specified range.
    */
  override def getRange(from: Int, until: Int): Iterator[T] = {
    val cursor =
      collectionMgr.accessDocuments
        .sort(Sorts.ascending("_id"))
        .limit(until - from)
        .skip(from)
        .cursor()
    mkTIterator(cursor)
  }

  /**
    * Get an iterator of items starting after the specified offset.
    * @param offset the starting index (exclusive).
    * @return an iterator of items starting after the specified offset.
    */
  override def getAfter(offset: Int): Iterator[T] = {
    val cursor = collectionMgr.accessDocuments.sort(Sorts.ascending("_id")).skip(offset).cursor()
    mkTIterator(cursor)
  }

  /**
    * Get the item at the specified index.
    * @param i the index of the item.
    * @return the item at the specified index.
    * @throws RuntimeException if the index is out of bounds.
    */
  override def getItem(i: Int): T = {
    val cursor =
      collectionMgr.accessDocuments
        .sort(Sorts.ascending("_id"))
        .limit(1)
        .skip(i)
        .cursor()

    if (!cursor.hasNext) {
      throw new RuntimeException(f"Index $i out of bounds")
    }
    fromDocument(cursor.next())
  }

  /**
    * Get the count of items in the MongoDB collection.
    * @return the number of items.
    */
  override def getCount: Long = {
    collectionMgr.getCount
  }

  def getNumericColStats: Map[String, Map[String, Double]] =
    collectionMgr.calculateNumericStats()

  def getDateColStats: Map[String, Map[String, Date]] = collectionMgr.calculateDateStats()

  def getCategoricalStats: Map[String, Map[String, Map[String, Integer]]] =
    collectionMgr.calculateCategoricalStats()
}
