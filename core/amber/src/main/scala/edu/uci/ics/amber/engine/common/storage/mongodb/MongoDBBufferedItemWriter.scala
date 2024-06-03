package edu.uci.ics.amber.engine.common.storage.mongodb

import edu.uci.ics.amber.engine.common.storage.BufferedItemWriter
import edu.uci.ics.texera.web.storage.{MongoCollectionManager, MongoDatabaseManager}
import org.bson.Document

import scala.collection.mutable

class MongoDBBufferedItemWriter[T >: Null <: AnyRef](
    _bufferSize: Int,
    id: String,
    toDocument: T => Document
) extends BufferedItemWriter[T] {
  override val bufferSize: Int = _bufferSize

  var uncommittedInsertions: mutable.ArrayBuffer[T] = _
  @transient lazy val collection: MongoCollectionManager = MongoDatabaseManager.getCollection(id)
  override def open(): Unit = {
    uncommittedInsertions = new mutable.ArrayBuffer[T]()
  }

  override def close(): Unit = {
    if (uncommittedInsertions.nonEmpty) {
      collection.insertMany(uncommittedInsertions.map(toDocument))
      uncommittedInsertions.clear()
    }
  }

  override def putOne(item: T): Unit = {
    uncommittedInsertions.append(item)
    if (uncommittedInsertions.size == bufferSize) {
      collection.insertMany(uncommittedInsertions.map(toDocument))
      uncommittedInsertions.clear()
    }
  }

  override def removeOne(item: T): Unit = {
    val index = uncommittedInsertions.indexOf(item)
    if (index != -1) {
      uncommittedInsertions.remove(index)
    } else {
      collection.deleteMany(toDocument(item))
    }
  }
}
