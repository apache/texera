package edu.uci.ics.amber.engine.common.storage.mongodb

import com.mongodb.client.MongoCursor
import com.mongodb.client.model.Sorts
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.storage.mongodb.MongoDBStorable.ToDocument
import edu.uci.ics.amber.engine.common.storage.{BufferedItemWriter, VirtualDocument}
import edu.uci.ics.texera.web.storage.{MongoCollectionManager, MongoDatabaseManager}
import org.bson.Document

import java.net.URI

class MongoDocument[T >: Null <: AnyRef](
    id: String,
    toDocument: ToDocument,
    fromDocument: Document => T
) extends VirtualDocument[T] {

  val commitBatchSize: Int = AmberConfig.sinkStorageMongoDBConfig.getInt("commit-batch-size")
  MongoDatabaseManager.dropCollection(id)
  @transient lazy val collectionMgr: MongoCollectionManager = MongoDatabaseManager.getCollection(id)

  override def getURI: URI =
    throw new UnsupportedOperationException("getURI is not supported for MongoDocument")

  override def remove(): Unit = MongoDatabaseManager.dropCollection(id)

  override def write(): BufferedItemWriter[T] = {
    new MongoDBBufferedItemWriter[T](commitBatchSize, id, toDocument)
  }

  private[this] def mkTIterable(cursor: MongoCursor[Document]): Iterable[T] = {
    new Iterator[T] {
      override def hasNext: Boolean = cursor.hasNext

      override def next(): T = fromDocument(cursor.next())
    }.iterator.to(Iterable)
  }

  override def get(): Iterator[T] = {
    val cursor = collectionMgr.accessDocuments.sort(Sorts.ascending("_id")).cursor()
    mkTIterable(cursor).iterator
  }

  override def getAll(): Iterable[T] = {
    val cursor = collectionMgr.accessDocuments.sort(Sorts.ascending("_id")).cursor()
    mkTIterable(cursor)
  }

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
}
