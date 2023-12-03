package edu.uci.ics.texera.workflow.operators.sink.storage

import com.mongodb.client.model.Sorts
import com.mongodb.client.MongoCursor
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.texera.web.storage.{MongoCollectionManager, MongoDatabaseManager}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.TupleUtils.document2Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import org.bson.Document

import scala.collection.mutable

class MongoDBSinkStorage(id: String, schema: Schema) extends SinkStorageReader {

  // For backward compatibility of old mongoDB(version < 5)
  schema.getAttributeNames.stream.forEach(name =>
    assert(!name.matches(".*[\\$\\.].*"), s"illegal attribute name '$name' for mongo DB")
  )

  val commitBatchSize: Int = AmberConfig.sinkStorageMongoDBConfig.getInt("commit-batch-size")
  MongoDatabaseManager.dropCollection(id)
  val collectionMgr: MongoCollectionManager = MongoDatabaseManager.getCollection(id)

  class MongoDBSinkStorageWriter(bufferSize: Int) extends SinkStorageWriter {
    var uncommittedInsertions: mutable.ArrayBuffer[Tuple] = _
    var collection: MongoCollectionManager = _

    override def open(): Unit = {
      uncommittedInsertions = new mutable.ArrayBuffer[Tuple]()
      collection = MongoDatabaseManager.getCollection(id)
    }

    override def close(): Unit = {
      if (uncommittedInsertions.nonEmpty) {
        collection.insertMany(uncommittedInsertions.map(_.asDocument()))
        uncommittedInsertions.clear()
      }
    }

    override def putOne(tuple: Tuple): Unit = {
      uncommittedInsertions.append(tuple)
      if (uncommittedInsertions.size == bufferSize) {
        collection.insertMany(uncommittedInsertions.map(_.asDocument()))
        uncommittedInsertions.clear()
      }
    }

    override def removeOne(tuple: Tuple): Unit = {
      val index = uncommittedInsertions.indexOf(tuple)
      if (index != -1) {
        uncommittedInsertions.remove(index)
      } else {
        collection.deleteMany(tuple.asDocument())
      }
    }
  }

  private[this] def mkTupleIterable(cursor: MongoCursor[Document]): Iterable[Tuple] = {
    new Iterator[Tuple] {
      override def hasNext: Boolean = cursor.hasNext
      override def next(): Tuple = document2Tuple(cursor.next(), schema)
    }.toIterable
  }

  override def getAll: Iterable[Tuple] = {
    val cursor = collectionMgr.accessDocuments.sort(Sorts.ascending("_id")).cursor()
    mkTupleIterable(cursor)
  }

  override def getStorageWriter(): SinkStorageWriter =
    new MongoDBSinkStorageWriter(commitBatchSize)

  override def clear(): Unit = {
    MongoDatabaseManager.dropCollection(id)
  }

  override def getRange(from: Int, to: Int): Iterable[Tuple] = {
    val cursor =
      collectionMgr.accessDocuments
        .sort(Sorts.ascending("_id"))
        .limit(to - from)
        .skip(from)
        .cursor()
    mkTupleIterable(cursor)
  }

  override def getCount: Long = {
    collectionMgr.getCount
  }

  override def getAllAfter(offset: Int): Iterable[Tuple] = {
    val cursor = collectionMgr.accessDocuments.sort(Sorts.ascending("_id")).skip(offset).cursor()
    mkTupleIterable(cursor)
  }

  override def getSchema: Schema = schema
}
