package edu.uci.ics.texera.workflow.common.storage.mongo

import com.mongodb.BasicDBObject
import com.mongodb.client.model.{IndexOptions, Indexes, Sorts}
import com.mongodb.client.{MongoClient, MongoClients, MongoDatabase}
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.TupleUtils.{json2tuple, tuple2json}
import org.bson.Document

import java.util
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class MongoOpResultStorage extends OpResultStorage {

  private val lock = new ReentrantLock()

  val url: String = Constants.mongodbUrl

  val databaseName: String = Constants.mongodbDatabaseName

  val client: MongoClient = MongoClients.create(url)

  val database: MongoDatabase = client.getDatabase(databaseName)

  val collectionSet: mutable.HashSet[String] = mutable.HashSet[String]()

  override def put(key: String, records: List[Tuple]): Unit = {
    lock.lock()
    try {
      logger.debug("put {} of length {} start", key, records.stringPrefix)
      val collection = database.getCollection(key)
      if (collectionSet.contains(key)) {
        collection.deleteMany(new BasicDBObject())
      }
      var index = 0
      val documents = new util.LinkedList[Document]()
      records.foreach(record => {
        val document = new Document()
        document.put("index", index)
        document.put("record", tuple2json(record))
        documents.push(document)
        index += 1
      })
      collection.insertMany(documents)
      collection.createIndex(Indexes.ascending("index"), new IndexOptions().unique(true))
      logger.debug("put {} of length {} end", key, records.length)
      lock.unlock()
    } finally {
      lock.unlock()
    }
  }

  override def get(key: String): List[Tuple] = {
    lock.lock()
    try {
      logger.debug("get {} start", key)
      val collection = database.getCollection(key)
      val cursor = collection.find().sort(Sorts.ascending("index")).cursor()
      val recordBuffer = new ListBuffer[Tuple]()
      while (cursor.hasNext) {
        recordBuffer += json2tuple(cursor.next().get("record").toString)
      }
      lock.unlock()
      val res = recordBuffer.toList
      logger.debug("get {} of length {} end", key, res.length)
      res
    } finally {
      lock.unlock()
    }
  }

  override def remove(key: String): Unit = {
    lock.lock()
    try {
      logger.debug("remove {} start", key)
      collectionSet.remove(key)
      database.getCollection(key).drop()
      logger.debug("remove {} end", key)
      lock.unlock()
    } finally {
      lock.unlock()
    }
  }

  override def dump(): Unit = {
    throw new Exception("not implemented")
  }

  override def load(): Unit = {
    throw new Exception("not implemented")
  }

  override def close(): Unit = {
    this.client.close()
  }

}
