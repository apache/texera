package edu.uci.ics.texera.workflow.common.storage.mongo

import com.mongodb.BasicDBObject
import com.mongodb.client.model.{IndexOptions, Indexes, Sorts}
import com.mongodb.client.{MongoClient, MongoClients, MongoDatabase}
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.TupleUtils.{json2tuple, tuple2json}
import org.bson.Document

import java.util
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class MongoOpResultStorage extends OpResultStorage {

  val url: String = AmberUtils.amberConfig.getString("cache.mongodb.url")
  val databaseName: String = AmberUtils.amberConfig.getString("cache.mongodb.database")
  val client: MongoClient = MongoClients.create(url)
  val database: MongoDatabase = client.getDatabase(databaseName)
  val collectionSet: mutable.HashSet[String] = mutable.HashSet[String]()
  private val lock = new ReentrantLock()

  override def put(key: String, records: List[Tuple]): Unit = {
    lock.lock()
    try {
      logger.debug(s"put $key of length ${records.stringPrefix} start")
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
      logger.debug(s"put $key of length ${records.length} end")
    } finally {
      lock.unlock()
    }
  }

  override def get(key: String): List[Tuple] = {
    lock.lock()
    try {
      logger.debug(s"get $key start")
      val collection = database.getCollection(key)
      val cursor = collection.find().sort(Sorts.ascending("index")).cursor()
      val recordBuffer = new ListBuffer[Tuple]()
      while (cursor.hasNext) {
        recordBuffer += json2tuple(cursor.next().get("record").toString)
      }
      val res = recordBuffer.toList
      logger.debug(s"get $key of length ${res.length} end")
      res
    } finally {
      lock.unlock()
    }
  }

  override def remove(key: String): Unit = {
    lock.lock()
    try {
      logger.debug(s"remove $key start")
      collectionSet.remove(key)
      database.getCollection(key).drop()
      logger.debug(s"remove $key end")
    } finally {
      lock.unlock()
    }
  }

  override def dump(): Unit = {
    throw new NotImplementedError()
  }

  override def load(): Unit = {
    throw new NotImplementedError()
  }

  override def close(): Unit = {
    this.client.close()
  }

}
