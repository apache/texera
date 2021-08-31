package edu.uci.ics.amber.engine.architecture.storage.mongo

import com.mongodb.BasicDBObject
import com.mongodb.client.model.{IndexOptions, Indexes, Sorts}
import com.mongodb.client.{MongoClient, MongoClients, MongoDatabase}
import com.typesafe.scalalogging.Logger
import edu.uci.ics.amber.engine.architecture.storage.OpResultStorage
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.TupleUtils.{json2tuple, tuple2json}
import org.bson.Document

import java.util
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class MongoOpResultStorage extends OpResultStorage {

  private val logger = Logger(this.getClass.getName)

  private val lock = new ReentrantLock()

  val url: String = Constants.mongodbUrl

  val databaseName: String = Constants.mongodbDatabaseName

  val client: MongoClient = MongoClients.create(url)

  val database: MongoDatabase = client.getDatabase(databaseName)

  val collectionSet: mutable.HashSet[String] = mutable.HashSet[String]()

  override def put(opID: String, records: List[Tuple]): Unit = {
    lock.lock()
    logger.debug("put {} start", opID)
    val collection = database.getCollection(opID)
    if (collectionSet.contains(opID)) {
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
    logger.debug("put {} end", opID)
    lock.unlock()
  }

  override def get(opID: String): List[Tuple] = {
    lock.lock()
    logger.debug("get {} start", opID)
    val collection = database.getCollection(opID)
    val cursor = collection.find().sort(Sorts.ascending("index")).cursor()
    val recordBuffer = new ListBuffer[Tuple]()
    while (cursor.hasNext) {
      recordBuffer += json2tuple(cursor.next().get("record").toString)
    }
    lock.unlock()
    logger.debug("get {} end", opID)
    recordBuffer.toList
  }

  override def remove(opID: String): Unit = {
    lock.lock()
    logger.debug("remove {} start", opID)
    collectionSet.remove(opID)
    database.getCollection(opID).drop()
    logger.debug("remove {} end", opID)
    lock.unlock()
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
