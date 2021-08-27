package edu.uci.ics.amber.engine.architecture.storage.mongo

import com.mongodb.BasicDBObject
import com.mongodb.client.{MongoClient, MongoClients, MongoDatabase}
import edu.uci.ics.amber.engine.architecture.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.TupleUtils.{json2tuple, tuple2json}
import org.bson.Document

import java.util
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.mutable.ParHashSet

class MongoOpResultStorage extends OpResultStorage {

  val url = "mongodb://localhost:27017"

  val client: MongoClient = MongoClients.create(url)

  val database: MongoDatabase = client.getDatabase("texera_storage")

  val collectionSet: ParHashSet[String] = ParHashSet[String]()

  /**
    * Put the result of an operator to OpResultStorage.
    *
    * @param opID    The operator ID.
    * @param records The results.
    */
  override def put(opID: String, records: List[Tuple]): Unit = {
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
  }

  /**
    * Retrieve the result of an operator from OpResultStorage
    *
    * @param opID The operator ID.
    * @return The result of this operator.
    */
  override def get(opID: String): List[Tuple] = {
    assert(collectionSet.contains(opID))
    val collection = database.getCollection(opID)
    val cursor = collection.find().cursor()
    val recordBuffer = new ListBuffer[Tuple]()
    while (cursor.hasNext) {
      recordBuffer += json2tuple(cursor.next().get("record").toString)
    }
    recordBuffer.toList
  }

  /**
    * Manually remove an entry from the cache.
    *
    * @param opID The key to remove.
    */
  override def remove(opID: String): Unit = {
    assert(collectionSet.contains(opID))
    database.getCollection(opID).drop()
  }

  /**
    * Dump everything in result storage. Called when the system exits.
    */
  override def dump(): Unit = {
    throw new Exception("not implemented")
  }

  /**
    * Load and initialize result storage. Called when the system init.
    */
  override def load(): Unit = {
    throw new Exception("not implemented")
  }

}
