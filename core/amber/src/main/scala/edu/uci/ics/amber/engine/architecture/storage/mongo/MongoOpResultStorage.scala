package edu.uci.ics.amber.engine.architecture.storage.mongo

import com.fasterxml.jackson.databind.JsonNode
import com.mongodb.BasicDBObject
import com.mongodb.client.{MongoClient, MongoClients, MongoDatabase}
import edu.uci.ics.amber.engine.architecture.storage.OpResultStorage
import edu.uci.ics.amber.engine.architecture.storage.mongo.MongoOpResultStorage.{
  json2tuple,
  tuple2json
}
import edu.uci.ics.texera.Utils.objectMapper
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.{
  inferSchemaFromRows,
  parseField
}
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, Schema}
import edu.uci.ics.texera.workflow.operators.source.scan.json.JSONUtil.JSONToMap
import org.bson.Document

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.parallel.mutable.ParHashSet

object MongoOpResultStorage {
  protected def tuple2json(tuple: Tuple): String = {
    tuple.asKeyValuePairJson().toString
  }

  protected def json2tuple(json: String): Tuple = {
    var fieldNames = Set[String]()

    val allFields: ArrayBuffer[Map[String, String]] = ArrayBuffer()

    val root: JsonNode = objectMapper.readTree(json)
    if (root.isObject) {
      val fields: Map[String, String] = JSONToMap(root)
      fieldNames = fieldNames.++(fields.keySet)
      allFields += fields
    }

    val sortedFieldNames = fieldNames.toList

    val attributeTypes = inferSchemaFromRows(allFields.iterator.map(fields => {
      val result = ArrayBuffer[Object]()
      for (fieldName <- sortedFieldNames) {
        if (fields.contains(fieldName)) {
          result += fields(fieldName)
        } else {
          result += null
        }
      }
      result.toArray
    }))

    val schema = Schema.newBuilder
      .add(
        sortedFieldNames.indices
          .map(i => new Attribute(sortedFieldNames(i), attributeTypes(i)))
          .asJava
      )
      .build

    try {
      val fields = scala.collection.mutable.ArrayBuffer.empty[Object]
      val data = JSONToMap(objectMapper.readTree(json))

      for (fieldName <- schema.getAttributeNames.asScala) {
        if (data.contains(fieldName)) {
          fields += parseField(data(fieldName), schema.getAttribute(fieldName).getType)
        } else {
          fields += null
        }
      }
      Tuple.newBuilder(schema).addSequentially(fields.toArray).build()
    } catch {
      case e: Exception => throw e
    }
  }
}

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
      val hashMap = new util.HashMap[String, Object]()
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
