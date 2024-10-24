package edu.uci.ics.amber.storage.result.mongo

import com.mongodb.client.model.Aggregates._
import com.mongodb.client.model.{IndexOptions, Indexes}
import com.mongodb.client.{FindIterable, MongoCollection, MongoCursor}
import org.bson.Document

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.jdk.CollectionConverters.{SeqHasAsJava, _}

class MongoCollectionManager(collection: MongoCollection[Document]) {

  def insertOne(document: Document): Unit = {
    collection.insertOne(document)
  }

  def insertMany(documents: Iterable[Document]): Unit = {
    collection.insertMany(documents.toSeq.asJava)
  }

  def deleteMany(condition: Document): Unit = {
    collection.deleteMany(condition)
  }

  def getCount: Long = {
    collection.countDocuments()
  }

  def getColumnNames: Array[String] = {
    ???
  }

  def getAllColumnNames: Array[Array[String]] = {
    var numericFields: Set[String] = Set()
    var categoricalFields: Set[String] = Set()
    var dateFields: Set[String] = Set()

    collection
      .find()
      .limit(10)
      .forEach(doc => {
        val keys = doc.keySet()
        keys.forEach(key => {
          doc.get(key) match {
            case _: java.lang.String                      => categoricalFields += key
            case _: java.lang.Integer | _: java.lang.Long => numericFields += key
            case _: java.lang.Float | _: java.lang.Double => numericFields += key
            case _: java.util.Date                        => dateFields += key
            case _                                        => None
          }
        })
      })

    Array(numericFields.toArray, categoricalFields.toArray, dateFields.toArray)
  }

  def getDocuments(condition: Option[Document]): Iterable[Document] = {
    if (condition.isDefined) {
      val cursor: MongoCursor[Document] = collection.find(condition.get).cursor()
      new Iterator[Document] {
        override def hasNext: Boolean = cursor.hasNext
        override def next(): Document = cursor.next()
      }.iterator.to(Iterable)
    } else {
      Iterable(collection.find().first())
    }
  }

  def createIndex(
      columnName: String,
      ascendingFlag: Boolean,
      timeToLiveInMinutes: Option[Int]
  ): Unit = {
    collection.createIndex(
      Indexes.ascending(columnName),
      new IndexOptions().expireAfter(timeToLiveInMinutes.get, TimeUnit.MINUTES)
    )
  }

  def accessDocuments: FindIterable[Document] = {
    collection.find()
  }

  def calculateNumericStats(fieldName: String, offset: Long): Option[(Any, Any, Any, Long)] = {
    val fieldAsNumber =
      new Document("$convert", new Document("input", "$" + fieldName).append("to", "double"))
    val projection = new Document(fieldName, fieldAsNumber)
    val pipeline = java.util.Arrays.asList(
      new Document("$skip", offset.toInt),
      new Document("$project", projection),
      new Document(
        "$group",
        new Document("_id", null)
          .append("minValue", new Document("$min", "$" + fieldName))
          .append("maxValue", new Document("$max", "$" + fieldName))
          .append("meanValue", new Document("$avg", "$" + fieldName))
          .append("count", new Document("$sum", 1))
      )
    )
    val result = collection.aggregate(pipeline).iterator()
    if (result.hasNext) {
      val doc = result.next()
      val count = doc.get("count").toString.toLong
      Some(
        (doc.get("minValue"), doc.get("maxValue"), doc.get("meanValue"), count)
      )
    } else {
      None
    }
  }

  def calculateDateStats(fieldName: String, offset: Long): Option[(Any, Any, Any)] = {
    val fieldAsDate =
      new Document("$convert", new Document("input", "$" + fieldName).append("to", "date"))
    val projection = new Document(fieldName, fieldAsDate)

    val pipeline = java.util.Arrays.asList(
      new Document("$skip", offset.toInt),
      new Document("$project", projection),
      new Document(
        "$group",
        new Document("_id", null)
          .append("minValue", new Document("$min", "$" + fieldName))
          .append("maxValue", new Document("$max", "$" + fieldName))
          .append("count", new Document("$sum", 1))
      )
    )

    val result = collection.aggregate(pipeline).iterator()
    if (result.hasNext) {
      val doc = result.next()
      Some(
        (
          doc.get("minValue"),
          doc.get("maxValue"),
          doc.get("count").asInstanceOf[Number].longValue()
        )
      )
    } else {
      None
    }
  }

  def calculateCategoricalStats(fieldName: String, offset: Long): (Map[String, Int], Boolean) = {
    val pipeline = java.util.Arrays.asList(
      new Document("$skip", offset.toInt),
      group(
        "$" + fieldName,
        java.util.Arrays.asList(
          com.mongodb.client.model.Accumulators.sum("count", 1)
        )
      ),
      sort(com.mongodb.client.model.Sorts.descending("count")),
      limit(1000)
    )

    val result = collection.aggregate(pipeline).iterator().asScala.toList
    var stats: mutable.Map[String, Int] = mutable.Map()

    result.foreach(doc => {
      stats(doc.getString("_id")) = doc.get("count").asInstanceOf[Number].intValue()
    })

    val reachedLimit = result.size >= 1000

    (stats.toMap, reachedLimit)
  }
}
