package edu.uci.ics.amber.core.storage.util.mongo

import com.mongodb.client.model.{Aggregates, IndexOptions, Indexes, Sorts}
import com.mongodb.client.{FindIterable, MongoCollection}
import org.bson.Document

import java.util.Date
import scala.jdk.CollectionConverters._

class MongoCollectionManager(collection: MongoCollection[Document]) {

  /**
    * Insert multiple documents into the collection.
    */
  def insertMany(documents: Iterable[Document]): Unit = {
    collection.insertMany(documents.toSeq.asJava)
  }

  /**
    * Delete documents matching the condition.
    */
  def deleteMany(condition: Document): Unit = {
    collection.deleteMany(condition)
  }

  /**
    * Get the total count of documents in the collection.
    */
  def getCount: Long = {
    collection.countDocuments()
  }

  def accessDocuments: FindIterable[Document] = {
    collection.find()
  }

  /**
    * Calculate numeric statistics (min, max, mean) for numeric fields.
    */
  def calculateNumericStats(): Map[String, Map[String, Double]] = {
    val numericFields = detectNumericFields()

    numericFields.flatMap { field =>
      val fieldAsType =
        new Document("$convert", new Document("input", "$" + field).append("to", "double"))
      val projection = new Document(field, fieldAsType)
      val groupDoc = new Document("_id", null)
        .append("minValue", new Document("$min", "$" + field))
        .append("maxValue", new Document("$max", "$" + field))
        .append("meanValue", new Document("$avg", "$" + field))

      val pipeline = Seq(
        new Document("$project", projection),
        new Document("$group", groupDoc)
      )

      val result = collection.aggregate(pipeline.asJava).iterator().asScala.toList
      if (result.nonEmpty) {
        val doc = result.head
        val stats = Map(
          "min" -> doc.get("minValue").asInstanceOf[Number].doubleValue(),
          "max" -> doc.get("maxValue").asInstanceOf[Number].doubleValue(),
          "mean" -> doc.get("meanValue").asInstanceOf[Number].doubleValue()
        )
        Some(field -> stats)
      } else {
        None
      }
    }.toMap
  }

  /**
    * Calculate date statistics (min, max) for date fields.
    */
  def calculateDateStats(): Map[String, Map[String, Date]] = {
    val dateFields = detectDateFields()

    dateFields.flatMap { field =>
      val fieldAsType =
        new Document("$convert", new Document("input", "$" + field).append("to", "date"))
      val projection = new Document(field, fieldAsType)
      val groupDoc = new Document("_id", null)
        .append("minValue", new Document("$min", "$" + field))
        .append("maxValue", new Document("$max", "$" + field))

      val pipeline = Seq(
        new Document("$project", projection),
        new Document("$group", groupDoc)
      )

      val result = collection.aggregate(pipeline.asJava).iterator().asScala.toList
      if (result.nonEmpty) {
        val doc = result.head
        val stats = Map(
          "min" -> doc.get("minValue").asInstanceOf[Date],
          "max" -> doc.get("maxValue").asInstanceOf[Date]
        )
        Some(field -> stats)
      } else {
        None
      }
    }.toMap
  }

  /**
    * Calculate categorical statistics (value counts) for categorical fields.
    */
  def calculateCategoricalStats(): Map[String, Map[String, Map[String, Integer]]] = {
    val categoricalFields = detectCategoricalFields()

    categoricalFields.flatMap { field =>
      val pipeline = Seq(
        Aggregates.group("$" + field, com.mongodb.client.model.Accumulators.sum("count", 1)),
        Aggregates.sort(Sorts.descending("count")),
        Aggregates.limit(1000)
      )

      val result = collection.aggregate(pipeline.asJava).iterator().asScala.toList
      if (result.nonEmpty) {
        val counts = result.map(doc => doc.getString("_id") -> doc.getInteger("count")).toMap
        Some(field -> Map("counts" -> counts))
      } else {
        None
      }
    }.toMap
  }

  /**
    * Detect numeric fields by sampling the first 10 documents.
    */
  private def detectNumericFields(): Set[String] = {
    sampleDocuments().flatMap { doc =>
      doc.keySet().asScala.filter { key =>
        doc.get(key) match {
          case _: java.lang.Integer | _: java.lang.Long | _: java.lang.Float |
              _: java.lang.Double =>
            true
          case _ => false
        }
      }
    }.toSet
  }

  /**
    * Detect date fields by sampling the first 10 documents.
    */
  private def detectDateFields(): Set[String] = {
    sampleDocuments().flatMap { doc =>
      doc.keySet().asScala.filter(key => doc.get(key).isInstanceOf[Date])
    }.toSet
  }

  /**
    * Detect categorical fields by sampling the first 10 documents.
    */
  private def detectCategoricalFields(): Set[String] = {
    sampleDocuments().flatMap { doc =>
      doc.keySet().asScala.filter(key => doc.get(key).isInstanceOf[String])
    }.toSet
  }

  /**
    * Helper method to sample the first 10 documents.
    */
  private def sampleDocuments(): Seq[Document] = {
    collection.find().limit(10).iterator().asScala.toSeq
  }
}
