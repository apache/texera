package edu.uci.ics.texera.workflow.operators.sink.storage

import com.mongodb.client.model.Sorts
import com.mongodb.client.MongoCursor
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.model.tuple.{Schema, Tuple}
import edu.uci.ics.texera.web.storage.{MongoCollectionManager, MongoDatabaseManager}
import edu.uci.ics.amber.engine.common.model.tuple.TupleUtils.document2Tuple
import org.bson.Document

import scala.collection.mutable

class MongoDBSinkStorage(id: String) extends SinkStorageReader {

  val commitBatchSize: Int = AmberConfig.sinkStorageMongoDBConfig.getInt("commit-batch-size")
  MongoDatabaseManager.dropCollection(id)
  @transient lazy val collectionMgr: MongoCollectionManager = MongoDatabaseManager.getCollection(id)

  var previousCount: mutable.Map[String, Long] = mutable.Map()
  var previousNumStats: mutable.Map[String, (Double, Double, Double)] = mutable.Map()
  var previousDateStats: mutable.Map[String, (java.util.Date, java.util.Date)] = mutable.Map()
  var previousCatStats: Map[String, Map[String, Int]] = Map()
  val previousReachedLimit: mutable.Map[String, Int] = mutable.Map()

  class MongoDBSinkStorageWriter(bufferSize: Int) extends SinkStorageWriter {
    var uncommittedInsertions: mutable.ArrayBuffer[Tuple] = _
    @transient lazy val collection: MongoCollectionManager = MongoDatabaseManager.getCollection(id)

    override def open(): Unit = {
      uncommittedInsertions = new mutable.ArrayBuffer[Tuple]()
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
    }.iterator.to(Iterable)
  }

  override def getAll: Iterable[Tuple] = {
    val cursor = collectionMgr.accessDocuments.sort(Sorts.ascending("_id")).cursor()
    mkTupleIterable(cursor)
  }

  override def getStorageWriter: SinkStorageWriter =
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

  override def getSchema: Schema = {
    synchronized {
      schema
    }
  }

  override def setSchema(schema: Schema): Unit = {
    // Now we require mongodb version > 5 to support "." in field names
    synchronized {
      this.schema = schema
    }
  }

  override def getAllFields(): Array[Array[String]] = {
    collectionMgr.getAllColumnNames
  }

  override def getNumericColStats(fields: Iterable[String]): Map[String, Map[String, Any]] = {
    var result = mutable.Map[String, Map[String, Any]]()

    fields.foreach(field => {
      var fieldResult = mutable.Map[String, Any]()
      val offset: Long = previousCount.getOrElse(field, 0)
      val stats = collectionMgr.calculateNumericStats(field, offset)

      stats match {
        case Some((minValue, maxValue, meanValue, newCount)) =>
          val (prevMin, prevMax, prevMean) =
            previousNumStats.getOrElse(field, (Double.MaxValue, Double.MinValue, 0.0))

          val newMin =
            if (minValue != null) Math.min(prevMin, minValue.toString.toDouble) else prevMin
          val newMax =
            if (maxValue != null) Math.max(prevMax, maxValue.toString.toDouble) else prevMax
          val newMean =
            if (meanValue != null)
              (prevMean * offset + meanValue.toString.toDouble * newCount) / (offset + newCount)
            else prevMean

          previousNumStats(field) = (newMin, newMax, newMean)
          previousCount.update(field, offset + newCount)

          fieldResult("min") = newMin
          fieldResult("max") = newMax
          fieldResult("mean") = newMean
        case _ =>
          val (prevMin, prevMax, prevMean) =
            previousNumStats.getOrElse(field, (Double.MaxValue, Double.MinValue, 0.0))
          fieldResult("min") = prevMin
          fieldResult("max") = prevMax
          fieldResult("mean") = prevMean
      }

      if (fieldResult.nonEmpty) result(field) = fieldResult.toMap
    })

    result.toMap
  }

  override def getDateColStats(fields: Iterable[String]): Map[String, Map[String, Any]] = {
    var result = mutable.Map[String, Map[String, Any]]()

    fields.foreach(field => {
      var fieldResult = mutable.Map[String, Any]()
      val offset: Long = previousCount.getOrElse(field, 0)
      val stats = collectionMgr.calculateDateStats(field, offset)

      stats match {
        case Some((minValue: java.util.Date, maxValue: java.util.Date, count: Long)) =>
          val (prevMin, prevMax) = previousDateStats.getOrElse(
            field,
            (new java.util.Date(Long.MaxValue), new java.util.Date(Long.MinValue))
          )

          val newMin = if (minValue != null && minValue.before(prevMin)) minValue else prevMin
          val newMax = if (maxValue != null && maxValue.after(prevMax)) maxValue else prevMax

          previousDateStats(field) = (newMin, newMax)

          fieldResult("min") = newMin
          fieldResult("max") = newMax
          previousCount.update(field, offset + count)

        case _ =>
          val (prevMin, prevMax) = previousDateStats.getOrElse(
            field,
            (new java.util.Date(Long.MaxValue), new java.util.Date(Long.MinValue))
          )
          fieldResult("min") = prevMin
          fieldResult("max") = prevMax
      }

      if (fieldResult.nonEmpty) result(field) = fieldResult.toMap
    })

    result.toMap
  }

  override def getCatColStats(fields: Iterable[String]): Map[String, Map[String, Any]] = {
    var result = mutable.Map[String, Map[String, Any]]()

    fields.foreach(field => {
      var fieldResult = mutable.Map[String, Any]()
      var newCount: Long = 0
      val offset: Long = previousCount.getOrElse(field, 0)

      val (newStats, batchReachedLimit) = collectionMgr.calculateCategoricalStats(field, offset)
      var oldStats = previousCatStats.getOrElse(field, Map())

      val reachedLimit = previousReachedLimit.getOrElse(field, 0) == 1 || batchReachedLimit

      newStats.keySet.foreach(key => {
        oldStats += (key -> (oldStats.getOrElse(key, 0) + newStats(key)))
        newCount += newStats(key)
      })
      previousCount.update(field, offset + newCount)
      previousCatStats += (field -> oldStats)
      previousReachedLimit.update(field, if (reachedLimit) 1 else 0)

      val top2 = previousCatStats(field).toSeq.sortBy(-_._2).take(2).map(_._1)
      top2.size match {
        case 2 => {
          fieldResult("firstCat") = if (top2(0) != null) top2(0) else "NULL"
          fieldResult("secondCat") = if (top2(1) != null) top2(1) else "NULL"
          val first =
            (previousCatStats(field)(top2(0)).toDouble / (offset + newCount).toDouble) * 100
          val second =
            (previousCatStats(field)(top2(1)).toDouble / (offset + newCount).toDouble) * 100
          fieldResult("firstPercent") = first
          fieldResult("secondPercent") = second
          fieldResult("other") = (100 - first - second)
          fieldResult("reachedLimit") = if (reachedLimit) 1 else 0
        }
        case 1 => {
          fieldResult("firstCat") = if (top2(0) != null) top2(0) else "NULL"
          fieldResult("secondCat") = ""
          fieldResult("firstPercent") =
            (previousCatStats(field)(top2(0)).toDouble / (offset + newCount).toDouble) * 100
          fieldResult("secondPercent") = 0
          fieldResult("other") = 0
          fieldResult("reachedLimit") = if (reachedLimit) 1 else 0
        }
        case _ => None
      }

      if (fieldResult.nonEmpty) result(field) = fieldResult.toMap
    })

    result.toMap
  }
}
