package edu.uci.ics.texera.web.storage

import java.util
import com.mongodb.client.model.Sorts
import com.mongodb.client.{MongoCollection, MongoCursor}
import org.bson.Document
import com.mongodb.client.model.Indexes
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.TupleUtils.document2Tuple

import scala.collection.mutable
import collection.JavaConverters._

class MongoCollectionManager(collection: MongoCollection[Document]) {

  def insertOne(document: Document): Unit = {
    collection.insertOne(document)
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

  def getDocuments(condition: Option[Document]): Iterable[Document] = {
    if(condition.isDefined) {
      val cursor: MongoCursor[Document] = collection.find(condition.get).cursor()
      new Iterator[Document] {
        override def hasNext: Boolean = cursor.hasNext
        override def next(): Document = cursor.next()
      }.toIterable
    }
    else {
      Iterable(collection.find().first())
    }
  }

  def createIndex(columnName: String, ascendingFlag: Boolean, timeToLive: Option[]): Unit = {
    collection.createIndex(
      Indexes.ascending("created_at"),
      new IndexOptions().expireAfter(timeToLive, TimeUnit.MINUTES)
  }
}

