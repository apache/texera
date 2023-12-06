package edu.uci.ics.texera.web.storage

import com.mongodb.client.{MongoClient, MongoClients, MongoCollection, MongoDatabase}
import edu.uci.ics.amber.engine.common.AmberConfig
import org.bson.Document
import edu.uci.ics.texera.web.resource.dashboard.user.quota.UserQuotaResource.MongoStorage

import java.util

object MongoDatabaseManager {

  private val storageConfig = AmberConfig.sinkStorageMongoDBConfig
  val url: String = storageConfig.getString("url")
  val databaseName: String = storageConfig.getString("database")
  val client: MongoClient = MongoClients.create(url)
  val database: MongoDatabase = client.getDatabase(databaseName)

  def dropCollection(collectionName: String): Unit = {
    database.getCollection(collectionName).drop()
  }

  def getCollection(collectionName: String): MongoCollectionManager = {
    val collection: MongoCollection[Document] = database.getCollection(collectionName)
    new MongoCollectionManager(collection)
  }

  def isCollectionExist(collectionName: String): Boolean = {
    database.listCollectionNames().into(new util.ArrayList[String]()).contains(collectionName)
  }

  def getDatabaseSize(collectionNames: Array[MongoStorage]): Array[MongoStorage] = {
    var count = 0

    for (collection <- collectionNames) {
      val stats: Document = database.runCommand(new Document("collStats", collection.pointer))
      collectionNames(count) = MongoStorage(
        collection.workflowName,
        stats.getInteger("totalSize").toDouble,
        collection.pointer,
        collection.eid
      )
      count += 1
    }

    collectionNames
  }

}
