package edu.uci.ics.amber.core.storage.util.mongo

import com.mongodb.client.{MongoClient, MongoClients, MongoCollection, MongoDatabase}
import edu.uci.ics.amber.core.storage.StorageConfig
import org.bson.Document

import java.util

object MongoDatabaseManager {

  val url: String = StorageConfig.mongodbUrl
  val databaseName: String = StorageConfig.mongodbDatabaseName
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

}
