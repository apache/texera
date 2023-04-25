package edu.uci.ics.texera.web.storage

import com.mongodb.client.{MongoClient, MongoClients, MongoCollection, MongoDatabase}
import edu.uci.ics.amber.engine.common.AmberUtils
import org.bson.Document

import java.util

object MongoDatabaseManager {

  val url: String = AmberUtils.amberConfig.getString("storage.mongodb.url")
  val databaseName: String = AmberUtils.amberConfig.getString("storage.mongodb.database")
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
