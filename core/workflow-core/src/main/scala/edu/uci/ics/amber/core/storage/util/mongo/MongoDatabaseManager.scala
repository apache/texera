/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
