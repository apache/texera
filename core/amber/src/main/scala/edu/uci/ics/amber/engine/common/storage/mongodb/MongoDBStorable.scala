package edu.uci.ics.amber.engine.common.storage.mongodb

import org.bson.Document

object MongoDBStorable {
  // Function type to convert Any to Document
  type ToDocument = Any => Document

  // Function type to convert (Seq[Any]) to T
  type FromDocument[T] = Seq[Any] => Document => T
}
