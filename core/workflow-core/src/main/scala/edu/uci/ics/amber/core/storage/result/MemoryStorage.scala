package edu.uci.ics.amber.core.storage.result

import edu.uci.ics.amber.core.storage.result.{SinkStorageReader, SinkStorageWriter}
import edu.uci.ics.amber.core.tuple.{Schema, Tuple}
import edu.uci.ics.amber.core.storage.result.MemoryStorage.storageMapping

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
object MemoryStorage {
  val storageMapping: mutable.Map[String, ArrayBuffer[Tuple]] =
    mutable.HashMap[String, ArrayBuffer[Tuple]]()
}
class MemoryStorage(key: String) extends SinkStorageReader with SinkStorageWriter {

  private def results: ArrayBuffer[Tuple] = {
    if (!storageMapping.contains(key)) {
      storageMapping(key) = new ArrayBuffer[Tuple]()
    }
    storageMapping(key)
  }

  override def getAll: Iterable[Tuple] =
    synchronized {
      results
    }

  override def putOne(tuple: Tuple): Unit =
    synchronized {
      results += tuple
    }

  override def removeOne(tuple: Tuple): Unit =
    synchronized {
      results -= tuple
    }

  override def getAllAfter(offset: Int): Iterable[Tuple] =
    synchronized {
      results.slice(offset, results.size)
    }

  override def clear(): Unit =
    synchronized {
      results.clear()
    }

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def getStorageWriter: SinkStorageWriter = this

  override def getRange(from: Int, to: Int): Iterable[Tuple] =
    synchronized {
      results.slice(from, to)
    }

  override def getCount: Long = results.length

  override def getSchema: Schema = schema

  override def setSchema(schema: Schema): Unit = {
    this.schema = schema
  }
}
