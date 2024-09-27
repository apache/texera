package edu.uci.ics.texera.workflow.operators.sink.storage

import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

import java.util.concurrent.CopyOnWriteArrayList
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.CollectionHasAsScala

class MemoryStorage extends SinkStorageReader with SinkStorageWriter {

  private val results = new CopyOnWriteArrayList[Tuple]()

  override def getAll: Iterable[Tuple] =
    synchronized {
      results.asScala
    }

  override def putOne(tuple: Tuple): Unit =
    synchronized {
      results.add(tuple)
    }

  override def removeOne(tuple: Tuple): Unit =
    synchronized {
      results.remove(tuple)
    }

  override def getAllAfter(offset: Int): Iterable[Tuple] =
    synchronized {
      results.asScala.slice(offset, results.size)
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
      results.asScala.slice(from, to)
    }

  override def getCount: Long = results.size

  override def getSchema: Schema = schema

  override def setSchema(schema: Schema): Unit = {
    this.schema = schema
  }
}
