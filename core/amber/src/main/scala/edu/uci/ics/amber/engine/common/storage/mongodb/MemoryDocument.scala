package edu.uci.ics.amber.engine.common.storage.mongodb

import edu.uci.ics.amber.engine.common.storage.{BufferedItemWriter, VirtualDocument}
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import java.net.URI
import scala.collection.mutable.ArrayBuffer

class MemoryDocument[T >: Null <: AnyRef] extends VirtualDocument[T] with BufferedItemWriter[T] {

  private val results = new ArrayBuffer[T]()

  override def getURI: URI =
    throw new UnsupportedOperationException("getURI is not supported for MongoDocument")

  override def remove(): Unit =
    synchronized {
      results.clear()
    }

  override def get(): Iterator[T] =
    synchronized {
      results.to(Iterator)
    }

  override def getItem(i: Int): T =
    synchronized {
      results.apply(i)
    }

  override def getRange(from: Int, to: Int): Iterator[T] =
    synchronized {
      results.slice(from, to).to(Iterator)
    }

  override def getAfter(offset: Int): Iterator[T] =
    synchronized {
      results.slice(offset, results.size).to(Iterator)
    }

  override def getCount: Long = results.length
  override def append(item: T): Unit =
    synchronized {
      results += item
    }

  override def write(): BufferedItemWriter[T] = this

  override val bufferSize: Int = 1024

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def putOne(item: T): Unit =
    synchronized {
      results += item
    }

  override def removeOne(item: T): Unit =
    synchronized {
      results -= item
    }
}
