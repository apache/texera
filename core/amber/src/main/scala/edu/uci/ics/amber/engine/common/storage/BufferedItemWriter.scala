package edu.uci.ics.amber.engine.common.storage

trait BufferedItemWriter[T] {
  val bufferSize: Int
  def open(): Unit

  def close(): Unit

  def putOne(item: T): Unit

  def removeOne(item: T): Unit
}
