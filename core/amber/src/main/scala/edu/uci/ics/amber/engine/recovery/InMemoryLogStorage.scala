package edu.uci.ics.amber.engine.recovery

import scala.collection.mutable

object InMemoryLogStorage {

  private lazy val logs = new mutable.HashMap[String, mutable.Queue[_]]()

  def getLogOf[T](k: String): mutable.Queue[T] = {
    if (!logs.contains(k)) {
      logs(k) = new mutable.Queue[T]()
    }
    logs(k).asInstanceOf[mutable.Queue[T]]
  }

  def clearLogOf(k: String): Unit = {
    if (logs.contains(k)) {
      logs.remove(k)
    }
  }

}

class InMemoryLogStorage[T](logName: String) extends LogStorage[T](logName) {

  override def persistElement(elem: T): Unit = {
    InMemoryLogStorage.getLogOf(logName).enqueue(elem)
  }

  override def load(): Iterable[T] = {
    InMemoryLogStorage.getLogOf(logName)
  }

  override def clear(): Unit = {
    InMemoryLogStorage.clearLogOf(logName)
  }
}
