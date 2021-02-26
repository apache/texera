package edu.uci.ics.amber.engine.recovery

class EmptyLogStorage[T] extends LogStorage[T]("") {

  override def load(): Iterable[T] = Iterable.empty

  override def persistElement(elem: T): Unit = {}

  override def clear(): Unit = {}
}
