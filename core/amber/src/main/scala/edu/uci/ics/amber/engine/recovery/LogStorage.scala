package edu.uci.ics.amber.engine.recovery

abstract class LogStorage[T](logName: String) {

  // for persist:
  def persistElement(elem: T)

  // for recovery:
  def load(): Iterable[T]

  // clear everything
  def clear(): Unit

}
