package edu.uci.ics.amber.operator.source.scan

/**
  * A wrapper around an Interator that automatically invokes a cleanup function
  * when the iterator is fully consumed (i.e. hasNext returns false)
  *
  * This is useful for lazily reading resources like InputStreams and ensuring
  * they are properly closed once no more data is available
  *
  * @param iter the underlying iterator to consume
  * @param onClose a function to call once the iterator is exhausted
  */
class AutoClosingIterator[T](iter: Iterator[T], onClose: () => Unit) extends Iterator[T] {
  private var alreadyClosed = false

  override def hasNext: Boolean = {
    val hn = iter.hasNext
    if (!hn && !alreadyClosed) {
      onClose()
      alreadyClosed = true
    }
    hn
  }

  override def next(): T = iter.next()
}
