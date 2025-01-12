package edu.uci.ics.amber.operator.source.cache

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.storage.model.VirtualDocument
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}

import java.net.URI

class CacheSourceOpExec(storageUri: URI) extends SourceOperatorExecutor with LazyLogging {
  private val storage =
    DocumentFactory.openDocument(storageUri)._1.asInstanceOf[VirtualDocument[Tuple]]

  override def produceTuple(): Iterator[TupleLike] = storage.get()

}
