package edu.uci.ics.amber.operator.source.cache

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.core.storage.model.VirtualDocument
import edu.uci.ics.amber.core.storage.{DocumentFactory, VFSResourceType, VFSURIFactory}
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}

import java.net.URI

class CacheSourceOpExec(storageUri: URI) extends SourceOperatorExecutor with LazyLogging {
  val (_, _, _, resourceType) = VFSURIFactory.decodeURI(storageUri)
  if (resourceType != VFSResourceType.RESULT) {
    throw new RuntimeException("The storage URI must point to a result storage")
  }

  private val storage =
    DocumentFactory.openDocument(storageUri)._1.asInstanceOf[VirtualDocument[Tuple]]

  override def produceTuple(): Iterator[TupleLike] = storage.get()

}
