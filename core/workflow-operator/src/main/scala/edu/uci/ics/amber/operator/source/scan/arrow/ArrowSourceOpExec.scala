package edu.uci.ics.amber.operator.source.scan.arrow

import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.core.storage.DocumentFactory
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowFileReader
import edu.uci.ics.amber.core.tuple.{Schema, TupleLike}
import edu.uci.ics.amber.operator.source.scan.FileDecodingMethod
import edu.uci.ics.amber.util.ArrowUtils

import java.net.URI
import java.nio.file.{Files}
import java.nio.file.StandardOpenOption

class ArrowSourceOpExec(
    fileUri: String,
    fileEncoding: FileDecodingMethod,
    limit: Option[Int],
    offset: Option[Int],
    schemaFunc: () => Schema
) extends SourceOperatorExecutor {

  var reader: ArrowFileReader = _
  var root: VectorSchemaRoot = _
  var schema: Schema = _

  override def open(): Unit = {
    val file = DocumentFactory.newReadonlyDocument(new URI(fileUri)).asFile()
    val allocator = new RootAllocator()
    val channel = Files.newByteChannel(file.toPath, StandardOpenOption.READ)
    reader = new ArrowFileReader(channel, allocator)
    root = reader.getVectorSchemaRoot
    schema = schemaFunc()
    reader.loadNextBatch()
  }

  override def produceTuple(): Iterator[TupleLike] = {
    val rowIterator = new Iterator[TupleLike] {
      private var currentIndex = 0

      override def hasNext: Boolean = currentIndex < root.getRowCount

      override def next(): TupleLike = {
        val tuple = ArrowUtils.getTexeraTuple(currentIndex, root)
        currentIndex += 1
        tuple
      }
    }

    var tupleIterator = rowIterator.drop(offset.getOrElse(0))
    if (limit.isDefined) tupleIterator = tupleIterator.take(limit.get)
    tupleIterator
  }

  override def close(): Unit = {
    if (reader != null) {
      reader.close()
    }
    if (root != null) {
      root.close()
    }
  }
}
