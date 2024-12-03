package edu.uci.ics.amber.operator.source.scan.arrow

import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.core.storage.DocumentFactory
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowFileReader
import edu.uci.ics.amber.core.tuple.{Schema, TupleLike}
import edu.uci.ics.amber.util.ArrowUtils

import java.net.URI
import java.nio.file.{Files}
import java.nio.file.StandardOpenOption

class ArrowSourceOpExec(
    fileUri: String,
    limit: Option[Int],
    offset: Option[Int],
    schemaFunc: () => Schema
) extends SourceOperatorExecutor {

  private var reader: Option[ArrowFileReader] = None
  private var root: Option[VectorSchemaRoot] = None
  private var schema: Option[Schema] = None
  private var allocator: Option[RootAllocator] = None

  override def open(): Unit = {
    try {
      val file = DocumentFactory.newReadonlyDocument(new URI(fileUri)).asFile()
      val alloc = new RootAllocator()
      allocator = Some(alloc)
      val channel = Files.newByteChannel(file.toPath, StandardOpenOption.READ)
      val arrowReader = new ArrowFileReader(channel, alloc)
      val vectorRoot = arrowReader.getVectorSchemaRoot
      schema = Some(schemaFunc())
      arrowReader.loadNextBatch()
      reader = Some(arrowReader)
      root = Some(vectorRoot)
    } catch {
      case e: Exception =>
        close() // Ensure resources are closed in case of an error
        throw new RuntimeException("Failed to open Arrow source", e)
    }
  }

  override def produceTuple(): Iterator[TupleLike] = {
    val rowIterator = new Iterator[TupleLike] {
      private var currentIndex = 0

      override def hasNext: Boolean = root.exists(_.getRowCount > currentIndex)

      override def next(): TupleLike = {
        root match {
          case Some(r) =>
            val tuple = ArrowUtils.getTexeraTuple(currentIndex, r)
            currentIndex += 1
            tuple
          case None =>
            throw new NoSuchElementException("No more tuples available")
        }
      }
    }

    var tupleIterator = rowIterator.drop(offset.getOrElse(0))
    if (limit.isDefined) tupleIterator = tupleIterator.take(limit.get)
    tupleIterator
  }

  override def close(): Unit = {
    reader.foreach(_.close())
    root.foreach(_.close())
    allocator.foreach(_.close())
  }
}
