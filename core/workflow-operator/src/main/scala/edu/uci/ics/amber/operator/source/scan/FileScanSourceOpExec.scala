package edu.uci.ics.amber.operator.source.scan

import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.tuple.AttributeTypeUtils.parseField
import edu.uci.ics.amber.core.tuple.TupleLike
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import org.apache.commons.io.IOUtils.toByteArray
import java.io._
import java.net.URI
import java.nio.ByteBuffer
import java.util.zip.ZipInputStream
import scala.collection.mutable
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.Using
import scala.collection.mutable.ArrayBuffer

class FileScanSourceOpExec private[scan] (
    descString: String
) extends SourceOperatorExecutor {
  private val desc: FileScanSourceOpDesc =
    objectMapper.readValue(descString, classOf[FileScanSourceOpDesc])

  // Size of each chunk when reading large files (1GB)
  private val BufferSizeBytes: Int = 1 * 1024 * 1024 * 1024

  /**
    * Reads an InputStream into a List of ByteBuffers.
    * This allows handling files up to 1TB (1GB * 1000 sub-columns).
    *
    * @param input the input stream to read
    * @return a List of ByteBuffers containing the data
    */
  private def readToByteBuffers(input: InputStream): List[ByteBuffer] = {
    Using.resource(input) { inputStream =>
      val buffers = ArrayBuffer.empty[ByteBuffer]
      val buffer = new Array[Byte](BufferSizeBytes)

      Iterator
        .continually(inputStream.read(buffer))
        .takeWhile(_ != -1)
        .filter(_ > 0)
        .foreach { bytesRead =>
          val byteBuffer = ByteBuffer.allocate(bytesRead)
          byteBuffer.put(buffer, 0, bytesRead)
          byteBuffer.flip() // Prepare for reading
          buffers += byteBuffer
        }

      buffers.toList
    }
  }

  @throws[IOException]
  override def produceTuple(): Iterator[TupleLike] = {
    var filenameIt: Iterator[String] = Iterator.empty
    val fileEntries: Iterator[InputStream] = {
      val is = DocumentFactory.openReadonlyDocument(new URI(desc.fileName.get)).asInputStream()
      if (desc.extract) {
        val inputStream = new ZipInputStream(new BufferedInputStream(is))
        val (it1, it2) = Iterator
          .continually(inputStream.getNextEntry)
          .takeWhile(_ != null)
          .filterNot(_.getName.startsWith("__MACOSX"))
          .duplicate
        filenameIt = it1.map(entry => entry.getName)
        it2.map(_ => inputStream)
      } else {
        Iterator(is)
      }
    }

    if (desc.attributeType.isSingle) {
      fileEntries.zipAll(filenameIt, null, null).map {
        case (entry, fileName) =>
          val fields: mutable.ListBuffer[Any] = mutable.ListBuffer()
          if (desc.outputFileName) {
            fields.addOne(fileName)
          }
          fields.addOne(desc.attributeType match {
            case FileAttributeType.SINGLE_STRING =>
              new String(toByteArray(entry), desc.fileEncoding.getCharset)
            case _ =>
              val buffers = readToByteBuffers(entry)
              parseField(buffers, desc.attributeType.getType)
          })
          TupleLike(fields.toSeq: _*)
      }
    } else {
      fileEntries.flatMap(entry =>
        new BufferedReader(new InputStreamReader(entry, desc.fileEncoding.getCharset))
          .lines()
          .iterator()
          .asScala
          .slice(
            desc.fileScanOffset.getOrElse(0),
            desc.fileScanOffset.getOrElse(0) + desc.fileScanLimit.getOrElse(Int.MaxValue)
          )
          .map(line => {
            TupleLike(desc.attributeType match {
              case FileAttributeType.SINGLE_STRING => line
              case _                               => parseField(line, desc.attributeType.getType)
            })
          })
      )
    }
  }
}
