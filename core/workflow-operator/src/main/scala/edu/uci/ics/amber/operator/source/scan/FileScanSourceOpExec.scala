package edu.uci.ics.amber.operator.source.scan

import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.tuple.AttributeTypeUtils.parseField
import edu.uci.ics.amber.core.tuple.TupleLike
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import org.apache.commons.compress.archivers.{ArchiveInputStream, ArchiveStreamFactory}
import org.apache.commons.io.IOUtils.toByteArray

import java.io._
import java.net.URI
import scala.collection.mutable
import scala.jdk.CollectionConverters.IteratorHasAsScala

class FileScanSourceOpExec private[scan] (
    descString: String
) extends SourceOperatorExecutor {
  private val desc: FileScanSourceOpDesc =
    objectMapper.readValue(descString, classOf[FileScanSourceOpDesc])

  @throws[IOException]
  override def produceTuple(): Iterator[TupleLike] = {
    var filenameIt: Iterator[String] = Iterator.empty
    val fileEntries: Iterator[InputStream] = {
      val is = DocumentFactory.openReadonlyDocument(new URI(desc.fileName.get)).asInputStream()
      if (desc.extract) {
        val inputStream: ArchiveInputStream = new ArchiveStreamFactory().createArchiveInputStream(
          new BufferedInputStream(is)
        )
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
            case _ => parseField(toByteArray(entry), desc.attributeType.getType)
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
