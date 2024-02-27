package edu.uci.ics.texera.workflow.operators.source.scan

import edu.uci.ics.amber.engine.common.ISourceOperatorExecutor
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.parseField
import org.apache.commons.compress.archivers.{ArchiveInputStream, ArchiveStreamFactory}
import org.apache.commons.io.IOUtils.toByteArray

import java.io._
import scala.collection.mutable
import scala.jdk.CollectionConverters.IteratorHasAsScala

class FileScanSourceOpExec private[scan] (
    filePath: String,
    fileAttributeType: FileAttributeType,
    fileEncoding: FileDecodingMethod,
    extract: Boolean,
    outputFileName: Boolean,
    fileScanLimit: Option[Int] = None,
    fileScanOffset: Option[Int] = None
) extends ISourceOperatorExecutor {

  @throws[IOException]
  override def produceTuple(): Iterator[TupleLike] = {
    var filenameIt: Iterator[String] = Iterator.empty
    val fileEntries: Iterator[InputStream] =
      if (extract) {
        val inputStream: ArchiveInputStream = new ArchiveStreamFactory().createArchiveInputStream(
          new BufferedInputStream(new FileInputStream(filePath))
        )
        val (it1, it2) = Iterator
          .continually(inputStream.getNextEntry)
          .takeWhile(_ != null)
          .filterNot(_.getName.startsWith("__MACOSX"))
          .duplicate
        filenameIt = it1.map(entry => entry.getName)
        it2.map(_ => inputStream)
      } else {
        Iterator(new FileInputStream(filePath))
      }

    if (fileAttributeType.isSingle) {
      fileEntries.zipAll(filenameIt, null, null).map {
        case (entry, fileName) =>
          val fields: mutable.ListBuffer[Any] = mutable.ListBuffer()
          fields.addOne(fileAttributeType match {
            case FileAttributeType.SINGLE_STRING =>
              new String(toByteArray(entry), fileEncoding.getCharset)
            case _ => parseField(toByteArray(entry), fileAttributeType.getType)
          })
          if (outputFileName) {
            fields.addOne(fileName)
          }

          TupleLike(fields.toSeq: _*)
      }
    } else {
      fileEntries.flatMap(entry =>
        new BufferedReader(new InputStreamReader(entry, fileEncoding.getCharset))
          .lines()
          .iterator()
          .asScala
          .slice(
            fileScanOffset.getOrElse(0),
            fileScanOffset.getOrElse(0) + fileScanLimit.getOrElse(Int.MaxValue)
          )
          .map(line => {
            TupleLike(fileAttributeType match {
              case FileAttributeType.SINGLE_STRING => line
              case _                               => parseField(line, fileAttributeType.getType)
            })
          })
      )
    }
  }

  override def open(): Unit = {}

  override def close(): Unit = {}
}
