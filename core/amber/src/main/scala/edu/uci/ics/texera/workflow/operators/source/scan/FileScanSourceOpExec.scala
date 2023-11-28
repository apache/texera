package edu.uci.ics.texera.workflow.operators.source.scan

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.parseField
import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.commons.io.IOUtils.toByteArray

import java.io._
import scala.jdk.CollectionConverters.asScalaIteratorConverter

class FileScanSourceOpExec private[scan] (val desc: FileScanSourceOpDesc)
    extends SourceOperatorExecutor {

  @throws[IOException]
  override def produceTexeraTuple(): Iterator[Tuple] = {
    val fileEntries: Iterator[InputStream] = if (desc.extract) {
      val input = new ArchiveStreamFactory().createArchiveInputStream(
        new BufferedInputStream(new FileInputStream(desc.filePath.get))
      )
      Iterator
        .continually(input.getNextEntry)
        .takeWhile(_ != null)
        .filterNot(_.getName.startsWith("__MACOSX"))
        .map(_ => input)
    } else {
      Iterator(new FileInputStream(desc.filePath.get))
    }

    if (desc.attributeType.isSingle) {
      fileEntries.map(entry => produceSingleTuple(toByteArray(entry)))
    } else {
      fileEntries.flatMap(entry =>
        new BufferedReader(new InputStreamReader(entry, desc.encoding.getCharset))
          .lines()
          .iterator()
          .asScala
          .slice(
            desc.fileScanOffset.getOrElse(0),
            desc.fileScanOffset.getOrElse(0) + desc.fileScanLimit.getOrElse(Int.MaxValue)
          )
          .map(line => produceSingleTuple(line))
      )
    }
  }

  private def produceSingleTuple(field: Object): Tuple = {
    Tuple
      .newBuilder(desc.sourceSchema())
      .add(
        desc.sourceSchema().getAttributes.get(0),
        desc.attributeType match {
          case FileAttributeType.SINGLE_STRING =>
            new String(field.asInstanceOf[Array[Byte]], desc.encoding.getCharset)
          case _ => parseField(field, desc.attributeType.getType)
        }
      )
      .build()
  }

  override def open(): Unit = {}

  override def close(): Unit = {}
}
