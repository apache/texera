package edu.uci.ics.texera.workflow.operators.source.scan

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.parseField
import org.apache.commons.io.IOUtils.toByteArray

import java.io._
import java.util.zip.ZipFile
import scala.jdk.CollectionConverters.{
  asScalaIteratorConverter,
  enumerationAsScalaIteratorConverter
}

class FileScanSourceOpExec private[scan] (val desc: FileScanSourceOpDesc)
    extends SourceOperatorExecutor {

  @throws[IOException]
  override def produceTexeraTuple(): Iterator[Tuple] = {
    val fileEntries: Iterator[InputStream] = if (desc.extract) {
      val zipReader = new ZipFile(desc.filePath.get)
      zipReader
        .entries()
        .asScala
        .filterNot(entry => entry.getName.startsWith("__MACOSX"))
        .map(entry => zipReader.getInputStream(entry))
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
