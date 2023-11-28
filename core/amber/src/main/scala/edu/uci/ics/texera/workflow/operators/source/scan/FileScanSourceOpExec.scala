package edu.uci.ics.texera.workflow.operators.source.scan

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils
import org.apache.commons.io.IOUtils.toByteArray

import java.io._
import java.util.zip.ZipFile
import scala.jdk.CollectionConverters.{asScalaIteratorConverter, enumerationAsScalaIteratorConverter}

class FileScanSourceOpExec private[text] (val desc: FileScanSourceOpDesc)
    extends SourceOperatorExecutor {

  @throws[IOException]
  override def produceTexeraTuple(): Iterator[Tuple] = {
    val fileEntries: Iterator[InputStream] = if (desc.unzip) {
      val zipReader = new ZipFile(desc.filePath.get)
      zipReader.entries()
        .asScala
        .filterNot(entry => entry.getName.startsWith("__MACOSX"))
        .map(entry => zipReader.getInputStream(entry))
    } else {
      Iterator(new FileInputStream(desc.filePath.get))
    }

    if (desc.attributeType.isSingle) {
      fileEntries.map(entry => produceSingleTuple(entry))
    } else {
      fileEntries.flatMap(entry => new BufferedReader(new InputStreamReader(entry, desc.encoding.getCharset))
        .lines()
        .iterator()
        .asScala
        .slice(
          desc.fileScanOffset.getOrElse(0),
          desc.fileScanOffset.getOrElse(0) + desc.fileScanLimit.getOrElse(Int.MaxValue)
        )
        .map(line => produceSingleTuple(new ByteArrayInputStream(line.getBytes))
        ))
    }
  }

  private def produceSingleTuple(stream: InputStream): Tuple = {
    val line = toByteArray(stream)
    Tuple
      .newBuilder(desc.sourceSchema())
      .add(
        desc.sourceSchema().getAttributes.get(0),
        desc.attributeType match {
          case FileAttributeType.SINGLE_STRING => new String(line, desc.encoding.getCharset)
          case _ => AttributeTypeUtils.parseField(line, desc.attributeType.getType)
        }
      )
      .build()
  }

  override def open(): Unit = {}

  override def close(): Unit = {}
}
