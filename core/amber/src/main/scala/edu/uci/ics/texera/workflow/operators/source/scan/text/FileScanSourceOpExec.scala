package edu.uci.ics.texera.workflow.operators.source.scan.text

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils
import org.apache.commons.compress.archivers.{ArchiveEntry, ArchiveStreamFactory}
import org.apache.commons.io.IOUtils.toByteArray

import java.io._
import java.nio.file.{Files, Paths}
import java.util.zip.ZipFile
import scala.collection.mutable
import scala.jdk.CollectionConverters.{asScalaIteratorConverter, enumerationAsScalaIteratorConverter}

class FileScanSourceOpExec private[text] (val desc: FileScanSourceOpDesc)
    extends SourceOperatorExecutor {

  @throws[IOException]
  override def produceTexeraTuple(): Iterator[Tuple] = {
    if (desc.unzip) {
      val zipReader = new ZipFile(desc.filePath.get)
      val entries = zipReader.entries().asScala.filterNot(entry => entry.getName.startsWith("__MACOSX"))
      if (desc.attributeType.isSingle) {
        entries.map(entry => produceSingleTuple(toByteArray(zipReader.getInputStream(entry))))
      } else {
        entries.flatMap(entry => multipleTuple(zipReader.getInputStream(entry)))
      }
    } else {
      if (desc.attributeType.isSingle) {
        Iterator(produceSingleTuple(Files.readAllBytes(Paths.get(desc.filePath.get))))
      } else {
        multipleTuple(new FileInputStream(desc.filePath.get))
      }
    }
  }

  private def produceSingleTuple(field: Object): Tuple =
    Tuple
      .newBuilder(desc.sourceSchema())
      .add(
        desc.sourceSchema().getAttributes.get(0),
        desc.attributeType match {
          case FileAttributeType.SINGLE_STRING => new String(field.asInstanceOf[Array[Byte]], desc.encoding.getCharset)
          case _ => AttributeTypeUtils.parseField(field, desc.attributeType.getType)
        }
      )
      .build()

  private def multipleTuple(stream: InputStream): Iterator[Tuple] = {
    new BufferedReader(new InputStreamReader(stream, desc.encoding.getCharset))
      .lines()
      .iterator()
      .asScala
      .slice(
        desc.fileScanOffset.getOrElse(0),
        desc.fileScanOffset.getOrElse(0) + desc.fileScanLimit.getOrElse(Int.MaxValue)
      )
      .map(line => produceSingleTuple(line))
  }

  override def open(): Unit = {}

  override def close(): Unit = {}
}
