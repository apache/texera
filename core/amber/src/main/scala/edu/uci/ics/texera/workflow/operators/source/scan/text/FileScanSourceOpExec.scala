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
    val filePath = desc.filePath.getOrElse(throw new RuntimeException("Empty file path accepted."))

    val fileEntries: Iterator[InputStream] = if (desc.unzip) {
      val zipReader = new ZipFile(filePath)
      zipReader.entries()
        .asScala
        .filterNot(entry => entry.getName.startsWith("__MACOSX"))
        .map(entry => zipReader.getInputStream(entry))
    } else {
      Iterator(new FileInputStream(filePath))
    }


    if (desc.attributeType.isSingle) {
      fileEntries.map(entry => produceSingleTuple(entry))
    } else {
      fileEntries.flatMap(entry => produceMultiTuples(entry))
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

  private def produceMultiTuples(stream: InputStream): Iterator[Tuple] = {
    new BufferedReader(new InputStreamReader(stream, desc.encoding.getCharset))
      .lines()
      .iterator()
      .asScala
      .slice(
        desc.fileScanOffset.getOrElse(0),
        desc.fileScanOffset.getOrElse(0) + desc.fileScanLimit.getOrElse(Int.MaxValue)
      )
      .map(line => produceSingleTuple( new ByteArrayInputStream(line.getBytes))
      )
  }

  override def open(): Unit = {}

  override def close(): Unit = {}
}
