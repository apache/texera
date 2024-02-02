package edu.uci.ics.texera.workflow.operators.source.scan

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.parseField
import org.apache.commons.compress.archivers.{ArchiveInputStream, ArchiveStreamFactory}
import org.apache.commons.io.IOUtils.toByteArray

import java.io._
import scala.jdk.CollectionConverters.IteratorHasAsScala

class FileScanSourceOpExec private[scan] (val desc: FileScanSourceOpDesc)
    extends SourceOperatorExecutor {

  @throws[IOException]
  override def produceTexeraTuple(): Iterator[Tuple] = {
    var filenameIt: Iterator[String] = Iterator.empty
    val fileEntries: Iterator[InputStream] =
      if (desc.extract) {
        val inputStream: ArchiveInputStream = new ArchiveStreamFactory().createArchiveInputStream(
          new BufferedInputStream(new FileInputStream(desc.filePath.get))
        )
        val (it1, it2) = Iterator
          .continually(inputStream.getNextEntry)
          .takeWhile(_ != null)
          .filterNot(_.getName.startsWith("__MACOSX"))
          .duplicate
        filenameIt = it1.map(entry => entry.getName)
        it2.map(_ => inputStream)
      } else {
        Iterator(new FileInputStream(desc.filePath.get))
      }

    if (desc.attributeType.isSingle) {
      fileEntries.zipAll(filenameIt, null, null).map {
        case (entry, fileName) =>
          val TupleBuilder = Tuple
            .newBuilder(desc.sourceSchema())
            .add(
              if (desc.outputFileName) {
                desc.sourceSchema().getAttributes.get(1)
              } else {
                desc.sourceSchema().getAttributes.get(0)
              },
              desc.attributeType match {
                case FileAttributeType.SINGLE_STRING =>
                  new String(toByteArray(entry), desc.encoding.getCharset)
                case _ => parseField(toByteArray(entry), desc.attributeType.getType)
              }
            )
          if (desc.outputFileName) {
            TupleBuilder.add(
              desc.sourceSchema().getAttributes.get(0),
              fileName
            )
          }
          TupleBuilder.build()
      }
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
          .map(line => {
            Tuple
              .newBuilder(desc.sourceSchema())
              .add(
                desc.sourceSchema().getAttributes.get(0),
                desc.attributeType match {
                  case FileAttributeType.SINGLE_STRING => line
                  case _                               => parseField(line, desc.attributeType.getType)
                }
              )
              .build()
          })
      )
    }
  }

  override def open(): Unit = {}

  override def close(): Unit = {}
}
