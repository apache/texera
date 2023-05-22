package edu.uci.ics.texera.workflow.operators.source.scan.text

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

import java.io.{BufferedReader, FileReader, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters.asScalaIteratorConverter

class TextScanSourceOpExec private[text] (
    val desc: TextScanSourceOpDesc,
    val startOffset: Int,
    val endOffset: Int,
    val outputAttributeName: String
) extends SourceOperatorExecutor {
  private var schema: Schema = _
  private var reader: BufferedReader = _
  private var rows: Iterator[String] = _
  private var path: Path = _

  @throws[IOException]
  override def produceTexeraTuple(): Iterator[Tuple] = {
    if (desc.outputAsSingleTuple) {
      Iterator(
        Tuple
          .newBuilder(schema)
          .add(
            schema.getAttribute(outputAttributeName),
            new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
            // currently using UTF_8 encoding, which will support all files
            // that can be represented using Unicode characters
            // NOTE : currently this mode may not support all binary files,
            // as not all possible binary characters can necessarily be converted to valid UTF-8 strings
          )
          .build()
      )
    } else { // normal text file scan mode
      rows.map(line => {
        Tuple.newBuilder(schema).add(schema.getAttribute(outputAttributeName), line).build()
      })
    }
  }

  override def open(): Unit = {
    schema = desc.inferSchema()
    if (desc.outputAsSingleTuple) {
      path = Paths.get(desc.filePath.get)
    } else {
      reader = new BufferedReader(new FileReader(desc.filePath.get))
      rows = reader.lines().iterator().asScala.slice(startOffset, endOffset)
    }
  }

  // in outputAsSingleTuple mode, Files.readAllBytes handles the closing of file
  override def close(): Unit = if (!desc.outputAsSingleTuple) reader.close()
}
