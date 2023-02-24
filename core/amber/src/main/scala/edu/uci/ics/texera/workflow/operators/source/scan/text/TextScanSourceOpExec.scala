package edu.uci.ics.texera.workflow.operators.source.scan.text

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

import java.io.{BufferedReader, FileReader, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.asScalaIteratorConverter

class TextScanSourceOpExec private[text] (
    val desc: TextScanSourceOpDesc,
    val startOffset: Int,
    val endOffset: Int,
    val outputAsSingleTuple: Boolean
) extends SourceOperatorExecutor {
  private var schema: Schema = _
  private var reader: BufferedReader = _
  private var rows: Iterator[String] = _
  private var path: Path = _

  @throws[IOException]
  override def produceTexeraTuple(): Iterator[Tuple] = {
    if (outputAsSingleTuple) {
      Iterator(
        Tuple
          .newBuilder(schema)
          .add(
            schema.getAttribute("file"),
            new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
          )
          .build()
      )
    } else { // normal text file scan mode
      rows.map(line => {
        Tuple.newBuilder(schema).add(schema.getAttribute("line"), line).build()
      })
    }
  }

  override def open(): Unit = {
    schema = desc.inferSchema()
    if (outputAsSingleTuple) {
      path = Path.of(desc.filePath.get)
    } else {
      reader = new BufferedReader(new FileReader(desc.filePath.get))
      rows = reader.lines().iterator().asScala.slice(startOffset, endOffset)
    }
  }

  override def close(): Unit = if (!outputAsSingleTuple) reader.close()
}
