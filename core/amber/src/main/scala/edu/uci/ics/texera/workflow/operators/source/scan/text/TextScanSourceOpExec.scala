package edu.uci.ics.texera.workflow.operators.source.scan.text

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

import java.io.{BufferedReader, FileReader}
import scala.jdk.CollectionConverters.asScalaIteratorConverter

class TextScanSourceOpExec private[text] (val desc: TextScanSourceOpDesc, val startOffset: Int, val endOffset: Int) extends SourceOperatorExecutor{
  private var schema: Schema = _
  private var reader: BufferedReader = _
  private var rows: Iterator[String] = _

  override def produceTexeraTuple(): Iterator[Tuple] = {
    rows.map(line => {
      Tuple.newBuilder(schema).add(schema.getAttribute("line"), line).build()
    })
  }

  override def open(): Unit = {
    schema = desc.inferSchema()
    reader = new BufferedReader(new FileReader(desc.filePath.get))
    rows = reader.lines().iterator().asScala.slice(startOffset, endOffset)
  }

  override def close(): Unit = reader.close()
}
