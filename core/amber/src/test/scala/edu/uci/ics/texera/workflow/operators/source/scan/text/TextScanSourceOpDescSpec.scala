package edu.uci.ics.texera.workflow.operators.source.scan.text

import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, Schema}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`

class TextScanSourceOpDescSpec extends AnyFlatSpec with BeforeAndAfter {
  var textScanSourceOpDesc: TextScanSourceOpDesc = _

  val TestTextFilePath: String = "src/test/resources/line_numbers.txt"
  val StartOffset: Int = 0
  val EndOffset: Int = 5

  before {
    textScanSourceOpDesc = new TextScanSourceOpDesc()
    textScanSourceOpDesc.filePath = Some(TestTextFilePath)
  }

  it should "always infer schema with single column representing each line of text" in {
    val inferredSchema: Schema = textScanSourceOpDesc.inferSchema()

    assert(inferredSchema.getAttributes.length == 1)
    assert(inferredSchema.getAttribute("line").getType == AttributeType.STRING)
  }

  it should "read first 5 lines of the input text file into corresponding output tuples" in {
    val outputAsSingleTuple: Boolean = false
    val textScanSourceOpExec =
      new TextScanSourceOpExec(textScanSourceOpDesc, StartOffset, EndOffset, outputAsSingleTuple)
    textScanSourceOpExec.open()
    val processedTuple: Iterator[Tuple] = textScanSourceOpExec.produceTexeraTuple()

    assert(processedTuple.next().getField("line").equals("line1"))
    assert(processedTuple.next().getField("line").equals("line2"))
    assert(processedTuple.next().getField("line").equals("line3"))
    assert(processedTuple.next().getField("line").equals("line4"))
    assert(processedTuple.next().getField("line").equals("line5"))
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("line"))
    textScanSourceOpExec.close()
  }

  it should "read first 5 lines of the input text file into a single output tuple" in {
    val outputAsSingleTuple: Boolean = true
    val textScanSourceOpExec =
      new TextScanSourceOpExec(textScanSourceOpDesc, StartOffset, EndOffset, outputAsSingleTuple)
    textScanSourceOpExec.open()
    val processedTuple: Iterator[Tuple] = textScanSourceOpExec.produceTexeraTuple()

    assert(processedTuple.next().getField("line").equals("line1line2line3line4line5"))
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("line"))
    textScanSourceOpExec.close()
  }
}
