package edu.uci.ics.texera.workflow.operators.source.scan.text

import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, Schema}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`

class TextScanSourceOpDescSpec extends AnyFlatSpec with BeforeAndAfter {
  var textScanSourceOpDesc: TextScanSourceOpDesc = _

  val TestTextFilePath: String = "src/test/resources/line_numbers.txt"
  val TestCRLFTextFilePath: String = "src/test/resources/line_numbers_crlf.txt"
  val StartOffset: Int = 0
  val EndOffset: Int = 5

  before {
    textScanSourceOpDesc = new TextScanSourceOpDesc()
    textScanSourceOpDesc.filePath = Some(TestTextFilePath)
  }

  it should "infer schema with single column representing each line of text in normal text scan mode" in {
    val inferredSchema: Schema = textScanSourceOpDesc.inferSchema()

    assert(inferredSchema.getAttributes.length == 1)
    assert(inferredSchema.getAttribute("line").getType == AttributeType.STRING)
  }

  it should "infer schema with single column representing entire file in outputAsSingleTuple mode" in {
    val outputAsSingleTuple: Boolean = true
    textScanSourceOpDesc.outputAsSingleTuple = outputAsSingleTuple
    val inferredSchema: Schema = textScanSourceOpDesc.inferSchema()

    assert(inferredSchema.getAttributes.length == 1)
    assert(inferredSchema.getAttribute("file").getType == AttributeType.STRING)
  }

  it should "infer schema with user-specified output schema attribute" in {
    val outputAsSingleTuple: Boolean = false
    textScanSourceOpDesc.outputAsSingleTuple = outputAsSingleTuple
    val customOutputAttributeName: String = "testing"
    textScanSourceOpDesc.attributeName = Option(customOutputAttributeName)
    val inferredSchema: Schema = textScanSourceOpDesc.inferSchema()

    assert(inferredSchema.getAttributes.length == 1)
    assert(inferredSchema.getAttribute("testing").getType == AttributeType.STRING)
  }

  it should "read first 5 lines of the input text file into corresponding output tuples" in {
    val outputAsSingleTuple: Boolean = false
    textScanSourceOpDesc.outputAsSingleTuple = outputAsSingleTuple
    val textScanSourceOpExec =
      new TextScanSourceOpExec(textScanSourceOpDesc, StartOffset, EndOffset, "line")
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

  it should "read first 5 lines of the input text file with CRLF separators into corresponding output tuples" in {
    textScanSourceOpDesc.filePath = Some(TestCRLFTextFilePath)
    val outputAsSingleTuple: Boolean = false
    textScanSourceOpDesc.outputAsSingleTuple = outputAsSingleTuple
    val textScanSourceOpExec =
      new TextScanSourceOpExec(textScanSourceOpDesc, StartOffset, EndOffset, "line")
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
    textScanSourceOpDesc.outputAsSingleTuple = outputAsSingleTuple
    val textScanSourceOpExec =
      new TextScanSourceOpExec(textScanSourceOpDesc, StartOffset, EndOffset, "file")
    textScanSourceOpExec.open()
    val processedTuple: Iterator[Tuple] = textScanSourceOpExec.produceTexeraTuple()

    assert(
      processedTuple
        .next()
        .getField("file")
        .equals("line1\nline2\nline3\nline4\nline5\nline6\nline7\nline8\nline9\nline10")
    )
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("file"))
    textScanSourceOpExec.close()
  }
}
