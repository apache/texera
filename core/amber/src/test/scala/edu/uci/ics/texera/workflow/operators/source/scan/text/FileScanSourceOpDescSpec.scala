package edu.uci.ics.texera.workflow.operators.source.scan.text

import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, Schema}
import edu.uci.ics.texera.workflow.operators.source.scan.{
  FileAttributeType,
  FileDecodingMethod,
  FileScanSourceOpDesc,
  FileScanSourceOpExec
}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`

class FileScanSourceOpDescSpec extends AnyFlatSpec with BeforeAndAfter {
  val TestTextFilePath: String = "src/test/resources/line_numbers.txt"
  val TestCRLFTextFilePath: String = "src/test/resources/line_numbers_crlf.txt"
  val TestNumbersFilePath: String = "src/test/resources/numbers.txt"
  var fileScanSourceOpDesc: FileScanSourceOpDesc = _

  before {
    fileScanSourceOpDesc = new FileScanSourceOpDesc()
    fileScanSourceOpDesc.filePath = Some(TestTextFilePath)
    fileScanSourceOpDesc.fileEncoding = FileDecodingMethod.UTF_8
  }

  it should "infer schema with single column representing each line of text in normal text scan mode" in {
    val inferredSchema: Schema = fileScanSourceOpDesc.inferSchema()

    assert(inferredSchema.getAttributes.length == 1)
    assert(inferredSchema.getAttribute("line").getType == AttributeType.STRING)
  }

  it should "infer schema with single column representing entire file in outputAsSingleTuple mode" in {
    fileScanSourceOpDesc.attributeType = FileAttributeType.SINGLE_STRING
    val inferredSchema: Schema = fileScanSourceOpDesc.inferSchema()

    assert(inferredSchema.getAttributes.length == 1)
    assert(inferredSchema.getAttribute("line").getType == AttributeType.STRING)
  }

  it should "infer schema with user-specified output schema attribute" in {
    fileScanSourceOpDesc.attributeType = FileAttributeType.STRING
    val customOutputAttributeName: String = "testing"
    fileScanSourceOpDesc.attributeName = customOutputAttributeName
    val inferredSchema: Schema = fileScanSourceOpDesc.inferSchema()

    assert(inferredSchema.getAttributes.length == 1)
    assert(inferredSchema.getAttribute("testing").getType == AttributeType.STRING)
  }

  it should "infer schema with integer attribute type" in {
    fileScanSourceOpDesc.attributeType = FileAttributeType.INTEGER
    val inferredSchema: Schema = fileScanSourceOpDesc.inferSchema()

    assert(inferredSchema.getAttributes.length == 1)
    assert(inferredSchema.getAttribute("line").getType == AttributeType.INTEGER)
  }

  it should "read first 5 lines of the input text file into corresponding output tuples" in {
    fileScanSourceOpDesc.attributeType = FileAttributeType.STRING
    fileScanSourceOpDesc.fileScanLimit = Option(5)
    val FileScanSourceOpExec =
      new FileScanSourceOpExec(fileScanSourceOpDesc)
    FileScanSourceOpExec.open()
    val processedTuple: Iterator[Tuple] = FileScanSourceOpExec.produceTexeraTuple()

    assert(processedTuple.next().getField("line").equals("line1"))
    assert(processedTuple.next().getField("line").equals("line2"))
    assert(processedTuple.next().getField("line").equals("line3"))
    assert(processedTuple.next().getField("line").equals("line4"))
    assert(processedTuple.next().getField("line").equals("line5"))
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("line"))
    FileScanSourceOpExec.close()
  }

  it should "read first 5 lines of the input text file with CRLF separators into corresponding output tuples" in {
    fileScanSourceOpDesc.filePath = Some(TestCRLFTextFilePath)
    fileScanSourceOpDesc.attributeType = FileAttributeType.STRING
    fileScanSourceOpDesc.fileScanLimit = Option(5)
    val FileScanSourceOpExec =
      new FileScanSourceOpExec(fileScanSourceOpDesc)
    FileScanSourceOpExec.open()
    val processedTuple: Iterator[Tuple] = FileScanSourceOpExec.produceTexeraTuple()

    assert(processedTuple.next().getField("line").equals("line1"))
    assert(processedTuple.next().getField("line").equals("line2"))
    assert(processedTuple.next().getField("line").equals("line3"))
    assert(processedTuple.next().getField("line").equals("line4"))
    assert(processedTuple.next().getField("line").equals("line5"))
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("line"))
    FileScanSourceOpExec.close()
  }

  it should "read first 5 lines of the input text file into a single output tuple" in {
    fileScanSourceOpDesc.attributeType = FileAttributeType.SINGLE_STRING
    val FileScanSourceOpExec =
      new FileScanSourceOpExec(fileScanSourceOpDesc)
    FileScanSourceOpExec.open()
    val processedTuple: Iterator[Tuple] = FileScanSourceOpExec.produceTexeraTuple()

    assert(
      processedTuple
        .next()
        .getField("line")
        .equals("line1\nline2\nline3\nline4\nline5\nline6\nline7\nline8\nline9\nline10")
    )
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("line"))
    FileScanSourceOpExec.close()
  }

  it should "read first 5 lines of the input text into corresponding output INTEGER tuples" in {
    fileScanSourceOpDesc.filePath = Some(TestNumbersFilePath)
    fileScanSourceOpDesc.attributeType = FileAttributeType.INTEGER
    fileScanSourceOpDesc.fileScanLimit = Option(5)
    val FileScanSourceOpExec = new FileScanSourceOpExec(fileScanSourceOpDesc)
    FileScanSourceOpExec.open()
    val processedTuple: Iterator[Tuple] = FileScanSourceOpExec.produceTexeraTuple()

    assert(processedTuple.next().getField("line").equals(1))
    assert(processedTuple.next().getField("line").equals(2))
    assert(processedTuple.next().getField("line").equals(3))
    assert(processedTuple.next().getField("line").equals(4))
    assert(processedTuple.next().getField("line").equals(5))
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("line"))
    FileScanSourceOpExec.close()
  }

  it should "read first 5 lines of the input text file with US_ASCII encoding" in {
    fileScanSourceOpDesc.filePath = Some(TestCRLFTextFilePath)
    fileScanSourceOpDesc.fileEncoding = FileDecodingMethod.ASCII
    fileScanSourceOpDesc.attributeType = FileAttributeType.STRING
    fileScanSourceOpDesc.fileScanLimit = Option(5)
    val FileScanSourceOpExec =
      new FileScanSourceOpExec(fileScanSourceOpDesc)
    FileScanSourceOpExec.open()
    val processedTuple: Iterator[Tuple] = FileScanSourceOpExec.produceTexeraTuple()

    assert(processedTuple.next().getField("line").equals("line1"))
    assert(processedTuple.next().getField("line").equals("line2"))
    assert(processedTuple.next().getField("line").equals("line3"))
    assert(processedTuple.next().getField("line").equals("line4"))
    assert(processedTuple.next().getField("line").equals("line5"))
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("line"))
    FileScanSourceOpExec.close()
  }

}
