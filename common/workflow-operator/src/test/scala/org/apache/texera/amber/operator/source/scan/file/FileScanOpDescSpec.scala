/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.operator.source.scan.file

import org.apache.texera.amber.core.tuple.{
  Attribute,
  AttributeType,
  Schema,
  SchemaEnforceable,
  Tuple
}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.TestOperators
import org.apache.texera.amber.operator.source.scan.{FileAttributeType, FileDecodingMethod}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class FileScanOpDescSpec extends AnyFlatSpec with BeforeAndAfter {

  private val inputSchema = new Schema(new Attribute("filename", AttributeType.STRING))

  var fileScanOpDesc: FileScanOpDesc = _

  before {
    fileScanOpDesc = new FileScanOpDesc()
    fileScanOpDesc.fileEncoding = FileDecodingMethod.UTF_8
  }

  it should "infer schema with single column representing each line of text" in {
    val inferredSchema: Schema = fileScanOpDesc.sourceSchema()

    assert(inferredSchema.getAttributes.length == 1)
    assert(inferredSchema.getAttribute("line").getType == AttributeType.STRING)
  }

  it should "read first 5 lines from the input file path tuple into output tuples" in {
    fileScanOpDesc.attributeType = FileAttributeType.STRING
    fileScanOpDesc.fileScanLimit = Option(5)

    val inputTuple = Tuple(inputSchema, Array[Any](TestOperators.TestTextFilePath))
    val fileScanOpExec =
      new FileScanOpExec(objectMapper.writeValueAsString(fileScanOpDesc))

    fileScanOpExec.open()
    val processedTuple: Iterator[Tuple] = fileScanOpExec
      .processTuple(inputTuple, 0)
      .map(tupleLike =>
        tupleLike
          .asInstanceOf[SchemaEnforceable]
          .enforceSchema(fileScanOpDesc.sourceSchema())
      )

    assert(processedTuple.next().getField("line").equals("line1"))
    assert(processedTuple.next().getField("line").equals("line2"))
    assert(processedTuple.next().getField("line").equals("line3"))
    assert(processedTuple.next().getField("line").equals("line4"))
    assert(processedTuple.next().getField("line").equals("line5"))
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("line"))
    fileScanOpExec.close()
  }

  it should "read the lines after a 5-line offset from the input file path tuple when no limit is set" in {
    fileScanOpDesc.attributeType = FileAttributeType.STRING
    fileScanOpDesc.fileScanOffset = Option(5)

    val inputTuple = Tuple(inputSchema, Array[Any](TestOperators.TestTextFilePath))
    val fileScanOpExec =
      new FileScanOpExec(objectMapper.writeValueAsString(fileScanOpDesc))

    fileScanOpExec.open()
    val processedTuple: Iterator[Tuple] = fileScanOpExec
      .processTuple(inputTuple, 0)
      .map(tupleLike =>
        tupleLike
          .asInstanceOf[SchemaEnforceable]
          .enforceSchema(fileScanOpDesc.sourceSchema())
      )

    assert(processedTuple.next().getField("line").equals("line6"))
    assert(processedTuple.next().getField("line").equals("line7"))
    assert(processedTuple.next().getField("line").equals("line8"))
    assert(processedTuple.next().getField("line").equals("line9"))
    assert(processedTuple.next().getField("line").equals("line10"))
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("line"))
    fileScanOpExec.close()
  }

  it should "preserve the original input filename when include filename is enabled" in {
    fileScanOpDesc.attributeType = FileAttributeType.SINGLE_STRING
    fileScanOpDesc.outputFileName = true

    val inputFilePath = TestOperators.TestTextFilePath
    val inputTuple = Tuple(inputSchema, Array[Any](inputFilePath))
    val fileScanOpExec =
      new FileScanOpExec(objectMapper.writeValueAsString(fileScanOpDesc))

    fileScanOpExec.open()
    val outputSchema = fileScanOpDesc.sourceSchema()
    val processedTuple = fileScanOpExec
      .processTuple(inputTuple, 0)
      .next()
      .asInstanceOf[SchemaEnforceable]
      .enforceSchema(outputSchema)

    assert(processedTuple.getField[String]("filename") == inputFilePath)
    fileScanOpExec.close()
  }

  "FileScanOpDesc.getPhysicalOp" should
    "wire the FileScanOpExec class with one input port and one output port" in {
    val physical = fileScanOpDesc.getPhysicalOp(WorkflowIdentity(1L), ExecutionIdentity(1L))
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, payload) =>
        assert(className == classOf[FileScanOpExec].getName)
        assert(payload.nonEmpty)
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    assert(physical.inputPorts.size == 1)
    assert(physical.outputPorts.size == 1)
  }

  it should "propagate sourceSchema to its single output port" in {
    val physical = fileScanOpDesc.getPhysicalOp(WorkflowIdentity(1L), ExecutionIdentity(1L))
    val outPortId = fileScanOpDesc.operatorInfo.outputPorts.head.id
    val out = physical.propagateSchema.func(Map.empty)
    assert(out(outPortId) == fileScanOpDesc.sourceSchema())
  }

  "FileScanOpDesc.generateStandaloneCode" should
    "read every line as text with the configured encoding by default" in {
    assert(
      fileScanOpDesc.generateStandaloneCode() ==
        """_rows = []
          |for _fn in in1df.iloc[:, 0]:
          |    with open(_fn, "r", encoding="utf-8") as _f:
          |        _rows.extend(l.rstrip("\n") for l in _f)
          |out1df = pd.DataFrame({"line": _rows})""".stripMargin
    )
  }

  // The enum name is "US_ASCII", not a Python codec name.
  it should "render the encoding as a Python codec name" in {
    fileScanOpDesc.fileEncoding = FileDecodingMethod.ASCII
    assert(fileScanOpDesc.generateStandaloneCode().contains("""encoding="us-ascii""""))
  }

  it should "read the whole file in single-value mode, keeping the filename only when asked" in {
    fileScanOpDesc.attributeType = FileAttributeType.SINGLE_STRING

    fileScanOpDesc.outputFileName = true
    val withName = fileScanOpDesc.generateStandaloneCode()
    assert(withName.contains("        _rows.append((_fn, _f.read()))"))
    assert(withName.endsWith("""out1df = pd.DataFrame(_rows, columns=["filename", "line"])"""))

    fileScanOpDesc.outputFileName = false
    val withoutName = fileScanOpDesc.generateStandaloneCode()
    assert(withoutName.contains("        _rows.append(_f.read())"))
    assert(withoutName.endsWith("""out1df = pd.DataFrame({"line": _rows})"""))

    // The platform's line-by-line branch (FileScanUtils.createTuplesFromFile)
    // emits only the value, so line mode must drop the filename column too.
    fileScanOpDesc.attributeType = FileAttributeType.STRING
    fileScanOpDesc.outputFileName = true
    assert(!fileScanOpDesc.generateStandaloneCode().contains("filename"))
  }

  it should "open binary attribute types in binary mode" in {
    Seq(FileAttributeType.BINARY, FileAttributeType.LARGE_BINARY).foreach { attrType =>
      fileScanOpDesc.attributeType = attrType
      val code = fileScanOpDesc.generateStandaloneCode()
      assert(code.contains("""    with open(_fn, "rb") as _f:"""))
      assert(!code.contains("encoding="))
      assert(code.contains("        _rows.append(_f.read())"))
    }
  }

  it should "cast each line to the configured attribute type" in {
    val castByType = Seq(
      FileAttributeType.INTEGER -> "int(l.rstrip())",
      FileAttributeType.LONG -> "int(l.rstrip())",
      FileAttributeType.DOUBLE -> "float(l.rstrip())",
      FileAttributeType.BOOLEAN -> """l.rstrip().lower() == "true"""",
      FileAttributeType.TIMESTAMP -> "pd.Timestamp(l.rstrip())",
      FileAttributeType.STRING -> """l.rstrip("\n")"""
    )
    castByType.foreach {
      case (attrType, cast) =>
        fileScanOpDesc.attributeType = attrType
        assert(
          fileScanOpDesc
            .generateStandaloneCode()
            .contains(s"        _rows.extend($cast for l in _f)")
        )
    }
  }

  it should "materialize the lines and slice them when a limit or offset is set" in {
    fileScanOpDesc.attributeType = FileAttributeType.INTEGER

    fileScanOpDesc.fileScanOffset = Option(3)
    fileScanOpDesc.fileScanLimit = None
    assert(fileScanOpDesc.generateStandaloneCode().contains("    _rows.extend(_lines[3:])"))

    fileScanOpDesc.fileScanOffset = None
    fileScanOpDesc.fileScanLimit = Option(5)
    assert(fileScanOpDesc.generateStandaloneCode().contains("    _rows.extend(_lines[0:5])"))

    fileScanOpDesc.fileScanOffset = Option(3)
    fileScanOpDesc.fileScanLimit = Option(5)
    val code = fileScanOpDesc.generateStandaloneCode()
    assert(code.contains("        _lines = [int(l.rstrip()) for l in _f]"))
    assert(code.contains("    _rows.extend(_lines[3:8])"))
  }

  it should "warn that archive extraction is unsupported when extract is on" in {
    // `extract` is a val, so it can only be set through deserialization.
    val desc = objectMapper.readValue(
      """{"operatorType":"FileScanOp","extract":true}""",
      classOf[FileScanOpDesc]
    )
    assert(desc.generateStandaloneCode().startsWith("# WARNING: extract=true is not supported"))
  }
}
