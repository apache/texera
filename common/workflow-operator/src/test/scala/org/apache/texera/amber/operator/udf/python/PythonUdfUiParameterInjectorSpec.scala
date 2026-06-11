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
package org.apache.texera.amber.operator.udf.python

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PythonUdfUiParameterInjectorSpec extends AnyFlatSpec with Matchers {

  private def uiParameter(
      attributeName: String,
      attributeType: AttributeType,
      value: String
  ): UiUDFParameter = {
    val parameter = new UiUDFParameter
    parameter.attribute = new Attribute(attributeName, attributeType)
    parameter.value = value
    parameter
  }

  private def inject(parameters: UiUDFParameter*): String =
    PythonUdfUiParameterInjector.inject(baseUdfCode, parameters.toList)

  private def inject(code: String, parameters: UiUDFParameter*): String =
    PythonUdfUiParameterInjector.inject(code, parameters.toList)

  private def decoderCallCount(code: String): Int =
    code.sliding("self.decode_python_template".length).count(_ == "self.decode_python_template")

  private val baseUdfCode: String =
    """from pytexera import *
      |
      |class ProcessTupleOperator(UDFOperatorV2):
      |    @overrides
      |    def open(self):
      |        print("open")
      |
      |    @overrides
      |    def process_tuple(self, tuple_: Tuple, port: int):
      |        yield tuple_
      |""".stripMargin

  it should "return user code unchanged when there are no UI parameters" in {
    val injectedCode = inject()

    injectedCode should include("class ProcessTupleOperator(UDFOperatorV2):")
    injectedCode should include("""print("open")""")
    injectedCode should not include ("_texera_injected_ui_parameters")
    injectedCode should not include ("self.decode_python_template")
    injectedCode should not include ("import typing")
  }

  it should "return unsupported user code unchanged when there are no UI parameters" in {
    val nonSupportedCode =
      """from pytexera import *
        |
        |class SomethingElse:
        |    def open(self):
        |        pass
        |""".stripMargin

    inject(nonSupportedCode) shouldBe nonSupportedCode
  }

  it should "preserve user source lines that look like Scala stripMargin input" in {
    val udfCodeWithPipeLine =
      """from pytexera import *
        |
        |class ProcessTupleOperator(UDFOperatorV2):
        |    def open(self):
        |        pattern = "keep"
        |        text = '''
        |    |do not strip this line
        |'''
        |
        |    def process_tuple(self, tuple_: Tuple, port: int):
        |        yield tuple_
        |""".stripMargin

    val injectedCode = inject(udfCodeWithPipeLine, uiParameter("k", AttributeType.STRING, "v"))

    injectedCode should include("    |do not strip this line")
    injectedCode should include("def _texera_injected_ui_parameters(self) -> Dict[str, Any]:")
  }

  it should "inject UI parameter hook into supported UDF class using Dict and Any from pytexera" in {
    val injectedCode = inject(uiParameter("date", AttributeType.TIMESTAMP, "2024-01-01T00:00:00Z"))

    injectedCode should include("class ProcessTupleOperator(UDFOperatorV2):")
    injectedCode should include(
      "# Follow-up runtime support exports Dict/Any and defines the base hook that @overrides targets."
    )
    injectedCode should include("def _texera_injected_ui_parameters(self) -> Dict[str, Any]:")
    injectedCode should include("return {")
    injectedCode should include("self.decode_python_template")
    decoderCallCount(injectedCode) shouldBe 2
    injectedCode should include("""print("open")""")
    injectedCode should not include ("import typing")
    injectedCode should not include ("typing.Dict")
    injectedCode should not include ("typing.Any")
  }

  it should "append the reserved hook inside the class before the next top-level statement" in {
    val udfCodeWithSiblingDefinition =
      """from pytexera import *
        |
        |class ProcessTupleOperator(UDFOperatorV2):
        |    @overrides
        |    def open(self):
        |        print("open")
        |
        |    @overrides
        |    def process_tuple(self, tuple_: Tuple, port: int):
        |        yield tuple_
        |
        |def helper():
        |    return "outside"
        |""".stripMargin

    val injectedCode =
      inject(udfCodeWithSiblingDefinition, uiParameter("k", AttributeType.STRING, "v"))

    val hookIndex = injectedCode.indexOf("def _texera_injected_ui_parameters(self)")
    val processTupleIndex =
      injectedCode.indexOf("def process_tuple(self, tuple_: Tuple, port: int):")
    val helperIndex = injectedCode.indexOf("def helper():")

    hookIndex should be >= 0
    processTupleIndex should be < hookIndex
    helperIndex should be > hookIndex
  }

  it should "append the reserved hook after triple-quoted strings that contain top-level-looking lines" in {
    val udfCodeWithTripleQuotedString =
      """from pytexera import *
        |
        |class ProcessTupleOperator(UDFOperatorV2):
        |    def process_tuple(self, tuple_: Tuple, port: int):
        |        sql = '''
        |SELECT * FROM t
        |'''
        |        yield tuple_
        |
        |def helper():
        |    return "outside"
        |""".stripMargin

    val injectedCode =
      inject(udfCodeWithTripleQuotedString, uiParameter("k", AttributeType.STRING, "v"))

    val hookIndex = injectedCode.indexOf("def _texera_injected_ui_parameters(self)")
    val stringEndIndex = injectedCode.indexOf("'''\n        yield tuple_")
    val helperIndex = injectedCode.indexOf("def helper():")

    stringEndIndex should be >= 0
    stringEndIndex should be < hookIndex
    hookIndex should be < helperIndex
  }

  it should "preserve multiple UI parameters in the injected map" in {
    val injectedCode = inject(
      uiParameter("param1", AttributeType.DOUBLE, "12.5"),
      uiParameter("param2", AttributeType.INTEGER, "1"),
      uiParameter("param3", AttributeType.STRING, "Hola"),
      uiParameter("param4", AttributeType.TIMESTAMP, "2026-02-28T03:15:00Z")
    )

    injectedCode should include("def _texera_injected_ui_parameters(self) -> Dict[str, Any]:")
    injectedCode should include("self.decode_python_template")
    decoderCallCount(injectedCode) shouldBe 8
    injectedCode should not include ("import typing")
  }

  it should "throw when a parameter attribute is missing" in {
    val invalidParameter = new UiUDFParameter
    invalidParameter.attribute = null
    invalidParameter.value = "anything"

    val exception = the[RuntimeException] thrownBy {
      inject(invalidParameter)
    }

    exception.getMessage should include("UiParameter attribute is required")
  }

  it should "throw when a UI parameter name is duplicated" in {
    val exception = the[RuntimeException] thrownBy {
      inject(
        uiParameter("date", AttributeType.STRING, "2024-01-01"),
        uiParameter("date", AttributeType.TIMESTAMP, "2024-01-01T00:00:00Z")
      )
    }

    exception.getMessage should include("UiParameter name 'date' is declared more than once")
  }

  Seq(AttributeType.BINARY, AttributeType.LARGE_BINARY).foreach { unsupportedType =>
    it should s"throw when a UI parameter uses ${unsupportedType.name()} type" in {
      val exception = the[RuntimeException] thrownBy {
        inject(uiParameter("payload", unsupportedType, "68656c6c6f"))
      }

      exception.getMessage should include(
        s"UiParameter type '${unsupportedType.name()}' is not supported"
      )
    }
  }

  it should "throw when the reserved hook is already defined by the user" in {
    val udfWithReservedHook =
      """from pytexera import *
        |
        |class ProcessTupleOperator(UDFOperatorV2):
        |    def _texera_injected_ui_parameters(self):
        |        return {}
        |
        |    def open(self):
        |        pass
        |""".stripMargin

    val exception = the[RuntimeException] thrownBy {
      inject(udfWithReservedHook, uiParameter("k", AttributeType.STRING, "v"))
    }

    exception.getMessage should include(
      "Reserved method '_texera_injected_ui_parameters' is already defined"
    )
  }

  it should "throw when UI parameters are provided but no supported user class is present" in {
    val nonSupportedCode =
      """from pytexera import *
        |
        |class SomethingElse:
        |    def open(self):
        |        pass
        |""".stripMargin

    val exception = the[RuntimeException] thrownBy {
      inject(nonSupportedCode, uiParameter("k", AttributeType.STRING, "v"))
    }

    exception.getMessage should include("no supported Python UDF class was found")
  }
}
