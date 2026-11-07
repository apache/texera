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

package org.apache.texera.amber.operator

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.texera.amber.core.executor.OpExecWithCode
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{
  InputPort,
  OutputPort,
  PortIdentity,
  PreferCoordinator
}
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.SVCTrainer.{
  SklearnAdvancedSVCParameters,
  SklearnAdvancedSVCTrainerOpDesc
}
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.base.HyperParameters
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.decoderExpression
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PythonOperatorDescriptorSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private class OneToOnePyOp extends PythonOperatorDescriptor {
    override def generatePythonCode(): String = "print('hi')"
    override def getOutputSchemas(in: Map[PortIdentity, Schema]): Map[PortIdentity, Schema] =
      Map.empty
    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        "t",
        "d",
        OperatorGroupConstants.PYTHON_GROUP,
        List(InputPort()),
        List(OutputPort())
      )
  }

  private class SourcePyOp extends PythonOperatorDescriptor {
    override def asSource(): Boolean = true
    override def parallelizable(): Boolean = true
    override def generatePythonCode(): String = "yield None"
    override def getOutputSchemas(in: Map[PortIdentity, Schema]): Map[PortIdentity, Schema] =
      Map.empty
    override def operatorInfo: OperatorInfo =
      OperatorInfo("s", "d", OperatorGroupConstants.PYTHON_GROUP, List(), List(OutputPort()))
  }

  private class ThrowingPyOp extends PythonOperatorDescriptor {
    override def generatePythonCode(): String = throw new RuntimeException("boom")
    override def getOutputSchemas(in: Map[PortIdentity, Schema]): Map[PortIdentity, Schema] =
      Map.empty
    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        "x",
        "d",
        OperatorGroupConstants.PYTHON_GROUP,
        List(InputPort()),
        List(OutputPort())
      )
  }

  private def code(op: PythonOperatorDescriptor): String =
    op.getPhysicalOp(workflowId, executionId).opExecInitInfo match {
      case OpExecWithCode(c, _) => c
      case other                => fail(s"expected OpExecWithCode, got $other")
    }

  "PythonOperatorDescriptor.getPhysicalOp" should
    "embed the generation exception in the code instead of throwing" in {
    code(new ThrowingPyOp) shouldBe "#EXCEPTION DURING CODE GENERATION: boom"
  }

  it should "build a source PhysicalOp when asSource is true" in {
    val physical = (new SourcePyOp).getPhysicalOp(workflowId, executionId)
    physical.locationPreference shouldBe Some(PreferCoordinator)
    physical.opExecInitInfo match {
      case OpExecWithCode(c, language) =>
        c shouldBe "yield None"
        language shouldBe "python"
      case other => fail(s"expected OpExecWithCode, got $other")
    }
    physical.isSourceOperator shouldBe true
  }

  it should "build a one-to-one PhysicalOp when asSource is false" in {
    val physical = (new OneToOnePyOp).getPhysicalOp(workflowId, executionId)
    physical.locationPreference shouldBe None
    physical.opExecInitInfo match {
      case OpExecWithCode(c, _) => c shouldBe "print('hi')"
      case other                => fail(s"expected OpExecWithCode, got $other")
    }
  }

  // -------------------- loop-variable references --------------------

  // A real generated operator: the SVC trainer renders each hyperparameter row's `value` (an
  // Encodable string) through pyb's decoder, twice per row (the fit and the Parameters column).
  private def svcTrainer(
      rows: (SklearnAdvancedSVCParameters, String)*
  ): SklearnAdvancedSVCTrainerOpDesc = {
    val op = new SklearnAdvancedSVCTrainerOpDesc
    op.paraList = rows.map {
      case (parameter, value) =>
        val row = new HyperParameters[SklearnAdvancedSVCParameters]
        row.parameter = parameter
        row.value = value
        row
    }.toList
    op.selectedFeatures = List("x")
    op.groundTruthAttribute = "标签"
    op
  }

  private def lookup(name: String): String = s"self.loop_variable_text('$name')"

  private def occurrences(code: String, fragment: String): Int =
    code.sliding(fragment.length).count(_ == fragment)

  "PythonOperatorDescriptor.getPhysicalOp" should
    "read a text '$i' reference from the iteration's state instead of decoding the literal" in {
    val op = svcTrainer(SklearnAdvancedSVCParameters.C -> "$i")
    val generated = op.generatePythonCode()
    op.stateReferences = Map("/paraList/0/value" -> "i")

    val bound = code(op)

    bound should include(lookup("i"))
    bound should not include decoderExpression("$i")
    // Every decoded "$i" becomes the lookup, and nothing else in the code changes.
    occurrences(bound, lookup("i")) shouldBe occurrences(generated, decoderExpression("$i"))
    occurrences(bound, lookup("i")) shouldBe 2
    bound shouldBe generated.replace(decoderExpression("$i"), lookup("i"))
  }

  it should "leave the code as generated when the sidecar is empty, as outside every loop block" in {
    val op = svcTrainer(SklearnAdvancedSVCParameters.C -> "$i")

    val unbound = code(op)

    unbound shouldBe op.generatePythonCode()
    unbound should include(decoderExpression("$i"))
    unbound should not include "loop_variable_text"
  }

  it should "leave every text the sidecar does not name as it decodes, '$'-prefixed or not" in {
    val op = svcTrainer(
      SklearnAdvancedSVCParameters.C -> "$i",
      SklearnAdvancedSVCParameters.kernel -> "rbf",
      SklearnAdvancedSVCParameters.gamma -> "$j",
      SklearnAdvancedSVCParameters.coef0 -> "$index"
    )
    op.stateReferences = Map("/paraList/0/value" -> "i")

    val bound = code(op)

    bound should include(lookup("i"))
    Seq("rbf", "$j", "$index", "x", "标签").foreach(text =>
      bound should include(decoderExpression(text))
    )
    bound should not include lookup("j")
    bound should not include lookup("index")
  }

  it should "bind two different references, and every property that refers to the same one" in {
    val op = svcTrainer(
      SklearnAdvancedSVCParameters.C -> "$c",
      SklearnAdvancedSVCParameters.kernel -> "$k",
      SklearnAdvancedSVCParameters.gamma -> "$c"
    )
    val generated = op.generatePythonCode()
    op.stateReferences =
      Map("/paraList/0/value" -> "c", "/paraList/1/value" -> "k", "/paraList/2/value" -> "c")

    val bound = code(op)

    occurrences(bound, lookup("c")) shouldBe 4
    occurrences(bound, lookup("k")) shouldBe 2
    bound should not include decoderExpression("$c")
    bound should not include decoderExpression("$k")
    bound shouldBe generated
      .replace(decoderExpression("$c"), lookup("c"))
      .replace(decoderExpression("$k"), lookup("k"))
  }

  it should "not rewrite a typed placeholder, even where its name's text is decoded elsewhere" in {
    // As the frontend sends it: the Boolean `parametersSource` holds "$k", which the parse turns
    // into the placeholder false and records. The String `value` keeps its "$k" text, which only
    // the compiler adds to the sidecar, and only inside a loop block.
    val op = objectMapper
      .readValue(
        """{"operatorType":"SVCTrainer","groundTruthAttribute":"y","Selected Features":["x"],
          |"paraList":[{"parameter":"C","value":"$k","parametersSource":"$k"}]}""".stripMargin,
        classOf[LogicalOp]
      )
      .asInstanceOf[SklearnAdvancedSVCTrainerOpDesc]
    op.stateReferences shouldBe Map("/paraList/0/parametersSource" -> "k")
    op.paraList.head.parametersSource shouldBe false

    val unbound = code(op)

    unbound shouldBe op.generatePythonCode()
    unbound should include(decoderExpression("$k"))
    unbound should not include "loop_variable_text"
  }

  it should "go by the text at each pointer, not by the names the sidecar lists" in {
    // Neither entry points at a "$i": one at "rbf", one at no property. The "$i" the operator
    // does hold is not named by the sidecar, so it stays the decoded literal.
    val op = svcTrainer(
      SklearnAdvancedSVCParameters.kernel -> "rbf",
      SklearnAdvancedSVCParameters.C -> "$i"
    )
    op.stateReferences = Map("/paraList/0/value" -> "i", "/paraList/9/value" -> "i")

    code(op) shouldBe op.generatePythonCode()
    code(op) should include(decoderExpression("$i"))
  }

  // Its generation fails with a message that holds the very expression its `label` renders as.
  private class FailingWithLabelPyOp extends PythonOperatorDescriptor {
    @JsonProperty var label: String = "$i"
    override def generatePythonCode(): String =
      throw new RuntimeException(s"cannot use ${decoderExpression(label)}")
    override def getOutputSchemas(in: Map[PortIdentity, Schema]): Map[PortIdentity, Schema] =
      Map.empty
    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        "f",
        "d",
        OperatorGroupConstants.PYTHON_GROUP,
        List(InputPort()),
        List(OutputPort())
      )
  }

  it should "leave the code of a failed generation as the error it embeds" in {
    // Were the error's code rewritten too, the lookup would take the expression's place in it.
    val op = new FailingWithLabelPyOp
    op.stateReferences = Map("/label" -> "i")

    code(op) shouldBe s"#EXCEPTION DURING CODE GENERATION: cannot use ${decoderExpression("$i")}"
    code(op) should not include "loop_variable_text"
  }

  "PythonOperatorDescriptor.loopVariableLookup" should
    "be the call pyamber's Operator.loop_variable_text answers, naming the variable without its '$'" in {
    PythonOperatorDescriptor.loopVariableLookup("i") shouldBe lookup("i")
    PythonOperatorDescriptor.loopVariableLookup("_Top_2") shouldBe
      "self.loop_variable_text('_Top_2')"
  }
}
