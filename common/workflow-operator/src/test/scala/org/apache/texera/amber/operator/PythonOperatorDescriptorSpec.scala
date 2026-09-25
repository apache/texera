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
import org.apache.texera.amber.operator.visualization.histogram2d.Histogram2DOpDesc
import org.apache.texera.amber.operator.visualization.radarChart.RadarChartOpDesc
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

  it should "not bind a boolean that picks the shape of the code, and say why" in {
    // As the frontend sends it: the Boolean `parametersSource` holds "$k", which the parse turns
    // into the placeholder false and records. Generated with true, the row reads its parameter
    // from the second input; with false, from its `value`: no one place in the code is the value.
    // The String `value` keeps its "$k" text, which only the compiler adds to the sidecar.
    val op = objectMapper
      .readValue(
        """{"operatorType":"SVCTrainer","groundTruthAttribute":"y","Selected Features":["x"],
          |"paraList":[{"parameter":"C","value":"$k","attribute":"p","parametersSource":"$k"}]}
          |""".stripMargin,
        classOf[LogicalOp]
      )
      .asInstanceOf[SklearnAdvancedSVCTrainerOpDesc]
    op.stateReferences shouldBe Map("/paraList/0/parametersSource" -> "k")
    op.paraList.head.parametersSource shouldBe false

    val binding = op.loopVariableBinding

    binding.unbound shouldBe Map(
      "/paraList/0/parametersSource" -> "its code is generated from the value before the loop runs"
    )
    code(op) shouldBe binding.code
    code(op) should not include "loop_variable_value"
    code(op) should not include "loop_variable_text"
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

  // -------------------- numeric and boolean loop-variable references --------------------

  private def valueLookup(name: String, kind: String): String =
    s"self.loop_variable_value('$name', '$kind')"

  /**
    * A Histogram2D as the frontend sends it, its bins given as JSON (`"\"$n\""` refers to `n`),
    * and `more` properties after them.
    */
  private def histogram2d(xBins: String, yBins: String, more: String = ""): Histogram2DOpDesc =
    objectMapper
      .readValue(
        s"""{"operatorType":"Histogram2D","xColumn":"x","yColumn":"y",
           |"xBins":$xBins,"yBins":$yBins$more}""".stripMargin,
        classOf[LogicalOp]
      )
      .asInstanceOf[Histogram2DOpDesc]

  private val checkedValue = "it checks or adjusts the value before the loop runs"

  "PythonOperatorDescriptor.getPhysicalOp" should
    "not bind the integers Histogram2D checks before the loop runs, and say why" in {
    // Its "X Bins must be > 0" passes for the probes, which land in the code as they are. It
    // rejects 0 and -1, though: read from the loop state, a 0 would reach plotly unchecked.
    val op = histogram2d("\"$n\"", "\"$m\"")
    op.stateReferences shouldBe Map("/xBins" -> "n", "/yBins" -> "m")

    val binding = op.loopVariableBinding

    binding.unbound shouldBe Map("/xBins" -> checkedValue, "/yBins" -> checkedValue)
    code(op) shouldBe binding.code
    code(op) should not include "loop_variable_value"
  }

  it should "not bind the values HuggingFace clamps before the loop runs" in {
    // Its code holds max(1, min(tokens, 4096)) and the temperature clamped to [0, 2]: the probes
    // land in it as they are, but 0 and 1000000 do not, and a lookup would skip the clamp.
    val op = objectMapper
      .readValue(
        """{"operatorType":"HuggingFace","hfApiToken":"t","promptColumn":"p",
          |"maxNewTokens":"$n","temperature":"$t"}""".stripMargin,
        classOf[LogicalOp]
      )
      .asInstanceOf[PythonOperatorDescriptor]
    op.stateReferences shouldBe Map("/maxNewTokens" -> "n", "/temperature" -> "t")

    op.loopVariableBinding.unbound shouldBe
      Map("/maxNewTokens" -> checkedValue, "/temperature" -> checkedValue)
    code(op) should not include "loop_variable_value"
  }

  it should "embed why the code generation failed when it fails for every probe value alike" in {
    // Y Bins is 0, which Histogram2D rejects whatever $n is: that is the error its code embeds,
    // for the code-generation check to report, and no reference is said to be unbound.
    val op = histogram2d("\"$n\"", "0")

    op.loopVariableBinding.unbound shouldBe empty
    code(op) shouldBe
      "#EXCEPTION DURING CODE GENERATION: assertion failed: Y Bins must be > 0, but got 0"
  }

  it should "generate the code from the properties the descriptor holds, null ones too" in {
    // A null Normalization fails the generation. Its copies must fail alike, not generate the
    // code from the field's default, 'density', which the descriptor does not hold.
    val literal = code(histogram2d("4", "4", ""","normalize":null"""))
    literal should startWith("#EXCEPTION DURING CODE GENERATION")

    val op = histogram2d("\"$n\"", "4", ""","normalize":null""")

    code(op) shouldBe literal
    op.loopVariableBinding.unbound shouldBe empty
  }

  it should "generate the code from the properties themselves when the sidecar is empty" in {
    // Outside every loop block: exactly the code and the schema of the plain values.
    val op = histogram2d("4", "6")
    op.stateReferences shouldBe empty

    val physical = op.getPhysicalOp(workflowId, executionId)

    code(op) shouldBe op.generatePythonCode()
    code(op) should include("nbinsx=4,")
    code(op) should not include "loop_variable_value"
    op.loopVariableBinding.unbound shouldBe empty
    physical.propagateSchema.func(Map.empty) shouldBe op.getOutputSchemas(Map.empty)
  }

  /** A Radar Chart as the frontend sends it, its opacity given as JSON. */
  private def radarChart(fillOpacity: String, nameColumn: String = "name"): RadarChartOpDesc =
    objectMapper
      .readValue(
        s"""{"operatorType":"RadarChart","nameColumn":"$nameColumn","valueColumns":["a","b"],
           |"fillOpacity":$fillOpacity}""".stripMargin,
        classOf[LogicalOp]
      )
      .asInstanceOf[RadarChartOpDesc]

  it should "read a floating-point reference as a number" in {
    // Radar Chart writes its opacity into the code as it is, for every value: no check to skip.
    val op = radarChart("\"$r\"")
    op.stateReferences shouldBe Map("/fillOpacity" -> "r")
    val plain = radarChart("0.4").generatePythonCode()

    code(op) shouldBe plain.replace("opacity=0.4\n", s"opacity=${valueLookup("r", "number")}\n")
    op.loopVariableBinding.unbound shouldBe empty
  }

  it should "read a typed and a text reference together" in {
    val op = radarChart("\"$r\"", nameColumn = "$col")
    op.stateReferences = op.stateReferences + ("/nameColumn" -> "col")
    val plain = radarChart("0.4", nameColumn = "$col").generatePythonCode()

    code(op) shouldBe plain
      .replace("opacity=0.4\n", s"opacity=${valueLookup("r", "number")}\n")
      .replace(decoderExpression("$col"), lookup("col"))
    code(op) should include(lookup("col"))
    op.loopVariableBinding.unbound shouldBe empty
  }

  "PythonOperatorDescriptor.loopVariableValueLookup" should
    "be the call pyamber's Operator.loop_variable_value answers, naming the variable and its kind" in {
    PythonOperatorDescriptor.loopVariableValueLookup("n", "integer") shouldBe
      "self.loop_variable_value('n', 'integer')"
    PythonOperatorDescriptor.loopVariableValueLookup("_r2", "number") shouldBe
      "self.loop_variable_value('_r2', 'number')"
    PythonOperatorDescriptor.loopVariableValueLookup("flag", "boolean") shouldBe
      valueLookup("flag", "boolean")
  }
}
