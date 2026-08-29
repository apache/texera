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

package org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.base

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.KNNTrainer.SklearnAdvancedKNNClassifierTrainerOpDesc
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.SVCTrainer.SklearnAdvancedSVCTrainerOpDesc
import org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.SVRTrainer.SklearnAdvancedSVRTrainerOpDesc
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorMetadataGenerator}
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.IteratorHasAsScala

class SklearnAdvancedBaseDescSpec extends AnyFlatSpec with Matchers {

  // A minimal ParamClass: the generated code only ever consults getName/getType, the other
  // two being read by the schema rather than by code generation.
  private class TestParam(name: String, typ: String) extends ParamClass {
    override def getName: String = name
    override def getType: String = typ
    override def getSampleValue: String = ""
    override def getAllowedValues: Array[String] = Array.empty
  }

  // A concrete descriptor supplying just the two abstract hooks; everything
  // under test (operatorInfo, getOutputSchemas, generatePythonCode, getParameter)
  // lives in the abstract base.
  private class TestSklearnMLOp extends SklearnMLOperatorDescriptor[TestParam] {
    override def getImportStatements: String =
      "from sklearn.neighbors import KNeighborsClassifier"
    override def getOperatorInfo: String = "KNN Classifier"
  }

  private def hyperParam(
      name: String,
      typ: String,
      fromWorkflow: Boolean,
      attribute: String = "",
      value: String = ""
  ): HyperParameters[TestParam] = {
    val hp = new HyperParameters[TestParam]
    hp.parameter = new TestParam(name, typ)
    hp.parametersSource = fromWorkflow
    hp.attribute = attribute
    hp.value = value
    hp
  }

  private def newOp(
      paraList: List[HyperParameters[TestParam]],
      features: List[EncodableString] = List("f1", "f2"),
      groundTruth: EncodableString = "label"
  ): TestSklearnMLOp = {
    val d = new TestSklearnMLOp
    d.paraList = paraList
    d.selectedFeatures = features
    d.groundTruthAttribute = groundTruth
    d
  }

  "SklearnMLOperatorDescriptor.operatorInfo" should
    "advertise a training/parameter 2-in 1-out shape with a port dependency" in {
    val info = new TestSklearnMLOp().operatorInfo
    info.userFriendlyName shouldBe "KNN Classifier"
    info.operatorDescription shouldBe "Sklearn KNN Classifier Operator"
    info.operatorGroupName shouldBe OperatorGroupConstants.ADVANCED_SKLEARN_GROUP
    info.inputPorts.map(_.displayName) shouldBe List("training", "parameter")
    info.inputPorts(1).dependencies shouldBe List(info.inputPorts.head.id)
    info.outputPorts should have length 1
  }

  "SklearnMLOperatorDescriptor.getOutputSchemas" should
    "produce a Model:BINARY, Parameters:STRING schema on the output port" in {
    val d = newOp(List(hyperParam("n_neighbors", "int", fromWorkflow = false, value = "5")))
    val schema = d.getOutputSchemas(Map.empty)(d.operatorInfo.outputPorts.head.id)
    schema.getAttributeNames shouldBe List("Model", "Parameters")
    schema.getAttribute("Model").getType shouldBe AttributeType.BINARY
    schema.getAttribute("Parameters").getType shouldBe AttributeType.STRING
  }

  "SklearnMLOperatorDescriptor.generatePythonCode" should
    "emit the ProcessTableOperator skeleton driven by the model import statement" in {
    val d = newOp(List(hyperParam("n_neighbors", "int", fromWorkflow = false, value = "5")))
    val code = d.generatePythonCode()
    code should include("from pytexera import *")
    code should include("from sklearn.neighbors import KNeighborsClassifier")
    code should include("class ProcessTableOperator(UDFTableOperator):")
    code should include("def process_table(")
    // trainingName is the last token of the import statement.
    code should include("model = KNeighborsClassifier(")
    code should include("model.fit(X_train, y_train)")
    code should include("yield df")
  }

  // This family reads a named list of features rather than every column, so the
  // drop names those columns: a blank anywhere else must not cost the row.
  it should "drop rows missing a selected feature or the ground truth" in {
    val d = newOp(List(hyperParam("n_neighbors", "int", fromWorkflow = false, value = "5")))
    d.generatePythonCode() should include("self.dataset.dropna(subset=features + [")
  }

  it should "loop once when no parameter is sourced from the workflow" in {
    val d = newOp(List(hyperParam("n_neighbors", "int", fromWorkflow = false, value = "5")))
    val code = d.generatePythonCode()
    code should include("loop_times = 1")
    code should not include ".values.shape[0]"
  }

  it should "loop over the sourced column length when a parameter comes from the workflow" in {
    val d = newOp(List(hyperParam("n_neighbors", "int", fromWorkflow = true, attribute = "k_col")))
    val code = d.generatePythonCode()
    code should include("loop_times = table[")
    code should include(".values.shape[0]")
  }

  "SklearnMLOperatorDescriptor.getParameter" should
    "build a constant assignment for a value-sourced parameter" in {
    val d = new TestSklearnMLOp
    val paraList = List(hyperParam("n_neighbors", "int", fromWorkflow = false, value = "5"))
    val paramString = d.getParameter(paraList)(1).encode
    paramString.filterNot(_.isWhitespace) should include("n_neighbors=int(")
    // The value is user input, so it is emitted as a safe decode expression.
    paramString should include("decode_python_template")
    paramString should not include ".values[i]"
  }

  it should "read from the table column for a workflow-sourced parameter" in {
    val d = new TestSklearnMLOp
    val paraList = List(hyperParam("n_neighbors", "int", fromWorkflow = true, attribute = "k_col"))
    val paramString = d.getParameter(paraList)(1).encode
    paramString should include("n_neighbors = int(table[")
    paramString should include(".values[i]")
  }

  // The rules are built from the enum bound to the descriptor's type argument, so they need a
  // real operator rather than the stub above, whose TestParam is a class and has no constants.
  private def valueRulesOf(opClass: Class[_ <: org.apache.texera.amber.operator.LogicalOp]) = {
    val schema = OperatorMetadataGenerator.generateOperatorJsonSchema(opClass)
    val row = schema
      .path("definitions")
      .path(
        schema
          .path("properties")
          .path("paraList")
          .path("items")
          .path("$ref")
          .asText()
          .stripPrefix("#/definitions/")
      )
    row.path("properties").path("value").path("valueRules").path("allOf")
  }

  private def ruleFor(rules: JsonNode, parameter: String): JsonNode =
    rules
      .elements()
      .asScala
      .find(
        _.path("if")
          .path("parameter")
          .path("valEnum")
          .elements()
          .asScala
          .exists(_.asText() == parameter)
      )
      .map(_.path("then"))
      .getOrElse(fail(s"no rule for $parameter"))

  "SklearnMLOperatorDescriptor.customizeJsonSchema" should
    "offer a chosen-from-a-set parameter exactly the values scikit-learn accepts" in {
    val kernel = ruleFor(valueRulesOf(classOf[SklearnAdvancedSVCTrainerOpDesc]), "kernel")
    // scikit-learn's own default leads, so a reader taking the first takes that one
    kernel.path("enum").elements().asScala.map(_.asText()).toSeq shouldBe
      Seq("rbf", "linear", "poly", "sigmoid", "precomputed")
    // an accepted set replaces a type rather than joining it: the values are the constraint
    kernel.has("type") shouldBe false
    // and it replaces an example too, having already named every value worth offering
    kernel.has("examples") shouldBe false
  }

  it should "read a parameter with no set of its own from the converter it names" in {
    val rules = valueRulesOf(classOf[SklearnAdvancedSVCTrainerOpDesc])
    ruleFor(rules, "C").path("type").asText() shouldBe "number"
    ruleFor(rules, "degree").path("type").asText() shouldBe "integer"
  }

  it should "carry the estimator's bound under whichever name matches its range" in {
    val rules = valueRulesOf(classOf[SklearnAdvancedSVCTrainerOpDesc])
    // C's range is open at zero and degree's is closed, so the two take different names
    ruleFor(rules, "C").path("exclusiveMinimum").asDouble() shouldBe 0.0
    ruleFor(rules, "C").has("minimum") shouldBe false
    ruleFor(rules, "degree").path("minimum").asDouble() shouldBe 0.0
    ruleFor(rules, "degree").has("exclusiveMinimum") shouldBe false
    // coef0 is bounded by nothing, so it gets neither rather than a made-up zero
    ruleFor(rules, "coef0").has("minimum") shouldBe false
    ruleFor(rules, "coef0").has("exclusiveMinimum") shouldBe false
  }

  it should "keep a sentinel value reachable when it lies below the useful range" in {
    // SVR's max_iter uses -1 for no limit, so the bound has to admit it
    val rules = valueRulesOf(classOf[SklearnAdvancedSVRTrainerOpDesc])
    ruleFor(rules, "max_iter").path("minimum").asDouble() shouldBe -1.0
  }

  it should "describe a parameter choosing between a set and a number with a pattern" in {
    // gamma takes either of two words or a number, which no type names, so the rule carries a
    // pattern in place of one. Its example is the estimator's own default, a word.
    val gamma = ruleFor(valueRulesOf(classOf[SklearnAdvancedSVCTrainerOpDesc]), "gamma")
    gamma.has("type") shouldBe false
    gamma.path("examples").path(0).asText() shouldBe "scale"
    val pattern = gamma.path("pattern").asText()
    Seq("scale", "auto", "0.1", "1e-3", " 1 ").foreach(v => v.matches(pattern) shouldBe true)
    Seq("abc", "", "scaleauto", "1.2.3").foreach(v => v.matches(pattern) shouldBe false)
  }

  it should "name the parameter as the config spells it, not as the estimator does" in {
    // SVR's `shrinking` is offered by a constant named `probability`, and a chosen parameter
    // reaches the config as the constant. A condition naming the keyword instead would hold
    // for nothing, leaving the value it constrains free.
    val shrinking = ruleFor(valueRulesOf(classOf[SklearnAdvancedSVRTrainerOpDesc]), "probability")
    shrinking.path("enum").elements().asScala.map(_.asText()).toSeq shouldBe Seq("true", "false")
  }

  it should "state a rule for every parameter whose converter says anything about it" in {
    val rules = valueRulesOf(classOf[SklearnAdvancedKNNClassifierTrainerOpDesc])
    val covered = rules
      .elements()
      .asScala
      .flatMap(_.path("if").path("parameter").path("valEnum").elements().asScala)
      .map(_.asText())
      .toSet
    // The rules follow the converter each parameter names, not what scikit-learn goes on to
    // accept: metric is declared int and so is constrained to whole numbers, even though the
    // metrics themselves are words. metric_params names str, which says nothing, so it is the
    // one parameter left free.
    covered shouldBe Set("n_neighbors", "p", "weights", "algorithm", "leaf_size", "metric")
  }

  it should "leave a schema it cannot find the row in alone" in {
    // The three shapes that make the rules unwritable. None arises from a schema the generator
    // produced, so the point is that each is passed over rather than thrown on.
    val svc = new SklearnAdvancedSVCTrainerOpDesc
    Seq(
      objectMapper.createObjectNode(),
      objectMapper.readTree("""{"properties": {"paraList": {"items": {}}}}"""),
      objectMapper.readTree(
        """{"properties": {"paraList": {"items": {"$ref": "#/definitions/Row"}}},
           "definitions": {"Row": {"properties": {"value": "not an object"}}}}"""
      )
    ).foreach { schema =>
      val node = schema.asInstanceOf[ObjectNode]
      noException should be thrownBy svc.customizeJsonSchema(node)
      node.findValue("valueRules") shouldBe null
    }
  }

  it should "write no rules for a descriptor whose type argument is not an enum" in {
    // TestParam is a class standing in for one, so there are no constants to read. A real
    // operator always binds an enum; this is the path a test stub takes.
    val schema = objectMapper
      .readTree(
        """{"properties": {"paraList": {"items": {"$ref": "#/definitions/Row"}}},
           "definitions": {"Row": {"properties": {"value": {"type": "string"}}}}}"""
      )
      .asInstanceOf[ObjectNode]
    new TestSklearnMLOp().customizeJsonSchema(schema)
    schema.findValue("valueRules") shouldBe null
  }

  "HyperParameters" should "require whichever of the two inputs the row actually uses" in {
    val schema =
      OperatorMetadataGenerator.generateOperatorJsonSchema(classOf[SklearnAdvancedSVCTrainerOpDesc])
    val row = schema
      .path("definitions")
      .path(
        schema
          .path("properties")
          .path("paraList")
          .path("items")
          .path("$ref")
          .asText()
          .stripPrefix("#/definitions/")
      )
    val branch = row.path("allOf").path(0)
    branch
      .path("if")
      .path("properties")
      .path("parametersSource")
      .path("const")
      .asBoolean() shouldBe true
    branch.path("then").path("required").path(0).asText() shouldBe "attribute"
    branch.path("else").path("required").path(0).asText() shouldBe "value"
    // neither may be required outright: the hide rules show only one of them at a time
    row
      .path("required")
      .elements()
      .asScala
      .map(_.asText())
      .toSeq should contain noneOf ("value", "attribute")
  }
}
