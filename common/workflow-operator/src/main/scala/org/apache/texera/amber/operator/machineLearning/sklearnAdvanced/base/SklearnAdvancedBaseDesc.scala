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

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.metadata.annotations.{
  AutofillAttributeName,
  AutofillAttributeNameList
}
import org.apache.texera.amber.operator.metadata.{
  JsonSchemaCustomizer,
  OperatorGroupConstants,
  OperatorInfo
}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder
import org.apache.texera.amber.util.JSONUtils.objectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import java.lang.reflect.{ParameterizedType, Type}
import scala.util.Try

/**
  * One hyperparameter a trainer offers. `getName` is the keyword argument passed to the
  * estimator and `getType` the callable converting the user's text, so together they already
  * decide which values the estimator can be given. The two below state that so the form can
  * hold the user to it, rather than leaving it to be discovered when `fit` raises.
  */
trait ParamClass {
  def getName: String

  def getType: String

  /** One value this parameter accepts, offered as an example rather than imposed as a
    * default: what the estimator itself documents as its default is the obvious choice, but
    * nothing here decides a hyperparameter on the user's behalf. Empty for a parameter with an
    * accepted set, which already names every value worth offering, and empty where even the
    * estimator's own default is a value `getType` cannot convert, since an example the
    * operator would then reject is worse than none.
    */
  def getSampleValue: String

  /** The values the estimator accepts, where it accepts a fixed set rather than a range, the
    * estimator's own default first. Empty for a parameter taking any number, which its
    * converter already constrains.
    */
  def getAllowedValues: Array[String]

  /** The shape the value takes, for a parameter that accepts neither a fixed set nor a plain
    * number but a choice between them. Empty for every parameter one of the other two
    * describes, which is most of them.
    */
  def getPattern: String = ""

  /** How low the value may go, written the way the estimator's own range reads: `">0"` where
    * zero itself is refused and `">=1"` where the bound is included. Empty for a parameter
    * bounded by nothing, which a number alone cannot be told from one whose bound nobody
    * looked up.
    */
  def getMinimum: String = ""
}

abstract class SklearnMLOperatorDescriptor[T <: ParamClass]
    extends PythonOperatorDescriptor
    with JsonSchemaCustomizer {
  @JsonIgnore
  def getImportStatements: String

  @JsonIgnore
  def getOperatorInfo: String

  @JsonProperty(required = true)
  @JsonSchemaTitle("Parameter Setting")
  var paraList: List[HyperParameters[T]] = List()

  @JsonProperty(required = true)
  @JsonSchemaTitle("Ground Truth Attribute Column")
  @JsonPropertyDescription("Ground truth attribute column")
  @AutofillAttributeName
  var groundTruthAttribute: EncodableString = ""

  @JsonProperty(value = "Selected Features", required = true)
  @JsonSchemaTitle("Selected Features")
  @JsonPropertyDescription("Features used to train the model")
  @AutofillAttributeNameList
  var selectedFeatures: List[EncodableString] = _

  /**
    * State what a `paraList` row's `value` may hold, which depends on the `parameter` chosen
    * beside it and so cannot be annotated on a field shared by every parameter.
    *
    * The rules go under a key of Texera's own rather than as a JSON-Schema `allOf`, for the
    * same reason `attributeTypeRules` does, and whose grammar they borrow: the form builder
    * merges the members of an `allOf` into a single field, which would leave one control
    * carrying every parameter's constraints at once.
    */
  override def customizeJsonSchema(schema: ObjectNode): Unit = {
    val rowSchema = hyperParameterRowSchema(schema)
    if (rowSchema == null) return
    val value = rowSchema.path("properties").path("value")
    if (!value.isObject) return

    val branches = objectMapper.createArrayNode()
    paramConstants.foreach { param =>
      val condition = objectMapper.createObjectNode()
      condition
        .putObject("parameter")
        .putArray("valEnum")
        .add(chosenValueOf(param))

      val outcome = objectMapper.createObjectNode()
      if (param.getAllowedValues.nonEmpty) {
        // The accepted set says everything: it names each value a reader could pick and each
        // value a sweep should try, so an example alongside it would only repeat one of them.
        param.getAllowedValues.foreach(outcome.withArray("enum").add)
      } else {
        // A pattern is what a parameter offering a choice between a set and a number has
        // instead, so it stands in for the type rather than joining it.
        if (param.getPattern.nonEmpty) outcome.put("pattern", param.getPattern)
        else
          valueTypeOf(param).foreach { valueType =>
            outcome.put("type", valueType)
            // A bound belongs to a value read as a number, so it rides with the type rather
            // than standing on its own.
            addMinimum(param.getMinimum, outcome)
          }
        if (param.getSampleValue.nonEmpty) outcome.withArray("examples").add(param.getSampleValue)
      }

      if (!outcome.isEmpty) {
        val branch = objectMapper.createObjectNode()
        branch.set[ObjectNode]("if", condition)
        branch.set[ObjectNode]("then", outcome)
        branches.add(branch)
      }
    }
    if (!branches.isEmpty)
      value
        .asInstanceOf[ObjectNode]
        .putObject("valueRules")
        .set[ObjectNode]("allOf", branches)
  }

  /** What a chosen `parameter` holds in the config, which a rule's condition has to name to
    * hold: the enum constant, since that is what Jackson writes and what the form compares
    * against. Not `getName`, the keyword the emitted Python passes on, which SVR's `shrinking`
    * spells differently from the constant offering it.
    */
  private def chosenValueOf(param: ParamClass): String =
    param match {
      case constant: Enum[_] => constant.name
      case _                 => param.getName
    }

  /** Puts a declared bound under the JSON Schema name for it, `>` and `>=` being the two forms
    * an estimator's range takes at the low end. A bound spelled any other way is skipped
    * rather than guessed at, since a wrong one turns away values that work.
    */
  private def addMinimum(bound: String, outcome: ObjectNode): Unit =
    if (bound.startsWith(">=")) numberOf(bound.drop(2)).foreach(outcome.put("minimum", _))
    else if (bound.startsWith(">"))
      numberOf(bound.drop(1)).foreach(outcome.put("exclusiveMinimum", _))

  private def numberOf(text: String): Option[Double] = Try(text.trim.toDouble).toOption

  /** How the form should read a value with no fixed set of its own: from the callable the
    * parameter names, since that is what the emitted code puts the text through. A parameter
    * converted by anything else is left unconstrained rather than guessed at.
    */
  private def valueTypeOf(param: ParamClass): Option[String] =
    param.getType match {
      case "int"              => Some("integer")
      case "float" | "double" => Some("number")
      case _                  => None
    }

  /** The `HyperParameters` definition this operator's `paraList` points at. Followed by its
    * `$ref` rather than by name, which carries the parameter enum and so differs per operator.
    */
  private def hyperParameterRowSchema(schema: ObjectNode): ObjectNode = {
    val ref = schema.path("properties").path("paraList").path("items").path("$ref").asText("")
    val name = ref.stripPrefix("#/definitions/")
    if (name.isEmpty) return null
    schema.path("definitions").path(name) match {
      case row: ObjectNode => row
      case _               => null
    }
  }

  /** The hyperparameters this operator offers, from the enum bound to `T`. The field itself
    * cannot say: erasure leaves `paraList` holding a plain `HyperParameters`, so the binding
    * survives only on the generic supertype. Empty where `T` is not an enum, which is only
    * ever a test stub standing in for one.
    */
  private def paramConstants: Seq[ParamClass] = {
    var t: Type = getClass.getGenericSuperclass
    while (t != null) t match {
      case p: ParameterizedType =>
        val raw = p.getRawType.asInstanceOf[Class[_]]
        if (raw == classOf[SklearnMLOperatorDescriptor[_]])
          return p.getActualTypeArguments()(0) match {
            case bound: Class[_] =>
              val constants = bound.getEnumConstants.asInstanceOf[Array[AnyRef]]
              if (constants == null) Seq.empty
              else constants.toSeq.map(_.asInstanceOf[ParamClass])
            case _ => Seq.empty
          }
        t = raw.getGenericSuperclass
      case c: Class[_] => t = c.getGenericSuperclass
      case _           => t = null
    }
    Seq.empty
  }

  private def getLoopTimes(paraList: List[HyperParameters[T]]): PythonTemplateBuilder = {
    for (ele <- paraList) {
      if (ele.parametersSource) {
        return pyb"""table[${ele.attribute}].values.shape[0]"""
      }
    }
    pyb"1"
  }

  def getParameter(paraList: List[HyperParameters[T]]): List[PythonTemplateBuilder] = {
    var workflowParam = s"";
    var portParam = pyb"";
    var paramString = pyb""
    for (ele <- paraList) {
      if (ele.parametersSource) {
        workflowParam = s"$workflowParam${ele.parameter.getName} = {},"
        portParam =
          portParam + pyb"${ele.parameter.getType}(table[${ele.attribute}].values[i]),"
        paramString =
          pyb"$paramString${ele.parameter.getName} = ${ele.parameter.getType}(table[${ele.attribute}].values[i]),"
      } else {
        workflowParam = s"$workflowParam${ele.parameter.getName} = {},"
        portParam = pyb"$portParam${ele.parameter.getType} (${ele.value}),"
        paramString =
          pyb"$paramString${ele.parameter.getName} = ${ele.parameter.getType} (${ele.value}),"
      }
    }
    List(pyb""""$workflowParam".format($portParam)""", paramString)

  }

  override def generatePythonCode(): String = {
    val listFeatures = selectedFeatures.map(feature => pyb"""$feature""").mkString(",")
    val trainingName = getImportStatements.split(" ").last
    val stringList = getParameter(paraList)
    val trainingParam = stringList(1)
    val paramString = stringList(0)
    val finalCode =
      pyb"""
         |from pytexera import *
         |
         |import pandas as pd
         |${getImportStatements}
         |
         |class ProcessTableOperator(UDFTableOperator):
         |
         |  @overrides
         |  def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |    model_list = []
         |    para_list = []
         |    features = [$listFeatures]
         |
         |    if port == 0:
         |      self.dataset = table
         |
         |    if port == 1 :
         |      rows_read = len(self.dataset)
         |      dataset = self.dataset.dropna(subset=features + [$groundTruthAttribute]) #remove missing values
         |      if len(dataset) < rows_read:
         |        print("Skipped", rows_read - len(dataset), "of", rows_read, "rows with missing values")
         |      y_train = dataset[$groundTruthAttribute]
         |      X_train = dataset[features]
         |      loop_times = ${getLoopTimes(paraList)}
         |
         |      for i in range(loop_times):
         |        model = ${trainingName}(${trainingParam})
         |        model.fit(X_train, y_train)
         |        model_list.append(model)
         |        para_str = ${paramString}
         |        para_list.append(para_str)
         |
         |      data = dict()
         |      data["Model"]= model_list
         |      data["Parameters"] =para_list
         |
         |      df = pd.DataFrame(data)
         |      yield df
         |
         |"""
    finalCode.encode
  }

  override def operatorInfo: OperatorInfo = {
    val name = getOperatorInfo
    OperatorInfo(
      name,
      "Sklearn " + name + " Operator",
      OperatorGroupConstants.ADVANCED_SKLEARN_GROUP,
      inputPorts = List(
        InputPort(
          PortIdentity(0),
          displayName = "training"
        ),
        InputPort(
          PortIdentity(1),
          displayName = "parameter",
          dependencies = List(PortIdentity(0))
        )
      ),
      outputPorts = List(OutputPort())
    )
  }

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema(
      List(
        new Attribute("Model", AttributeType.BINARY),
        new Attribute("Parameters", AttributeType.STRING)
      )
    )

    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }
}
