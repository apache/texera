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

package org.apache.texera.amber.operator.statistics

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.fasterxml.jackson.databind.node.ObjectNode
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.metadata.{
  JsonSchemaCustomizer,
  OperatorGroupConstants,
  OperatorInfo
}
import org.apache.texera.amber.operator.metadata.annotations.{
  AutofillAttributeName,
  AutofillAttributeNameList
}
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext
import org.apache.texera.amber.util.JSONUtils.objectMapper

@JsonSchemaInject(json = """{
  "attributeTypeRules": {
    "target": {"enum": ["integer", "long", "double"]},
    "predictors": {"enum": ["integer", "long", "double"]}
  }
}""")
class OLSAnalysisOpDesc extends PythonOperatorDescriptor with JsonSchemaCustomizer {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Response")
  @JsonPropertyDescription("Numeric outcome to explain; not a predictor.")
  @AutofillAttributeName
  var target: String = ""

  @JsonProperty(required = true)
  @JsonSchemaTitle("Predictors")
  @JsonPropertyDescription(
    "Unique numeric predictors, in coefficient output order. No automatic categorical encoding."
  )
  @AutofillAttributeNameList
  var predictors: List[String] = List.empty

  @JsonProperty(defaultValue = "true")
  @JsonSchemaTitle("Include Intercept")
  @JsonPropertyDescription("Include a constant term. Without it, R-squared is uncentered.")
  var includeIntercept: java.lang.Boolean = true

  @JsonProperty(defaultValue = "omit")
  @JsonSchemaTitle("Missing Values")
  @JsonPropertyDescription(
    "Omit rows missing the response or a selected predictor, or fail. Other columns do not affect the fit. Counts are reported."
  )
  @JsonSchemaInject(json = """{"enum": ["omit", "error"]}""")
  var missingValues: String = "omit"

  override def customizeJsonSchema(schema: ObjectNode): Unit = {
    // Collection-field JsonSchemaInject is applied to the item schema; these
    // constraints belong to the array itself.
    schema
      .get("properties")
      .get("predictors")
      .asInstanceOf[ObjectNode]
      .put("minItems", 1)
      .put("uniqueItems", true)
  }

  private def validateConfiguration(): Unit = {
    require(target != null && target.nonEmpty, "Choose a numeric response column")
    require(predictors != null && predictors.nonEmpty, "Choose at least one numeric predictor")
    require(predictors.forall(p => p != null && p.nonEmpty), "Every predictor must name a column")
    require(predictors.distinct.size == predictors.size, "Predictors must be unique")
    require(!predictors.contains(target), "The response cannot also be a predictor")
    require(includeIntercept != null, "Choose whether to include an intercept")
    require(
      Set("omit", "error").contains(missingValues),
      "Unknown missing-values policy; choose omit or error"
    )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "OLS Analysis",
      "Fit unweighted ordinary least squares on the full table (held in memory). " +
        "One row per coefficient, with classical standard errors, two-sided t-tests and 95% confidence intervals; " +
        "model statistics and row counts repeat on each row. Null means a non-finite/undefined statistic. " +
        "Rejects rank-deficient predictors instead of silently dropping aliased terms. No train/test split.",
      OperatorGroupConstants.MACHINE_LEARNING_GENERAL_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(blocking = true))
    )

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    validateConfiguration()
    val schema = inputSchemas.getOrElse(
      operatorInfo.inputPorts.head.id,
      throw new IllegalArgumentException("OLS requires an input schema")
    )
    (target :: predictors).foreach { name =>
      require(schema.containsAttribute(name), s"OLS column '$name' is not in the input table")
      require(
        Set(AttributeType.INTEGER, AttributeType.LONG, AttributeType.DOUBLE)
          .contains(schema.getAttribute(name).getType),
        s"OLS column '$name' must be numeric (integer, long or double)"
      )
    }
    val doubles = List("estimate", "std_error", "t_statistic", "p_value", "ci_lower", "ci_upper")
    val counts = List("n_input", "n_used", "n_omitted", "df_model", "df_residual")
    val model = List("residual_std_error", "r_squared", "adj_r_squared", "f_statistic", "f_p_value")
    Map(
      operatorInfo.outputPorts.head.id -> Schema(
        List(
          new Attribute("term", AttributeType.STRING),
          new Attribute("is_intercept", AttributeType.BOOLEAN)
        ) ++
          doubles.map(new Attribute(_, AttributeType.DOUBLE)) ++
          counts.map(new Attribute(_, AttributeType.LONG)) ++
          model.map(new Attribute(_, AttributeType.DOUBLE))
      )
    )
  }

  override def generatePythonCode(): String = {
    // A half-filled UI configuration must still generate syntactically valid
    // Python. Schema propagation rejects it, and this encoded error protects
    // callers that execute generated code without propagating a schema first.
    val configurationError: EncodableString =
      try {
        validateConfiguration()
        ""
      } catch {
        case ex: IllegalArgumentException => ex.getMessage
      }
    // Encode all user-controlled names as data, never as Python source/formulas.
    val config: EncodableString = objectMapper.writeValueAsString(
      Map(
        "target" -> target,
        "predictors" -> predictors,
        "intercept" -> includeIntercept,
        "missing" -> missingValues
      )
    )
    pyb"""
       |import json
       |import numpy as np
       |from pandas.api.types import is_numeric_dtype, is_bool_dtype, is_complex_dtype
       |from statsmodels.regression.linear_model import OLS
       |from pytexera import *
       |
       |class ProcessTableOperator(UDFTableOperator):
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        configuration_error = $configurationError
       |        if configuration_error:
       |            raise ValueError(configuration_error)
       |        config = json.loads($config)
       |        target = config["target"]
       |        predictors = config["predictors"]
       |        intercept = config["intercept"]
       |        columns = [target] + predictors
       |        n_input = len(table)
       |        if n_input == 0:
       |            raise ValueError("No complete rows available for OLS")
       |        if not table.columns.is_unique:
       |            raise ValueError("OLS input column names must be unique")
       |        for column in columns:
       |            if column not in table.columns:
       |                raise ValueError(f"OLS column {column!r} is absent")
       |        complete = table[columns].dropna()
       |        n_used = len(complete)
       |        n_omitted = n_input - n_used
       |        if config["missing"] == "error" and n_omitted:
       |            raise ValueError(f"OLS found {n_omitted} rows with missing selected values")
       |        if n_used == 0:
       |            raise ValueError("No complete rows available for OLS")
       |        for column in columns:
       |            dtype = complete[column].dtype
       |            if not is_numeric_dtype(dtype) or is_bool_dtype(dtype) or is_complex_dtype(dtype):
       |                raise ValueError(f"OLS column {column!r} must contain real numeric values")
       |        values = complete.to_numpy(dtype=float)
       |        if not np.isfinite(values).all():
       |            raise ValueError("OLS requires finite values in retained rows")
       |        y = values[:, 0]
       |        x = values[:, 1:]
       |        terms = list(predictors)
       |        if intercept:
       |            x = np.column_stack([np.ones(n_used), x])
       |            terms.insert(0, "(Intercept)")
       |        if n_used <= x.shape[1]:
       |            raise ValueError("OLS needs more complete rows than fitted coefficients for inference")
       |        if np.linalg.matrix_rank(x) != x.shape[1]:
       |            raise ValueError("OLS design is rank-deficient; remove redundant or constant predictors")
       |        fit = OLS(y, x, missing="raise", hasconst=intercept).fit(use_t=True)
       |        if not np.isfinite(fit.params).all():
       |            raise ValueError("OLS could not compute finite coefficients; check data scaling")
       |
       |        def finite(value):
       |            return float(value) if np.isfinite(value) else None
       |
       |        # Constant outcomes/perfect fits can have undefined inference. Preserve
       |        # their finite coefficients; emit null rather than NaN/Infinity.
       |        with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
       |            standard_errors = fit.bse
       |            t_values = fit.tvalues
       |            p_values = fit.pvalues
       |            intervals = fit.conf_int(alpha=0.05)
       |            total_ss = fit.centered_tss if intercept else fit.uncentered_tss
       |            summary = {
       |                "n_input": n_input,
       |                "n_used": n_used,
       |                "n_omitted": n_omitted,
       |                "df_model": int(fit.df_model),
       |                "df_residual": int(fit.df_resid),
       |                "residual_std_error": finite(np.sqrt(fit.mse_resid)),
       |                "r_squared": finite(fit.rsquared) if total_ss > 0 else None,
       |                "adj_r_squared": finite(fit.rsquared_adj) if total_ss > 0 else None,
       |                "f_statistic": finite(fit.fvalue) if total_ss > 0 else None,
       |                "f_p_value": finite(fit.f_pvalue) if total_ss > 0 else None,
       |            }
       |        for index, term in enumerate(terms):
       |            yield {
       |                "term": term,
       |                "is_intercept": bool(intercept and index == 0),
       |                "estimate": finite(fit.params[index]),
       |                "std_error": finite(standard_errors[index]),
       |                "t_statistic": finite(t_values[index]),
       |                "p_value": finite(p_values[index]),
       |                "ci_lower": finite(intervals[index, 0]),
       |                "ci_upper": finite(intervals[index, 1]),
       |                **summary,
       |            }
       |""".encode
  }
}
