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

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.executor.{OpExecInitInfo, OpExecWithCode}
import org.apache.texera.amber.core.storage.{FileResolver, RepositoryMountManager}
import org.apache.texera.amber.core.workflow.ExecutionTimeBinding
import org.apache.texera.amber.operator.udf.python.PythonUdfUiParameterSupport._

/** Shared serialized UI-parameter property and code-injection hook for Python UDF descriptors. */
trait PythonUdfUiParameterSupport {

  @JsonProperty
  @JsonSchemaTitle("Parameters")
  @JsonPropertyDescription(
    "Parameters inferred from active self.UiParameter(...) calls in the Python script"
  )
  var uiParameters: List[UiUDFParameter] = List()

  /**
    * The code to compile against. A parameter naming a resource keeps the version path the user
    * chose; the execution-time binding replaces it with the mount path before any worker sees
    * it, so editing a workflow never pays to resolve one.
    */
  protected final def injectUiParameters(code: String): String = {
    resourceParameters(uiParameters).foreach(selectedVersionPath)
    PythonUdfUiParameterInjector.inject(code, uiParameters)
  }

  /** None when no parameter names a resource: the compiled code is then the code to run. */
  protected final def executionBinding(code: String): Option[ExecutionTimeBinding] =
    Option.when(resourceParameters(uiParameters).nonEmpty)(
      new ResourceParameterBinding(code, uiParameters)
    )
}

object PythonUdfUiParameterSupport {

  // The kinds of resource a parameter's value can name; keep in sync with pytexera's Resource.
  private val ResourceInputTypes: Set[String] = Set("model", "dataset")

  private def isResource(parameter: UiUDFParameter): Boolean =
    parameter != null && ResourceInputTypes.contains(Option(parameter.inputType).getOrElse(""))

  private def resourceParameters(parameters: List[UiUDFParameter]): List[UiUDFParameter] =
    Option(parameters).getOrElse(Nil).filter(isResource)

  // Checked on every compile, so an unchosen version shows in the editor rather than at run time.
  private def selectedVersionPath(parameter: UiUDFParameter): String =
    Option(parameter.value)
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse(
        throw new RuntimeException(
          s"No ${parameter.inputType} selected for the parameter " +
            s"'${Option(parameter.attribute).map(_.getName).getOrElse("")}'."
        )
      )

  /**
    * Resolves each resource parameter to the directory its version is mounted at, yielding both
    * the code the workers run and the repositories behind those directories. Memoized, since
    * resolving costs a database round trip per parameter.
    */
  private class ResourceParameterBinding(code: String, parameters: List[UiUDFParameter])
      extends ExecutionTimeBinding {

    private lazy val resolved: (List[UiUDFParameter], Set[String]) = {
      val bound = parameters.map { parameter =>
        if (!isResource(parameter)) (parameter, None)
        else {
          val (repositoryName, commitHash) =
            FileResolver.resolveRepositoryVersion(selectedVersionPath(parameter))
          val locator = s"$repositoryName:$commitHash"
          val mounted = new UiUDFParameter
          mounted.attribute = parameter.attribute
          mounted.inputType = parameter.inputType
          mounted.value = RepositoryMountManager.mountPointOf(locator).toString
          (mounted, Some(locator))
        }
      }
      (bound.map(_._1), bound.flatMap(_._2).toSet)
    }

    override lazy val opExecInitInfo: OpExecInitInfo =
      OpExecWithCode(PythonUdfUiParameterInjector.inject(code, resolved._1), "python")

    override def mountLocators: Set[String] = resolved._2
  }
}
