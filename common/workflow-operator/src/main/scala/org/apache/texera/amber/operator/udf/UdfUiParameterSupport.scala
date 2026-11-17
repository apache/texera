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

package org.apache.texera.amber.operator.udf

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.executor.OpExecWithCode
import org.apache.texera.amber.core.storage.{FileResolver, RepositoryMountManager}
import org.apache.texera.amber.core.workflow.ExecutionTimeBinding

/** Shared serialized UI-parameter property and execution-time resource binding. */
trait UdfUiParameterSupport {
  @JsonProperty
  @JsonSchemaTitle("Parameters")
  @JsonPropertyDescription(
    "Parameters inferred from active UiParameter declarations in the UDF script"
  )
  var uiParameters: List[UiUDFParameter] = List()

  protected def injectParameterValues(code: String, parameters: List[UiUDFParameter]): String
  protected def uiParameterLanguage: String
  protected def mounts: RepositoryMountManager = RepositoryMountManager

  protected final def injectUiParameters(code: String): String = {
    resourceParameters(uiParameters).foreach(selectedVersionPath)
    injectParameterValues(code, uiParameters)
  }

  protected final def executionBinding(code: String): Option[ExecutionTimeBinding] =
    Option.when(resourceParameters(uiParameters).nonEmpty) {
      // Snapshot the compiled descriptor; resolving versions stays lazy until execution.
      val parameters = uiParameters
      val language = uiParameterLanguage
      val mountManager = mounts
      new ExecutionTimeBinding {
        private lazy val resolved = {
          val bound = parameters.map { parameter =>
            if (!isResource(parameter)) (parameter, None)
            else {
              val (repositoryName, commitHash) =
                FileResolver.resolveRepositoryVersion(selectedVersionPath(parameter))
              val locator = s"$repositoryName:$commitHash"
              val mounted = new UiUDFParameter
              mounted.attribute = parameter.attribute
              mounted.inputType = parameter.inputType
              mounted.value = mountManager.mountPointOf(locator).toString
              (mounted, Some(locator))
            }
          }
          (bound.map(_._1), bound.flatMap(_._2).toSet)
        }

        override lazy val opExecInitInfo: OpExecWithCode =
          OpExecWithCode(injectParameterValues(code, resolved._1), language)

        override def mountLocators: Set[String] = resolved._2
      }
    }

  private def isResource(parameter: UiUDFParameter): Boolean =
    parameter != null && UiUdfParameterContract.resourceInputTypes.values.exists(
      _ == Option(parameter.inputType).getOrElse("")
    )

  private def resourceParameters(parameters: List[UiUDFParameter]): List[UiUDFParameter] =
    Option(parameters).getOrElse(Nil).filter(isResource)

  private def selectedVersionPath(parameter: UiUDFParameter): String =
    Option(parameter.value).map(_.trim).filter(_.nonEmpty).getOrElse {
      throw new RuntimeException(
        s"No ${parameter.inputType} selected for the parameter " +
          s"'${Option(parameter.attribute).map(_.getName).getOrElse("")}'."
      )
    }
}
