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

package org.apache.texera.service.resource

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import com.typesafe.scalalogging.LazyLogging
import jakarta.annotation.security.RolesAllowed
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.{Consumes, POST, Path, Produces}
import org.apache.texera.common.compiler.model.{LogicalPlan, LogicalPlanPojo}
import org.apache.texera.amber.translator.WorkflowToPythonTranslator

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type"
)
@JsonSubTypes(
  Array(
    new JsonSubTypes.Type(value = classOf[WorkflowToPythonSuccess], name = "success"),
    new JsonSubTypes.Type(value = classOf[WorkflowToPythonFailure], name = "failure")
  )
)
sealed trait WorkflowToPythonResponse

case class WorkflowToPythonSuccess(pythonCode: String) extends WorkflowToPythonResponse

case class WorkflowToPythonFailure(errorMessage: String) extends WorkflowToPythonResponse

@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
@RolesAllowed(Array("REGULAR", "ADMIN"))
@Path("/workflow-to-python")
class WorkflowToPythonResource extends LazyLogging {

  private val translator = new WorkflowToPythonTranslator()

  @POST
  @Path("")
  def convertWorkflowToPython(
      logicalPlanPojo: LogicalPlanPojo
  ): WorkflowToPythonResponse = {
    try {
      val logicalPlan = LogicalPlan(logicalPlanPojo)
      val pythonCode = translator.translate(logicalPlan)
      WorkflowToPythonSuccess(pythonCode)
    } catch {
      case e: Exception =>
        logger.error("Failed to translate workflow to Python", e)
        WorkflowToPythonFailure(e.getMessage)
    }
  }
}
