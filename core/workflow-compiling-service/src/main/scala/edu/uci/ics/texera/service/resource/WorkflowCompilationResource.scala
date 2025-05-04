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

package edu.uci.ics.texera.service.resource

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.compiler.WorkflowCompiler
import edu.uci.ics.amber.compiler.model.LogicalPlanPojo
import edu.uci.ics.amber.core.tuple.Attribute
import edu.uci.ics.amber.core.workflow.{PhysicalPlan, WorkflowContext}
import edu.uci.ics.amber.core.virtualidentity.WorkflowIdentity
import edu.uci.ics.amber.core.workflowruntimestate.WorkflowFatalError
import jakarta.annotation.security.RolesAllowed
import jakarta.ws.rs.{Consumes, POST, Path, Produces}
import jakarta.ws.rs.core.MediaType

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type"
)
@JsonSubTypes(
  Array(
    new JsonSubTypes.Type(value = classOf[WorkflowCompilationSuccess], name = "success"),
    new JsonSubTypes.Type(value = classOf[WorkflowCompilationFailure], name = "failure")
  )
)
trait WorkflowCompilationResponse
case class WorkflowCompilationSuccess(
    physicalPlan: PhysicalPlan,
    operatorInputSchemas: Map[String, List[Option[List[Attribute]]]]
) extends WorkflowCompilationResponse

case class WorkflowCompilationFailure(
    operatorErrors: Map[String, WorkflowFatalError],
    operatorInputSchemas: Map[String, List[Option[List[Attribute]]]]
) extends WorkflowCompilationResponse

@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
@RolesAllowed(Array("REGULAR", "ADMIN"))
@Path("/compile")
class WorkflowCompilationResource extends LazyLogging {

  @POST
  @Path("")
  def compileWorkflow(
      logicalPlanPojo: LogicalPlanPojo
  ): WorkflowCompilationResponse = {
    // a placeholder workflow context, as compiling a workflow doesn't require a wid from the frontend
    val context = new WorkflowContext(workflowId = WorkflowIdentity(0))

    // Compile the pojo using WorkflowCompiler
    val compilationResult = new WorkflowCompiler(context).compile(logicalPlanPojo)

    val operatorInputSchemas = compilationResult.operatorIdToInputSchemas.map {
      case (operatorIdentity, schemas) =>
        val opId = operatorIdentity.id
        val attributes = schemas.map { schema =>
          if (schema.isEmpty)
            None
          else
            Some(schema.get.attributes)
        }
        (opId, attributes)
    }

    // Handle success case: No errors in the compilation result
    if (compilationResult.operatorIdToError.isEmpty && compilationResult.physicalPlan.nonEmpty) {
      WorkflowCompilationSuccess(
        physicalPlan = compilationResult.physicalPlan.get,
        operatorInputSchemas
      )
    }
    // Handle failure case: Errors found during compilation
    else {
      WorkflowCompilationFailure(
        operatorErrors = compilationResult.operatorIdToError.map {
          case (operatorIdentity, error) => (operatorIdentity.id, error)
        },
        operatorInputSchemas
      )
    }
  }
}
